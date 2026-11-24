from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, Callable

from ..exceptions import PipelineDefinitionError, StageContractError
from ..i18n import _
from .budget import Budget, BudgetExceeded, Limits
from .cel import CelEngine
from .context import Context
from .durable import (
    FAILED,
    FINISHED,
    RUNNING,
    WAITING,
    Checkpoint,
    CheckpointConflict,
    CheckpointError,
    CheckpointStore,
    RunSuspended,
)
from .event import Event
from .inputs import InputHub
from .instrument import Instrument
from .nodes import Node, StageNode, TerminalNode
from .pipeline import Pipeline
from .policy import OPEN, Policy
from .spec import stage_contract

if TYPE_CHECKING:  # pragma: no cover
    from .debug import StepDebugger
    from .nodes import WaitNode

#: When a session saves itself to its store (see `core/durable.py`).
CHECKPOINT_MODES = ("wait", "every_node")

EventHandler = Callable[[Event], None]

_STOPPED_RESULT = {"result": "stopped"}

_NO_FRAME = Context()


def _running_in(loop: asyncio.AbstractEventLoop) -> bool:
    try:
        return asyncio.get_running_loop() is loop
    except RuntimeError:
        return False


#: Maximum number of events kept per session. A cyclic graph can emit tens of
#: thousands of events per second, so the history is a bounded window.
EVENT_HISTORY = 10_000


@dataclass(slots=True)
class ScopeFrame:
    """The frame and current node of a block (try body) while it runs."""

    ctx: Context
    #: The node of this scope that is currently running. A `try` handler uses
    #: it to report which node failed; `_current_node_id` only tracks the
    #: top-level node.
    node_id: str | None = None


@dataclass(slots=True)
class SessionResult:
    """What a finished run returns: artifacts, the terminal's result, the
    event history, the final frame and the meters used.
    """

    artifacts: dict[str, Any]
    result: dict | None
    history: list[Event]
    context: Context
    #: Every meter the run used (steps, tokens, ...), for billing or logging.
    meters: dict[str, float] = field(default_factory=dict)

    @property
    def waiting(self) -> dict | None:
        """For a run saved at a `wait` node: `{"status": "waiting", "node",
        "input", "deadline"}`. None for a run that ended."""
        result = self.result or {}
        return result if result.get("status") == "waiting" else None

    def to_dict(self) -> dict:
        return {
            "artifacts": self.artifacts,
            "result": self.result,
            "history": [event.to_dict() for event in self.history],
            "context": self.context.to_dict(),
            "meters": self.meters,
        }


class Session:
    """One run of a pipeline.

    Takes the host's `policy` and `limits`, optional `debugger` and
    `instrument`, and an initial `context`. Call `run()` to execute; `stop`,
    `pause`, `resume` and `input` control a run in progress.
    """

    def __init__(
        self,
        id: str,
        pipeline: Pipeline,
        context: Context | None = None,
        event_handler: EventHandler | None = None,
        debugger: "StepDebugger | None" = None,
        policy: Policy | None = None,
        limits: Limits | None = None,
        budget: Budget | None = None,
        reports_budget: bool = True,
        instrument: Instrument | None = None,
        store: CheckpointStore | None = None,
        checkpoint: str = "wait",
    ):
        # The policy (what the pipeline may use) and the limits (how much it
        # may spend) always come from the host, never from the pipeline.
        # Limits usually arrive inside the policy; the separate `limits`
        # argument overrides them, including for validation. `budget` lets a
        # child session share its parent's running totals.
        self.policy = policy or OPEN
        if limits is not None:
            self.policy = replace(self.policy, limits=limits)
        self.budget = budget or Budget(self.policy.limits)
        # A subpipeline's child session shares the parent's budget and lets
        # BudgetExceeded propagate instead of turning it into a result.
        self._owns_budget = reports_budget
        pipeline.validate(self.policy)
        self.id = id
        self.pipeline = pipeline
        self.context = context or Context()
        self.cel = CelEngine()
        self._event_handler: EventHandler = event_handler or (lambda event: None)
        self.debugger = debugger
        #: Tracing/metrics/logging hooks (see `core/instrument.py`). Optional.
        self.instrument = instrument

        self.artifacts: dict[str, Any] = {}
        self.result: dict | None = None
        #: Set when a terminal node has ended the run.
        self.finished = False
        self.event_history: deque[Event] = deque(maxlen=EVENT_HISTORY)
        self.inputs = InputHub(self._emit_input_event)

        self._stop_requested = asyncio.Event()
        self._running = asyncio.Event()
        self._running.set()
        self._skip_requested = False
        self._current_node_id: str | None = None
        #: The task that walks the graph during `run`, and its event loop.
        #: `stop` cancels this task, from any thread.
        self._walker: asyncio.Task | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

        #: Where the run is saved to outlive this process; see `restore`.
        self.store = store
        if checkpoint not in CHECKPOINT_MODES:
            raise ValueError(f"checkpoint must be one of {CHECKPOINT_MODES}, got {checkpoint!r}")
        self.checkpoint = checkpoint
        #: The stored version this session last saved or loaded; None before
        #: the first save.
        self._version: int | None = None
        #: A run in state "running" may be taken over by `run()`: set by
        #: `restore(takeover=True)`, for a run whose process died.
        self._takeover = False
        #: Deadlines of the wait nodes this run is in, by node id (Unix time).
        self._waits: dict[str, float | None] = {}
        #: Set when the run was saved at a wait node instead of ending.
        self._suspended: RunSuspended | None = None
        #: How many times each node has started, for idempotency keys.
        self._node_starts: dict[str, int] = {}
        #: >0 while running inside a block (try, map, parallel).
        self._nesting = 0

    def emit(self, event: Event) -> None:
        self.event_history.append(event)
        self._event_handler(event)
        if self.instrument is not None:
            self.instrument.event(self, event)

    def emit_node_event(self, type_: str, node: Node, payload: dict | None = None) -> None:
        self.emit(
            Event(type=type_, session_id=self.id, stage_id=node.id, payload=payload or {})
        )

    def _emit_input_event(self, type_: str, payload: dict) -> None:
        self.emit(Event(type=type_, session_id=self.id, payload=payload))

    @property
    def input_history(self) -> list[dict[str, Any]]:
        return self.inputs.history

    async def input(self, type_: str, payload: dict[str, Any]) -> dict[str, Any]:
        entry = {"type": type_, "payload": payload}
        self.emit(Event(type="user_input", session_id=self.id, payload=entry))
        if type_ == "command":
            self._apply_command(payload.get("name"))
        self.inputs.deliver(entry)
        return entry

    def start_wait_input(self, type_: str) -> asyncio.Future:
        return self.inputs.start_wait(type_)

    async def finish_wait_input(
        self, type_: str, fut: asyncio.Future, timeout: float | None = None
    ) -> dict[str, Any] | None:
        """Wait for an input without counting the wait against the budget.

        Time spent waiting for a person is idle time, so it does not count
        towards the `seconds` limit or the stage timeout.
        """
        self.budget.begin_idle()
        try:
            return await self.inputs.finish_wait(type_, fut, timeout=timeout)
        finally:
            self.budget.end_idle()

    async def wait_input(
        self, type_: str, timeout: float | None = None
    ) -> dict[str, Any] | None:
        fut = self.start_wait_input(type_)
        return await self.finish_wait_input(type_, fut, timeout=timeout)

    def last_input(self, type_: str | None = None) -> dict[str, Any] | None:
        return self.inputs.last(type_)

    def is_waiting_for(self, type_: str) -> bool:
        return self.inputs.is_waiting(type_)

    def _apply_command(self, name: str | None) -> None:
        handlers = {
            "stop": self.stop,
            "pause": self.pause,
            "resume": self.resume,
            "skip": self.request_skip,
        }
        handler = handlers.get(name)
        if handler is not None:
            handler()

    def stop(self) -> None:
        """Stop the run: cancel the running node and finish with
        `{"result": "stopped"}`. Safe to call from another thread."""
        loop = self._loop
        if loop is not None and not loop.is_closed() and not _running_in(loop):
            loop.call_soon_threadsafe(self.stop)
            return
        if self._stop_requested.is_set():
            return
        self._stop_requested.set()
        self.emit(Event(type="session_stopped", session_id=self.id))
        walker = self._walker
        if walker is not None and not walker.done():
            walker.cancel()

    def pause(self) -> None:
        if self._running.is_set():
            self._running.clear()
            self.emit(Event(type="session_paused", session_id=self.id))

    def resume(self) -> None:
        if not self._running.is_set():
            self._running.set()
            self.emit(Event(type="session_resumed", session_id=self.id))

    def request_skip(self) -> None:
        self._skip_requested = True

    @property
    def stopped(self) -> bool:
        return self._stop_requested.is_set()

    @property
    def paused(self) -> bool:
        return not self._running.is_set()

    async def execute_node(self, node: Node, ctx: Context) -> tuple["Node | None", Context]:
        """Execute one node. Every node goes through here, whether it runs in
        the main loop, a `try` body, a `parallel` branch or a `map` item, so
        this is where the policy, budget, instrument and debugger hook in.
        """
        self.policy.check_node_type(node.type, node.id)
        self.budget.check_deadline()
        self.budget.charge("steps", 1)
        self._node_starts[node.id] = self._node_starts.get(node.id, 0) + 1

        if self.instrument is None:
            return await self._execute_node(node, ctx)
        with self.instrument.node(self, node):
            return await self._execute_node(node, ctx)

    async def _execute_node(self, node: Node, ctx: Context) -> tuple["Node | None", Context]:
        if self.debugger is None:
            next_node, ctx = await node.execute(self, ctx)
        else:
            ctx = await self.debugger.before_node(self, node, ctx) or ctx
            next_node, ctx = await node.execute(self, ctx)
            ctx = await self.debugger.after_node(self, node, ctx) or ctx

        self._measure_frame(ctx)
        return next_node, ctx

    def _measure_frame(self, ctx: Context) -> None:
        """Record the frame size, but only if `frame_bytes` is limited:
        measuring requires serializing the frame, which is expensive.
        """
        if not self.budget.measures("frame_bytes"):
            return
        try:
            size = len(json.dumps(dict(ctx.vars), default=str, ensure_ascii=False))
        except (TypeError, ValueError):  # pragma: no cover - a value json cannot see
            return
        self.budget.observe("frame_bytes", size)

    async def run_stage(self, node: StageNode, kwargs: dict) -> dict:
        self.policy.check_stage(node.stage, node.id)
        stage_cls = node.get_stage_class()

        # Reserve before running: refuse a stage that cannot be paid for
        # rather than cut it off halfway through.
        reserved = self._reservation(stage_cls, kwargs, node)
        over = self.budget.affordable(reserved)
        if over is not None:
            raise BudgetExceeded(*over)
        self.budget.charge_all(reserved)

        stage = stage_cls(stage_id=node.id, arguments=kwargs, session=self)
        stage.idempotency_key = f"{self.id}:{node.id}:{self._node_starts.get(node.id, 1)}"
        self.emit_node_event("stage_started", node, {"stage": node.stage})
        try:
            await self._run_stage_body(stage)
        except asyncio.TimeoutError:
            # A timeout caused by the session deadline is reported as
            # BudgetExceeded, not as the stage's own timeout.
            self.budget.check_deadline()
            self.emit_node_event("stage_timeout", node, {"stage": node.stage})
            raise
        except Exception as exc:  # noqa: BLE001
            self.emit_node_event("stage_failed", node, {"stage": node.stage, "error": str(exc)})
            raise
        finally:
            # Charged meters replace the reserved amounts. Meters the stage
            # did not charge stay as reserved, even if the stage failed.
            self._settle(reserved, stage.charged, node)
        self.emit_node_event("stage_completed", node, {"stage": node.stage})
        return stage.collected_outputs

    def _reservation(self, stage_cls, kwargs: dict, node: StageNode) -> dict[str, float]:
        """Evaluate the stage's `reserve` spec. Skipped when nothing is limited."""
        if self.budget.limits.unlimited:
            return {}
        declared = stage_contract(stage_cls).reserve
        if not declared:
            return {}
        amounts: dict[str, float] = {}
        for meter, spec in declared.items():
            # `reserve` expressions see `args` only (see docs/limits.md).
            value = (self.cel.eval(spec, _NO_FRAME, args=kwargs)
                     if isinstance(spec, str) else spec)
            try:
                amounts[meter] = float(value)
            except (TypeError, ValueError):
                raise StageContractError(
                    _("{node}: reserve '{meter}' of stage {stage} "
                      "is not a number: {value}",
                      node=node.id, meter=meter, stage=node.stage, value=repr(value))
                ) from None
        return amounts

    def _settle(self, reserved: dict, charged: dict, node: StageNode) -> None:
        if not charged:
            return
        for meter, amount in charged.items():
            self.budget.charge(meter, amount - reserved.get(meter, 0.0))
        self.emit_node_event("stage_charged", node, {"stage": node.stage, **charged})

    async def _run_stage_body(self, stage) -> None:
        """Run the stage under its timeout, measured in busy time.

        The timeout is paused while the session is idle (waiting for input),
        see `Budget.pausable`. The stage runs in the current task.
        """
        timeout = self.budget.timeout_for(stage.timeout)
        if timeout is None:
            await stage.run()
            return
        async with asyncio.timeout(timeout) as deadline:
            with self.budget.pausable(deadline):
                await stage.run()

    async def run_subpipeline(self, node, child_ctx: Context) -> SessionResult:
        if node.subpipeline_id not in self.pipeline.subpipelines:
            raise PipelineDefinitionError(
                _("Subpipeline '{name}' not found", name=node.subpipeline_id)
            )

        data = dict(self.pipeline.subpipelines[node.subpipeline_id])
        data.setdefault("subpipelines", self.pipeline.subpipelines)
        parent_types = self.pipeline.raw_json.get("types")
        if parent_types:
            data.setdefault("types", parent_types)
        child_pipeline = Pipeline.from_dict(data)

        def proxy_event(event: Event) -> None:
            self.emit(
                replace(event, payload={"subpipeline_node": node.id, **(event.payload or {})})
            )

        child = Session(
            id=f"{self.id}:{node.id}",
            pipeline=child_pipeline,
            context=child_ctx,
            event_handler=proxy_event,
            debugger=self.debugger,
            instrument=self.instrument,
            # The child runs under the parent's policy and budget.
            policy=self.policy,
            budget=self.budget,
            reports_budget=False,
        )
        with self.budget.gauge("depth"):
            return await child.run()

    async def run_scope(
        self,
        node: "Node | None",
        ctx: Context,
        scope: frozenset[str],
        frame: "ScopeFrame | None" = None,
    ) -> tuple["Node | None", Context]:
        self._nesting += 1
        try:
            while node is not None and node.id in scope and not self.finished:
                if frame is not None:
                    # Set before executing, so that it names the failing node.
                    frame.node_id = node.id
                node, ctx = await self.execute_node(node, ctx)
                if frame is not None:
                    frame.ctx = ctx
        finally:
            self._nesting -= 1
        return node, ctx

    async def run_subgraph(self, start_id: str, ctx: Context) -> Context:
        node: Node | None = self.pipeline.get_node(start_id)
        self._nesting += 1
        try:
            # Stop if a terminal in another branch has already ended the run.
            while node is not None and not self.finished:
                node, ctx = await self.execute_node(node, ctx)
        finally:
            self._nesting -= 1
        return ctx

    # ------------------------------------------------------------ waiting

    async def wait_at(self, node: "WaitNode") -> dict[str, Any] | None:
        """Called by a `wait` node: the input it waits for, or None when its
        timeout has passed. With a store and no input yet, saves the run
        instead of waiting (raises `RunSuspended`, which `run` turns into a
        `{"status": "waiting"}` result)."""
        if self._nesting or not self._owns_budget:
            # Validation refuses this; a graph built in Python may not have
            # been validated.
            raise PipelineDefinitionError(
                _("{node}: a wait node cannot be inside a block or a subpipeline",
                  node=node.id)
            )
        now = time.time()
        if node.id not in self._waits:
            timeout = node.timeout_seconds
            self._waits[node.id] = None if timeout is None else now + timeout
            self.emit_node_event("waiting_started", node, {
                "input": node.input, "deadline": self._waits[node.id]})
        deadline = self._waits[node.id]

        entry = self.inputs.take(node.input)
        if entry is None and (deadline is None or now < deadline):
            if self.store is not None:
                raise RunSuspended(node.id, node.input, deadline)
            entry = await self.wait_input(
                node.input, timeout=None if deadline is None else deadline - now)
        # Answered or timed out: a later visit (in a cycle) waits afresh.
        del self._waits[node.id]
        return entry

    # ------------------------------------------------------------ durability

    async def _save(self, state: str) -> None:
        deadline = self._suspended.deadline if state == WAITING and self._suspended else None
        checkpoint = Checkpoint(run_id=self.id, version=0, state=state,
                                snapshot=self.snapshot(history=False), deadline=deadline)
        stored = await self.store.save(checkpoint, expected=self._version)
        self._version = stored.version

    async def _claim(self) -> None:
        """Mark the run as running in the store before doing anything, so that
        a second process resuming it at the same time fails here."""
        if self._version is None:
            await self._save(RUNNING)  # a new run: its id must be free
            return
        current = await self.store.load(self.id)
        if current is None:
            raise CheckpointError(f"run '{self.id}' is not in the store")
        if current.version != self._version:
            raise CheckpointConflict(
                f"run '{self.id}' changed since it was restored "
                f"(version {self._version}, now {current.version})")
        if current.state in (FINISHED, FAILED):
            raise CheckpointError(f"run '{self.id}' is already {current.state}")
        if current.state == RUNNING and not self._takeover:
            raise CheckpointConflict(
                f"run '{self.id}' is running elsewhere; restore it with "
                f"takeover=True if that process is gone")
        await self._save(RUNNING)

    async def _durable_run(self) -> SessionResult:
        if self.store is None:
            return await self._run()
        await self._claim()
        try:
            result = await self._run()
        except Exception:
            await self._save(FAILED)
            raise
        # BaseExceptions (a cancelled run, an interpreter shutting down) leave
        # the run marked as running: `restore(takeover=True)` picks it up.
        await self._save(WAITING if self._suspended else FINISHED)
        return result

    def finish(self, node: TerminalNode, ctx: Context) -> None:
        # Two parallel branches may both reach a terminal; the first one wins.
        if self.finished:
            return
        self.artifacts = {name: ctx.get_var(name) for name in node.artifacts}
        self.result = node.result
        self.finished = True
        self.emit_node_event("session_terminated", node, {"artifacts": sorted(self.artifacts)})

    async def run(self) -> SessionResult:
        """Run to the end, or, with a store, until a `wait` node has no input
        yet: then the run is saved and the result is `{"status": "waiting"}`
        (see `SessionResult.waiting` and `restore`)."""
        if self.instrument is None:
            return await self._durable_run()
        with self.instrument.session(self):
            return await self._durable_run()

    async def _run(self) -> SessionResult:
        self.pipeline.typesystem.check_context(self.context, f"session '{self.id}'")
        self.budget.start()
        self._suspended = None
        self.emit(Event(type="session_started", session_id=self.id))

        node: Node | None = (
            self.pipeline.get_node(self._current_node_id)
            if self._current_node_id
            else self.pipeline.get_entry_node()
        )

        # Walk the graph in a separate task so that `stop` can interrupt the
        # running node by cancelling that task.
        self._loop = asyncio.get_running_loop()
        walker = self._walker = asyncio.create_task(self._walk(node))
        try:
            await walker
        except asyncio.CancelledError:
            current = asyncio.current_task()
            if (current is not None and current.cancelling()) or not walker.done():
                # `run` itself was cancelled (not a stop): propagate.
                walker.cancel()
                await asyncio.gather(walker, return_exceptions=True)
                raise
            self.result = dict(_STOPPED_RESULT)
        except BudgetExceeded as over:
            # The only place BudgetExceeded is caught. Pipelines cannot catch
            # it (it is not an Exception). The run ends with a budget result
            # and keeps the artifacts collected so far.
            self.emit(Event(type="budget_exceeded", session_id=self.id,
                            payload=over.as_result()))
            if not self._owns_budget:
                raise
            self.result = over.as_result()
        except RunSuspended as suspended:
            # Saved at a wait node: the run goes on from there in `restore`.
            self._suspended = suspended
            self.result = suspended.as_result()
            self.emit(Event(type="session_suspended", session_id=self.id,
                            payload=suspended.as_result()))
        finally:
            self._walker = None

        ctx = self.context
        if self._suspended is None:
            self._current_node_id = None
        if self._owns_budget:
            self.budget.stop()
        self.emit(Event(type="session_completed", session_id=self.id))
        return SessionResult(
            artifacts=self.artifacts,
            result=self.result,
            history=list(self.event_history),
            context=ctx,
            meters=self.budget.report(),
        )

    async def _walk(self, node: "Node | None") -> None:
        ctx = self.context
        while node is not None:
            self._current_node_id = node.id
            if not self._running.is_set():
                await self._running.wait()  # paused; a stop cancels this wait
            if self.stopped:
                self.result = dict(_STOPPED_RESULT)
                return
            if self._try_skip(node):
                node = self.pipeline.get_node(node.next) if node.next else None
                continue
            node, ctx = await self.execute_node(node, ctx)
            # Keep the current frame so that a snapshot resumes from it.
            self.context = ctx
            if self.checkpoint == "every_node" and self.store is not None \
                    and node is not None and not self.finished:
                # Saved between nodes: after a crash the run goes on from
                # the next one (`restore(takeover=True)`).
                self._current_node_id = node.id
                await self._save(RUNNING)

    def _try_skip(self, node: Node) -> bool:
        if not (self._skip_requested and isinstance(node, StageNode)):
            return False
        self._skip_requested = False
        if getattr(node.get_stage_class(), "skipable", False):
            self.emit_node_event("stage_skipped", node, {"stage": node.stage})
            return True
        self.emit_node_event("skip_denied", node, {"stage": node.stage})
        return False

    def snapshot(self, history: bool = True) -> dict:
        """The session as JSON-able data, for `from_snapshot`. A checkpoint
        leaves out the event history (`history=False`): it can be megabytes,
        and the run does not need it to go on."""
        return {
            "session_id": self.id,
            "current_node_id": self._current_node_id,
            "result": self.result,
            "artifacts": dict(self.artifacts),
            "context": self.context.to_dict(),
            "event_history": ([event.to_dict() for event in self.event_history]
                              if history else []),
            "input_history": list(self.inputs.history),
            "pending_inputs": {k: list(v) for k, v in self.inputs.pending.items()},
            "waits": dict(self._waits),
            "waiting": self._suspended.as_result() if self._suspended else None,
            "node_starts": dict(self._node_starts),
            "stopped": self.stopped,
            "paused": self.paused,
            "skip_requested": self._skip_requested,
            "pipeline": self.pipeline.raw_json,
            # Spent meters, so that a restored session continues from them.
            "meters": self.budget.report(),
        }

    @classmethod
    def from_snapshot(
        cls,
        snapshot: dict,
        pipeline: Pipeline | None = None,
        event_handler: EventHandler | None = None,
        *,
        policy: Policy,
        limits: Limits | None = None,
        instrument: Instrument | None = None,
        store: CheckpointStore | None = None,
        checkpoint: str = "wait",
    ) -> "Session":
        """Restore a session from `snapshot()`.

        `policy` is required and is not stored in the snapshot: a snapshot is
        data and could be edited, so the host must supply the policy again
        (`OPEN` for no restrictions). Spent meters are restored.
        """
        if pipeline is None:
            raw = snapshot.get("pipeline")
            if not raw:
                raise PipelineDefinitionError(
                    _("Pipeline is required to restore session")
                )
            pipeline = Pipeline.from_dict(raw)

        session = cls(
            id=snapshot.get("session_id"),
            pipeline=pipeline,
            context=Context.from_dict(snapshot.get("context", {})),
            event_handler=event_handler,
            policy=policy,
            limits=limits,
            instrument=instrument,
            store=store,
            checkpoint=checkpoint,
        )
        session.budget.restore(snapshot.get("meters"))
        session._current_node_id = snapshot.get("current_node_id")
        session.result = snapshot.get("result")
        session.artifacts = snapshot.get("artifacts", {})
        session.inputs.history.extend(snapshot.get("input_history", []))
        for type_, entries in (snapshot.get("pending_inputs") or {}).items():
            session.inputs.pending.setdefault(type_, []).extend(entries)
        session._waits = dict(snapshot.get("waits") or {})
        session._node_starts = dict(snapshot.get("node_starts") or {})
        session.event_history = deque(
            (Event.from_dict(e) for e in snapshot.get("event_history", [])),
            maxlen=EVENT_HISTORY,
        )
        session._skip_requested = snapshot.get("skip_requested", False)
        if snapshot.get("stopped", False):
            session._stop_requested.set()
        if snapshot.get("paused", False):
            session._running.clear()
        return session

    @classmethod
    async def restore(
        cls,
        store: CheckpointStore,
        run_id: str,
        event_handler: EventHandler | None = None,
        *,
        policy: Policy,
        limits: Limits | None = None,
        instrument: Instrument | None = None,
        checkpoint: str = "wait",
        takeover: bool = False,
    ) -> "Session":
        """Load a saved run to continue it:

            session = await Session.restore(store, run_id, policy=policy)
            await session.input("approval", {"ok": True})
            result = await session.run()

        Nothing is written until `run()`, which first marks the run as
        running: if another process got there first, `run()` raises
        `CheckpointConflict` before running anything. `takeover=True`
        continues a run left marked as running by a process that died.

        `policy` is required, as for `from_snapshot`. The pipeline is the one
        the run started with, stored in the checkpoint: editing a pipeline
        does not change runs already under way.
        """
        saved = await store.load(run_id)
        if saved is None:
            raise CheckpointError(f"run '{run_id}' is not in the store")
        if saved.state in (FINISHED, FAILED):
            raise CheckpointError(f"run '{run_id}' is already {saved.state}")
        session = cls.from_snapshot(
            saved.snapshot, event_handler=event_handler, policy=policy, limits=limits,
            instrument=instrument, store=store, checkpoint=checkpoint,
        )
        session._version = saved.version
        session._takeover = takeover
        if saved.state == WAITING:
            session.result = None  # "waiting" was the answer of the last run
        session.emit(Event(type="session_restored", session_id=run_id,
                           payload={"state": saved.state, "version": saved.version}))
        return session
