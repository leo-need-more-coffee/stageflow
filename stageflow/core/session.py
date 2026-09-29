from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, Callable

from ..exceptions import PipelineDefinitionError, StageContractError
from .budget import Budget, BudgetExceeded, Limits
from .cel import CelEngine
from .context import Context
from .event import Event
from .inputs import InputHub
from .nodes import Node, StageNode, TerminalNode
from .pipeline import Pipeline
from .policy import OPEN, Policy

if TYPE_CHECKING:  # pragma: no cover
    from .debug import StepDebugger

EventHandler = Callable[[Event], None]

_STOPPED_RESULT = {"result": "stopped"}

#: How many events one session keeps. A cyclic graph emits tens of thousands
#: a second, and an unbounded list is the cheapest way for a pipeline to take
#: a process down — the log is a window, not an archive.
EVENT_HISTORY = 10_000


@dataclass(slots=True)
class ScopeFrame:
    ctx: Context
    #: the node of this scope that is running. Kept so that a handler catching
    #: an error can name what failed: by the time the exception is caught the
    #: walker has unwound, and `_current_node_id` only follows the top-level
    #: one, so inside a block it is somebody else's node
    node_id: str | None = None


@dataclass(slots=True)
class SessionResult:
    artifacts: dict[str, Any]
    result: dict | None
    history: list[Event]
    context: Context
    #: every meter the run moved — what a host bills or logs by
    meters: dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "artifacts": self.artifacts,
            "result": self.result,
            "history": [event.to_dict() for event in self.history],
            "context": self.context.to_dict(),
            "meters": self.meters,
        }


class Session:
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
    ):
        # what this session may be made of, and how much it may spend. Both
        # given by the host, never by the pipeline: a graph that could widen
        # its own allowance has none. `budget` is how a child session shares
        # the parent's running total instead of getting a fresh one
        # a plan is one object, so the limits usually arrive inside the
        # policy; the separate argument is for a host with nothing to
        # restrict but something to bound, and it wins where both are given —
        # including for validation, or the static checks and the running
        # totals would answer to different numbers
        self.policy = policy or OPEN
        if limits is not None:
            self.policy = replace(self.policy, limits=limits)
        self.budget = budget or Budget(self.policy.limits)
        # a child session of a subpipeline shares the budget and must let a
        # ceiling travel up rather than answering for it. Said outright
        # rather than inferred from "was a budget passed in", which gets a
        # host handing its own budget to a top-level session backwards
        self._owns_budget = reports_budget
        pipeline.validate(self.policy)
        self.id = id
        self.pipeline = pipeline
        self.context = context or Context()
        self.cel = CelEngine()
        self._event_handler: EventHandler = event_handler or (lambda event: None)
        self.debugger = debugger

        self.artifacts: dict[str, Any] = {}
        self.result: dict | None = None
        #: a terminal node has ended the run; nothing else should be executed
        self.finished = False
        self.event_history: deque[Event] = deque(maxlen=EVENT_HISTORY)
        self.inputs = InputHub(self._emit_input_event)

        self._stop_requested = asyncio.Event()
        self._running = asyncio.Event()
        self._running.set()
        self._skip_requested = False
        self._current_node_id: str | None = None

    def emit(self, event: Event) -> None:
        self.event_history.append(event)
        self._event_handler(event)

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
        """Wait for an answer, and do not charge the session for the wait.

        A pipeline that asks a question and sits there is not consuming
        anything. Counting that time would make the deadline a limit on how
        fast the user reads.
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
        if self._stop_requested.is_set():
            return
        self._stop_requested.set()
        self.emit(Event(type="session_stopped", session_id=self.id))

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
        """The one path every node takes, wherever it is being run from.

        `run`, the body of a `try`, a branch of a `parallel` and an iteration
        of a `map` all come through here, which is why the debugger hooks in
        at this point — and why the budget does too.
        """
        self.policy.check_node_type(node.type, node.id)
        self.budget.check_deadline()
        self.budget.charge("steps", 1)

        if self.debugger is None:
            next_node, ctx = await node.execute(self, ctx)
        else:
            ctx = await self.debugger.before_node(self, node, ctx) or ctx
            next_node, ctx = await node.execute(self, ctx)
            ctx = await self.debugger.after_node(self, node, ctx) or ctx

        self._measure_frame(ctx)
        return next_node, ctx

    def _measure_frame(self, ctx: Context) -> None:
        """How big the frame has grown — only when somebody limits it.

        Measuring means serialising, which is the most expensive check here,
        so it does not happen at all unless `frame_bytes` is in the limits.
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

        # held before anything runs: a stage that cannot be paid for is better
        # not begun than cut off halfway through a call that already went out
        reserved = self._reservation(stage_cls, kwargs, node)
        over = self.budget.affordable(reserved)
        if over is not None:
            raise BudgetExceeded(*over)
        self.budget.charge_all(reserved)

        stage = stage_cls(stage_id=node.id, arguments=kwargs, session=self)
        self.emit_node_event("stage_started", node, {"stage": node.stage})
        try:
            await self._run_stage_body(stage)
        except asyncio.TimeoutError:
            # a stage cut short by the session deadline rather than by its own
            # timeout should say so: they are different problems
            self.budget.check_deadline()
            self.emit_node_event("stage_timeout", node, {"stage": node.stage})
            raise
        except Exception as exc:  # noqa: BLE001
            self.emit_node_event("stage_failed", node, {"stage": node.stage, "error": str(exc)})
            raise
        finally:
            # meters the stage charged replace what was reserved for them;
            # meters it did not charge stay reserved, failure included — a
            # call that went out and then timed out still spent what it spent
            self._settle(reserved, stage.charged, node)
        self.emit_node_event("stage_completed", node, {"stage": node.stage})
        return stage.collected_outputs

    def _reservation(self, stage_cls, kwargs: dict, node: StageNode) -> dict[str, float]:
        """What the stage's spec asks to hold, with its expressions evaluated.

        Nothing is evaluated when nothing is limited: a host with no budget
        pays neither for the CEL nor for the lookup.
        """
        if self.budget.limits.unlimited:
            return {}
        declared = stage_cls.get_specs().get("reserve") or {}
        if not declared:
            return {}
        amounts: dict[str, float] = {}
        for meter, spec in declared.items():
            value = (self.cel.eval(spec, self.context, args=kwargs)
                     if isinstance(spec, str) else spec)
            try:
                amounts[meter] = float(value)
            except (TypeError, ValueError):
                raise StageContractError(
                    f"{node.id}: reserve '{meter}' of stage {node.stage} "
                    f"is not a number: {value!r}"
                ) from None
        return amounts

    def _settle(self, reserved: dict, charged: dict, node: StageNode) -> None:
        if not charged:
            return
        for meter, amount in charged.items():
            self.budget.charge(meter, amount - reserved.get(meter, 0.0))
        self.emit_node_event("stage_charged", node, {"stage": node.stage, **charged})

    async def _run_stage_body(self, stage) -> None:
        """Run the stage under a deadline that does not tick while it waits.

        `wait_for` cannot express this: its clock is wall time, so a stage
        that asks a human a question dies thirty seconds later. Here the
        deadline is busy time — the wait wakes us, we see the stage was idle
        for it, and we give the time back instead of killing the stage.
        """
        timeout = self.budget.timeout_for(stage.timeout)
        if timeout is None:
            await stage.run()
            return

        task = asyncio.ensure_future(stage.run())
        started, idle_before = time.monotonic(), self.budget.idle_seconds
        try:
            while True:
                idled = self.budget.idle_seconds - idle_before
                left = timeout - ((time.monotonic() - started) - idled)
                if left <= 0:
                    raise asyncio.TimeoutError
                done, _ = await asyncio.wait({task}, timeout=left)
                if done:
                    await task  # re-raises whatever the stage raised
                    return
        except BaseException:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            raise

    async def run_subpipeline(self, node, child_ctx: Context) -> SessionResult:
        if node.subpipeline_id not in self.pipeline.subpipelines:
            raise PipelineDefinitionError(f"Subpipeline '{node.subpipeline_id}' not found")

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
            # the allowance is the tenant's, not the graph's: a subpipeline
            # is not a way out of it, and neither is it a fresh budget
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
        while node is not None and node.id in scope and not self.finished:
            if frame is not None:
                # before, not after: if this one raises, this is the answer to
                # "which node failed"
                frame.node_id = node.id
            node, ctx = await self.execute_node(node, ctx)
            if frame is not None:
                frame.ctx = ctx
        return node, ctx

    async def run_subgraph(self, start_id: str, ctx: Context) -> Context:
        node: Node | None = self.pipeline.get_node(start_id)
        while node is not None:
            node, ctx = await self.execute_node(node, ctx)
        return ctx

    def finish(self, node: TerminalNode, ctx: Context) -> None:
        self.artifacts = {name: ctx.get_var(name) for name in node.artifacts}
        self.result = node.result
        self.finished = True
        self.emit_node_event("session_terminated", node, {"artifacts": sorted(self.artifacts)})

    async def run(self) -> SessionResult:
        self.pipeline.typesystem.check_context(self.context, f"session '{self.id}'")
        self.budget.start()
        self.emit(Event(type="session_started", session_id=self.id))

        node: Node | None = (
            self.pipeline.get_node(self._current_node_id)
            if self._current_node_id
            else self.pipeline.get_entry_node()
        )
        ctx = self.context

        try:
            while node is not None:
                self._current_node_id = node.id

                await self._pause_gate()
                if self.stopped:
                    self.result = dict(_STOPPED_RESULT)
                    break

                if self._try_skip(node):
                    node = self.pipeline.get_node(node.next) if node.next else None
                    continue

                step = asyncio.create_task(self.execute_node(node, ctx))
                if await self._interrupted_by_stop(step):
                    self.result = dict(_STOPPED_RESULT)
                    break
                node, ctx = step.result()
        except BudgetExceeded as over:
            # the one place that catches it. A pipeline cannot: `try` blocks
            # and `run_with_retry` catch Exception, and this is not one. The
            # run ends the way a stop does — with a result and whatever
            # artifacts were already collected, rather than an exception
            # thrown through work that did happen
            self.emit(Event(type="budget_exceeded", session_id=self.id,
                            payload=over.as_result()))
            if not self._owns_budget:
                raise
            self.result = over.as_result()

        self.context = ctx
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

    async def _pause_gate(self) -> None:
        while not self._running.is_set() and not self.stopped:
            await self._race(self._running.wait(), self._stop_requested.wait())

    async def _interrupted_by_stop(self, step: asyncio.Task) -> bool:
        await self._race(step, self._stop_requested.wait())
        if step.done():
            return False
        step.cancel()
        try:
            await step
        except asyncio.CancelledError:
            pass
        return True

    @staticmethod
    async def _race(*aws) -> None:
        tasks = [aw if isinstance(aw, asyncio.Task) else asyncio.create_task(aw) for aw in aws]
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()

    def _try_skip(self, node: Node) -> bool:
        if not (self._skip_requested and isinstance(node, StageNode)):
            return False
        self._skip_requested = False
        if getattr(node.get_stage_class(), "skipable", False):
            self.emit_node_event("stage_skipped", node, {"stage": node.stage})
            return True
        self.emit_node_event("skip_denied", node, {"stage": node.stage})
        return False

    def snapshot(self) -> dict:
        return {
            "session_id": self.id,
            "current_node_id": self._current_node_id,
            "result": self.result,
            "artifacts": dict(self.artifacts),
            "context": self.context.to_dict(),
            "event_history": [event.to_dict() for event in self.event_history],
            "input_history": list(self.inputs.history),
            "stopped": self.stopped,
            "paused": self.paused,
            "skip_requested": self._skip_requested,
            "pipeline": self.pipeline.raw_json,
        }

    @classmethod
    def from_snapshot(
        cls,
        snapshot: dict,
        pipeline: Pipeline | None = None,
        event_handler: EventHandler | None = None,
    ) -> "Session":
        if pipeline is None:
            raw = snapshot.get("pipeline")
            if not raw:
                raise PipelineDefinitionError("Pipeline is required to restore session")
            pipeline = Pipeline.from_dict(raw)

        session = cls(
            id=snapshot.get("session_id"),
            pipeline=pipeline,
            context=Context.from_dict(snapshot.get("context", {})),
            event_handler=event_handler,
        )
        session._current_node_id = snapshot.get("current_node_id")
        session.result = snapshot.get("result")
        session.artifacts = snapshot.get("artifacts", {})
        session.inputs.history.extend(snapshot.get("input_history", []))
        session.event_history = [Event.from_dict(e) for e in snapshot.get("event_history", [])]
        session._skip_requested = snapshot.get("skip_requested", False)
        if snapshot.get("stopped", False):
            session._stop_requested.set()
        if snapshot.get("paused", False):
            session._running.clear()
        return session
