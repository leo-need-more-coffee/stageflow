import asyncio
from typing import Callable, Any

from .context import Context
from .event import Event
from .pipeline import Pipeline
from .node import StageNode, ConditionNode, ParallelNode, TerminalNode


class SessionResult:
    artifacts: dict[str, dict | None]
    result: dict | None
    history: list[Event]
    context: Context

    def __init__(self, artifacts: dict, result: dict | None, history: list[Event], context: Context):
        self.artifacts = artifacts
        self.result = result
        self.history = history
        self.context = context

    def to_dict(self) -> dict:
        return {
            "artifacts": self.artifacts,
            "result": self.result,
            "history": [e.to_dict() for e in self.history],
            "context": self.context.to_dict(),
        }


class Session:
    def __init__(
        self,
        id: str,
        pipeline: Pipeline,
        context: Context = None,
        event_handler: Callable[[Event], None] = None,
    ):
        pipeline.validate()
        self.id = id
        self.pipeline = pipeline
        self.context = context or Context()
        self.event_handler = event_handler or (lambda event: None)

        self.artifact_paths: list[str] = []
        self.result: dict | None = None
        self.event_history: list[Event] = []

        self.input_history: list[dict[str, Any]] = []
        self._waiting: dict[str, asyncio.Future] = {}

        self._stopped = False
        self._paused = False
        self._skip_requested = False

    def emit(self, event: Event):
        self.event_history.append(event)
        self.event_handler(event)

    async def input(self, type_: str, payload: dict[str, Any]):
        entry = {"type": type_, "payload": payload}
        self.input_history.append(entry)
        self.emit(Event(type="user_input", session_id=self.id, payload=entry))

        if type_ == "command":
            cmd = payload.get("name")
            if cmd == "stop":
                self._stopped = True
                self.emit(Event(type="session_stopped", session_id=self.id))
            elif cmd == "skip":
                self._skip_requested = True
            elif cmd == "pause":
                self._paused = True
                self.emit(Event(type="session_paused", session_id=self.id))
            elif cmd == "resume":
                self._paused = False
                self.emit(Event(type="session_resumed", session_id=self.id))

        if type_ in self._waiting and not self._waiting[type_].done():
            self._waiting[type_].set_result(entry)
        return entry

    async def wait_input(self, type_: str, timeout: float | None = None):
        fut = asyncio.get_event_loop().create_future()
        self._waiting[type_] = fut
        self.emit(Event(type="waiting_for_input", session_id=self.id, payload={"type": type_}))
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            self.emit(Event(type="input_timeout", session_id=self.id, payload={"type": type_}))
            return None
        finally:
            del self._waiting[type_]

    def last_input(self, type_: str | None = None):
        if not self.input_history:
            return None
        if type_ is None:
            return self.input_history[-1]
        for entry in reversed(self.input_history):
            if entry["type"] == type_:
                return entry
        return None

    async def run(self) -> SessionResult:
        self.emit(Event(type="session_started", session_id=self.id))
        node = self.pipeline.get_entry_node()

        while node:
            while self._paused and not self._stopped:
                await asyncio.sleep(0.1)

            if self._stopped:
                self.result = {"result": "stopped"}
                break

            if self._skip_requested and isinstance(node, StageNode):
                stage_class = node.get_stage_class()
                if getattr(stage_class, "skipable", True):
                    self.emit(Event(type="stage_skipped", session_id=self.id, stage_id=node.id))
                    node = self.pipeline.get_node(node.next) if node.next else None
                    self._skip_requested = False
                    continue
                else:
                    self.emit(Event(type="skip_denied", session_id=self.id, stage_id=node.id))
                    self._skip_requested = False

            handler = self._get_handler(node)
            handler_task = asyncio.create_task(handler(node))
            try:
                while not handler_task.done():
                    if self._stopped:
                        handler_task.cancel()
                        self.result = {"result": "stopped"}
                        try:
                            await handler_task
                        except asyncio.CancelledError:
                            pass
                        break
                    await asyncio.sleep(0.05)
                if handler_task.cancelled():
                    break
                next_node = await handler_task
            except asyncio.CancelledError:
                self.result = {"result": "stopped"}
                break
            except Exception as e:
                raise e

            if next_node is None and not isinstance(node, TerminalNode):
                raise RuntimeError(f"No next node for {node.id} in session {self.id}")

            node = next_node

        self.emit(Event(type="session_completed", session_id=self.id))
        artifacts = {
            a: self.context.get(a, None)
            for a in self.artifact_paths
        }
        return SessionResult(
            artifacts=artifacts,
            result=self.result,
            history=self.event_history,
            context=self.context,
        )

    def _get_handler(self, node):
        if isinstance(node, TerminalNode):
            return self._handle_terminal
        if isinstance(node, StageNode):
            return self._handle_stage
        if isinstance(node, ConditionNode):
            return self._handle_condition
        if isinstance(node, ParallelNode):
            return self._handle_parallel
        raise ValueError(f"Unknown node type: {type(node)}")

    async def _handle_terminal(self, node: TerminalNode):
        self.artifact_paths = node.artifact_paths
        self.result = node.result
        self.emit(Event(type="session_terminated", session_id=self.id, stage_id=node.id))
        return None

    async def _handle_stage(self, node: StageNode):
        stage_class = node.get_stage_class()
        if stage_class is None:
            raise ValueError(f"Stage class for node {node.id} not found")
        stage_instance = stage_class(stage_id=node.id, config=node.config,
                                     arguments=node.arguments, outputs=node.outputs, session=self)
        errors = []
        for retry in range(stage_instance.retries + 1):
            try:
                await asyncio.wait_for(stage_instance.run(), timeout=stage_instance.timeout)
            except asyncio.TimeoutError as e:
                self.emit(Event(type="stage_timeout", session_id=self.id, stage_id=node.id))
                self.result = "timeout"
                errors.append("timeout: " + str(e))
                continue
            except Exception as e:
                self.emit(Event(type="stage_failed", session_id=self.id, stage_id=node.id, payload={"error": str(e)}))
                self.result = "failed"
                errors.append(str(e))
                continue
            return self.pipeline.get_node(node.next) if node.next else None
        raise RuntimeError(f"Stage {node.id} failed after {stage_instance.retries} retries in session {self.id}: {errors}")

    async def _handle_condition(self, node: ConditionNode):
        next_node = None
        for condition in node.conditions:
            if condition.if_condition.evaluate(self.context):
                next_node = self.pipeline.get_node(condition.then_goto)
                break
        if not next_node and node.else_goto:
            next_node = self.pipeline.get_node(node.else_goto)

        self.emit(Event(
            type="condition_evaluated",
            session_id=self.id,
            stage_id=node.id,
            payload={"next_node": next_node.id if next_node else None},
        ))
        return next_node

    async def _handle_parallel(self, node: ParallelNode):
        tasks = []
        for branch_id in node.children:
            branch_node = self.pipeline.get_node(branch_id)
            if not isinstance(branch_node, StageNode):
                raise ValueError(f"Parallel branch node must be a StageNode, got {type(branch_node)}")

            stage_class = branch_node.get_stage_class()
            if stage_class is None:
                raise ValueError(f"Stage class for node {branch_node.id} not found")

            stage_instance = stage_class(stage_id=branch_node.id, config=branch_node.config,
                                         arguments=branch_node.arguments, outputs=branch_node.outputs, session=self)
            tasks.append(stage_instance.run())

        if node.policy == "all":
            await asyncio.gather(*tasks)
        elif node.policy == "any":
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        next_node = self.pipeline.get_node(node.next) if node.next else None
        self.emit(Event(
            type="parallel_completed",
            session_id=self.id,
            stage_id=node.id,
            payload={"next_node": next_node.id if next_node else None},
        ))
        return next_node
