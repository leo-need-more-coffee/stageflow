import asyncio
from typing import Callable, Any

from .context import Context
from .event import Event
from .pipeline import Pipeline
from .node import StageNode, ConditionNode, ParallelNode, TerminalNode, SubPipelineNode


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
        self._waiting: dict[str, list[asyncio.Future]] = {}

        self._stopped = False
        self._paused = False
        self._skip_requested = False
        self._current_node_id: str | None = None

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

        if type_ in self._waiting:
            for fut in list(self._waiting[type_]):
                if not fut.done():
                    fut.set_result(entry)
        return entry

    async def wait_input(self, type_: str, timeout: float | None = None):
        fut = asyncio.get_event_loop().create_future()
        self._waiting.setdefault(type_, []).append(fut)
        self.emit(Event(type="waiting_for_input", session_id=self.id, payload={"type": type_}))
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            self.emit(Event(type="input_timeout", session_id=self.id, payload={"type": type_}))
            return None
        finally:
            waiters = self._waiting.get(type_)
            if waiters and fut in waiters:
                waiters.remove(fut)
                if not waiters:
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

    def snapshot(self) -> dict:
        return {
            "session_id": self.id,
            "current_node_id": self._current_node_id,
            "result": self.result,
            "artifact_paths": list(self.artifact_paths),
            "context": self.context.to_dict(),
            "event_history": [e.to_dict() for e in self.event_history],
            "input_history": list(self.input_history),
            "stopped": self._stopped,
            "paused": self._paused,
            "skip_requested": self._skip_requested,
            "pipeline": self.pipeline.raw_json,
        }

    @classmethod
    def from_snapshot(
        cls,
        snapshot: dict,
        pipeline: Pipeline | None = None,
        event_handler: Callable[[Event], None] | None = None,
    ) -> "Session":
        pipe = pipeline
        if pipe is None:
            raw = snapshot.get("pipeline")
            if raw:
                pipe = Pipeline.from_dict(raw)
        if pipe is None:
            raise ValueError("Pipeline is required to restore session")
        ctx = Context.from_dict(snapshot.get("context", {}))
        sess = cls(
            id=snapshot.get("session_id"),
            pipeline=pipe,
            context=ctx,
            event_handler=event_handler,
        )
        sess._current_node_id = snapshot.get("current_node_id")
        sess.result = snapshot.get("result")
        sess.artifact_paths = snapshot.get("artifact_paths", [])
        sess.input_history = snapshot.get("input_history", [])
        sess._stopped = snapshot.get("stopped", False)
        sess._paused = snapshot.get("paused", False)
        sess._skip_requested = snapshot.get("skip_requested", False)
        sess.event_history = [Event.from_dict(e) for e in snapshot.get("event_history", [])]
        return sess

    async def run(self) -> SessionResult:
        self.emit(Event(type="session_started", session_id=self.id))
        node = self.pipeline.get_node(self._current_node_id) if self._current_node_id else self.pipeline.get_entry_node()

        while node:
            self._current_node_id = node.id
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

        self._current_node_id = None
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
        if isinstance(node, SubPipelineNode):
            return self._handle_subpipeline
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
                errors.append("timeout: " + str(e))
                continue
            except Exception as e:
                self.emit(Event(type="stage_failed", session_id=self.id, stage_id=node.id, payload={"error": str(e)}))
                errors.append(str(e))
                continue
            return self.pipeline.get_node(node.next) if node.next else None
        return self.pipeline.get_node(node.fallback) if node.fallback else None

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
            if isinstance(branch_node, StageNode):
                stage_class = branch_node.get_stage_class()
                if stage_class is None:
                    raise ValueError(f"Stage class for node {branch_node.id} not found")

                stage_instance = stage_class(stage_id=branch_node.id, config=branch_node.config,
                                             arguments=branch_node.arguments, outputs=branch_node.outputs, session=self)
                tasks.append(stage_instance.run())
            elif isinstance(branch_node, SubPipelineNode):
                tasks.append(self._run_subpipeline(branch_node))
            else:
                raise ValueError(f"Parallel branch node must be a StageNode or SubPipelineNode, got {type(branch_node)}")

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

    async def _run_subpipeline(self, node: SubPipelineNode):
        if node.subpipeline_id not in self.pipeline.subpipelines:
            raise ValueError(f"Subpipeline '{node.subpipeline_id}' not found")
        subpipeline_data = dict(self.pipeline.subpipelines[node.subpipeline_id])
        subpipeline_data["subpipelines"] = self.pipeline.subpipelines
        subpipeline = Pipeline.from_dict(subpipeline_data)
        child_ctx = Context(payload={})
        for child_path, parent_path in node.inputs.items():
            child_ctx.set(child_path, self.context.get(parent_path))

        def proxy_event(event: Event):
            event.payload = {"subpipeline_node": node.id, **(event.payload or {})}
            self.emit(event)

        child_session = Session(
            id=f"{self.id}:{node.id}",
            pipeline=subpipeline,
            context=child_ctx,
            event_handler=proxy_event,
        )
        result = await child_session.run()
        for parent_path, child_art in node.artifact_outputs.items():
            if child_art not in result.artifacts:
                raise ValueError(f"Artifact '{child_art}' not found in subpipeline '{node.subpipeline_id}'")
            self.context.set(parent_path, result.artifacts[child_art])
        if node.result_output:
            self.context.set(node.result_output, result.result)
        return result

    async def _handle_subpipeline(self, node: SubPipelineNode):
        await self._run_subpipeline(node)
        next_node = self.pipeline.get_node(node.next) if node.next else None
        self.emit(Event(
            type="subpipeline_completed",
            session_id=self.id,
            stage_id=node.id,
            payload={"next_node": next_node.id if next_node else None, "subpipeline_id": node.subpipeline_id},
        ))
        return next_node

    async def _handle_subpipeline(self, node: SubPipelineNode):
        if node.subpipeline_id not in self.pipeline.subpipelines:
            raise ValueError(f"Subpipeline '{node.subpipeline_id}' not found")
        subpipeline_data = dict(self.pipeline.subpipelines[node.subpipeline_id])
        subpipeline_data["subpipelines"] = self.pipeline.subpipelines
        subpipeline = Pipeline.from_dict(subpipeline_data)
        # Build child context with isolation.
        child_ctx = Context(payload={})
        for child_path, parent_path in node.inputs.items():
            child_ctx.set(child_path, self.context.get(parent_path))

        # Proxy events with subpipeline info.
        def proxy_event(event: Event):
            event.payload = {"subpipeline_node": node.id, **(event.payload or {})}
            self.emit(event)

        child_session = Session(
            id=f"{self.id}:{node.id}",
            pipeline=subpipeline,
            context=child_ctx,
            event_handler=proxy_event,
        )
        result = await child_session.run()

        # Map artifacts back.
        for parent_path, child_art in node.artifact_outputs.items():
            if child_art not in result.artifacts:
                raise ValueError(f"Artifact '{child_art}' not found in subpipeline '{node.subpipeline_id}'")
            self.context.set(parent_path, result.artifacts[child_art])
        if node.result_output:
            self.context.set(node.result_output, result.result)

        next_node = self.pipeline.get_node(node.next) if node.next else None
        self.emit(Event(
            type="subpipeline_completed",
            session_id=self.id,
            stage_id=node.id,
            payload={"next_node": next_node.id if next_node else None, "subpipeline_id": node.subpipeline_id},
        ))
        return next_node
