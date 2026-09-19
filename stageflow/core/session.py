"""Исполнение пайплайна.

Цикл сведён к ``node, ctx = await node.execute(session, ctx)`` — ни одного
isinstance, ни одного ``_handle_*`` (см. MEMORY_MODEL.md §7). Фрейм ``vars``
ходит явным параметром, а не общим мутабельным полем сессии, иначе ветки
``parallel`` затирали бы друг друга.

Управление (stop/pause/resume) построено на ``asyncio.Event``: никаких
опрашивающих циклов со sleep — цикл исполнения гонит шаг узла наперегонки
с сигналом остановки через ``asyncio.wait``.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from typing import Any, Callable

from ..exceptions import PipelineDefinitionError
from .cel import CelEngine
from .context import Context
from .event import Event
from .inputs import InputHub
from .nodes import Node, StageNode, TerminalNode
from .pipeline import Pipeline

EventHandler = Callable[[Event], None]

#: result сессии, прерванной командой stop.
_STOPPED_RESULT = {"result": "stopped"}


@dataclass(slots=True)
class ScopeFrame:
    """Курсор фрейма внутри области: последнее состояние, дожившее до конца
    очередного узла. Нужен, потому что упавший узел уносит свой контекст с
    собой, а блоку ``try`` он нужен для обработчика."""

    ctx: Context


@dataclass(slots=True)
class SessionResult:
    """Итог прогона: артефакты, result терминального узла, история событий
    и финальный контекст."""

    artifacts: dict[str, Any]
    result: dict | None
    history: list[Event]
    context: Context

    def to_dict(self) -> dict:
        return {
            "artifacts": self.artifacts,
            "result": self.result,
            "history": [event.to_dict() for event in self.history],
            "context": self.context.to_dict(),
        }


class Session:
    """Выполняющийся экземпляр пайплайна.

    Отвечает за цикл исполнения, телеметрию и снапшоты; ожидание
    пользовательского ввода делегировано :class:`InputHub` (``self.inputs``).
    """

    def __init__(
        self,
        id: str,
        pipeline: Pipeline,
        context: Context | None = None,
        event_handler: EventHandler | None = None,
        debugger: "Debugger | None" = None,
    ):
        pipeline.validate()
        self.id = id
        self.pipeline = pipeline
        self.context = context or Context()
        self.cel = CelEngine()
        self._event_handler: EventHandler = event_handler or (lambda event: None)
        # отладчик получает управление перед каждым узлом и после него:
        # только оттуда видно точку остановки и фрейм между шагами
        # (см. core/debug.py, REFACTORING.md §14)
        self.debugger = debugger

        self.artifacts: dict[str, Any] = {}
        self.result: dict | None = None
        self.event_history: list[Event] = []
        self.inputs = InputHub(self._emit_input_event)

        self._stop_requested = asyncio.Event()
        self._running = asyncio.Event()  # снят = пауза
        self._running.set()
        self._skip_requested = False
        self._current_node_id: str | None = None

    # ------------------------------------------------------------ события

    def emit(self, event: Event) -> None:
        self.event_history.append(event)
        self._event_handler(event)

    def emit_node_event(self, type_: str, node: Node, payload: dict | None = None) -> None:
        self.emit(
            Event(type=type_, session_id=self.id, stage_id=node.id, payload=payload or {})
        )

    def _emit_input_event(self, type_: str, payload: dict) -> None:
        self.emit(Event(type=type_, session_id=self.id, payload=payload))

    # -------------------------------------------------------------- ввод

    @property
    def input_history(self) -> list[dict[str, Any]]:
        return self.inputs.history

    async def input(self, type_: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Внешняя точка входа для пользовательского ввода и команд управления
        (``type_="command"``, ``payload={"name": "stop"|"pause"|...}``)."""
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
        return await self.inputs.finish_wait(type_, fut, timeout=timeout)

    async def wait_input(
        self, type_: str, timeout: float | None = None
    ) -> dict[str, Any] | None:
        fut = self.start_wait_input(type_)
        return await self.finish_wait_input(type_, fut, timeout=timeout)

    def last_input(self, type_: str | None = None) -> dict[str, Any] | None:
        return self.inputs.last(type_)

    def is_waiting_for(self, type_: str) -> bool:
        return self.inputs.is_waiting(type_)

    # --------------------------------------------------------- управление

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

    # -------------------------------------------------------- исполнение

    async def execute_node(self, node: Node, ctx: Context) -> tuple["Node | None", Context]:
        """Исполняет один узел — ЕДИНСТВЕННЫМ путём для всех циклов сессии
        (основного, тела ``try``, ветки ``parallel``), поэтому отладчик видит
        каждый узел, где бы он ни исполнялся, и правит тот самый фрейм,
        который пойдёт дальше."""
        if self.debugger is None:
            return await node.execute(self, ctx)
        ctx = await self.debugger.before_node(self, node, ctx) or ctx
        next_node, ctx = await node.execute(self, ctx)
        ctx = await self.debugger.after_node(self, node, ctx) or ctx
        return next_node, ctx

    async def run_stage(self, node: StageNode, kwargs: dict) -> dict:
        """Запускает стадию с уже резолвнутыми аргументами и возвращает то,
        что она отдала через ``set_outputs``."""
        stage_cls = node.get_stage_class()
        stage = stage_cls(stage_id=node.id, arguments=kwargs, session=self)
        self.emit_node_event("stage_started", node, {"stage": node.stage})
        try:
            await asyncio.wait_for(stage.run(), timeout=stage.timeout)
        except asyncio.TimeoutError:
            self.emit_node_event("stage_timeout", node, {"stage": node.stage})
            raise
        except Exception as exc:  # noqa: BLE001 - телеметрия, ошибка летит дальше
            self.emit_node_event("stage_failed", node, {"stage": node.stage, "error": str(exc)})
            raise
        self.emit_node_event("stage_completed", node, {"stage": node.stage})
        return stage.collected_outputs

    async def run_subpipeline(self, node, child_ctx: Context) -> SessionResult:
        """Вложенный пайплайн исполняется отдельной сессией со своим графом
        и своим фреймом; его события проксируются наверх с пометкой узла."""
        if node.subpipeline_id not in self.pipeline.subpipelines:
            raise PipelineDefinitionError(f"Subpipeline '{node.subpipeline_id}' not found")

        data = dict(self.pipeline.subpipelines[node.subpipeline_id])
        data.setdefault("subpipelines", self.pipeline.subpipelines)
        # именованные типы родителя видны ребёнку, если он не объявил свои
        parent_types = self.pipeline.raw_json.get("types")
        if parent_types:
            data.setdefault("types", parent_types)
        child_pipeline = Pipeline.from_dict(data)

        def proxy_event(event: Event) -> None:
            # копия, а не мутация: оригинал уже лежит в истории дочерней сессии
            self.emit(
                replace(event, payload={"subpipeline_node": node.id, **(event.payload or {})})
            )

        child = Session(
            id=f"{self.id}:{node.id}",
            pipeline=child_pipeline,
            context=child_ctx,
            event_handler=proxy_event,
            debugger=self.debugger,  # иначе шаг «проваливался» бы сквозь узел
        )
        return await child.run()

    async def run_scope(
        self,
        node: "Node | None",
        ctx: Context,
        scope: frozenset[str],
        frame: "ScopeFrame | None" = None,
    ) -> tuple["Node | None", Context]:
        """Гоняет управление, пока оно остаётся внутри ``scope`` (тело ``try``).

        Возвращает узел, на котором вышли за пределы области, или None, если
        цепочка закончилась. Исключения не перехватывает — их ловит сам блок;
        ``frame`` при этом хранит последний успешно применённый фрейм, чтобы
        обработчик увидел переменные, записанные до падения (как ``except``
        в Python видит всё, присвоенное до ошибки).
        """
        while node is not None and node.id in scope:
            node, ctx = await self.execute_node(node, ctx)
            if frame is not None:
                frame.ctx = ctx
        return node, ctx

    async def run_subgraph(self, start_id: str, ctx: Context) -> Context:
        """Прогоняет цепочку узлов от ``start_id`` до её естественного конца.
        Используется ветками parallel — у каждой свой фрейм."""
        node: Node | None = self.pipeline.get_node(start_id)
        while node is not None:
            node, ctx = await self.execute_node(node, ctx)
        return ctx

    def finish(self, node: TerminalNode, ctx: Context) -> None:
        """Вызывается терминальной нодой: фиксирует артефакты и result."""
        self.artifacts = {name: ctx.get_var(name) for name in node.artifacts}
        self.result = node.result
        self.emit_node_event("session_terminated", node, {"artifacts": sorted(self.artifacts)})

    async def run(self) -> SessionResult:
        # входной контекст (посев или восстановление из снапшота) обязан
        # соответствовать объявленным типам ещё до первого узла
        self.pipeline.typesystem.check_context(self.context, f"session '{self.id}'")
        self.emit(Event(type="session_started", session_id=self.id))

        node: Node | None = (
            self.pipeline.get_node(self._current_node_id)
            if self._current_node_id
            else self.pipeline.get_entry_node()
        )
        ctx = self.context

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

        self.context = ctx
        self._current_node_id = None
        self.emit(Event(type="session_completed", session_id=self.id))
        return SessionResult(
            artifacts=self.artifacts,
            result=self.result,
            history=self.event_history,
            context=ctx,
        )

    async def _pause_gate(self) -> None:
        """На паузе блокируется до resume; stop снимает и паузу тоже."""
        while not self._running.is_set() and not self.stopped:
            await self._race(self._running.wait(), self._stop_requested.wait())

    async def _interrupted_by_stop(self, step: asyncio.Task) -> bool:
        """Ждёт завершения шага узла наперегонки с сигналом stop.
        True — шаг прерван (отменён), False — шаг завершился сам."""
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
        """``asyncio.wait(FIRST_COMPLETED)`` с уборкой проигравших задач."""
        tasks = [aw if isinstance(aw, asyncio.Task) else asyncio.create_task(aw) for aw in aws]
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()

    def _try_skip(self, node: Node) -> bool:
        """Команда skip действует только на ближайший stage-узел и только
        если его стадия объявила себя ``skipable``."""
        if not (self._skip_requested and isinstance(node, StageNode)):
            return False
        self._skip_requested = False
        if getattr(node.get_stage_class(), "skipable", False):
            self.emit_node_event("stage_skipped", node, {"stage": node.stage})
            return True
        self.emit_node_event("skip_denied", node, {"stage": node.stage})
        return False

    # ---------------------------------------------------------- snapshot

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
