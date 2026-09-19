"""Пошаговая отладка сессии: где мы сейчас, шаг вперёд, правка фрейма.

Зачем это в ядре, а не в инструменте поверх него. Отлаживать пайплайн — значит
останавливаться МЕЖДУ узлами: смотреть фрейм в точке остановки, менять его и
идти дальше. Снаружи такую остановку не сделать: ``Session.pause()`` снимает
шаг в произвольном месте (гейт стоит перед узлом, но управлять им можно только
целиком — «идём» или «стоим»), а фрейм вообще недоступен — ``run()`` держит
``ctx`` в локальной переменной и передаёт от узла к узлу, поэтому подмена
``session.context`` посреди прогона ничего не меняет.

Поэтому у сессии появилась одна точка расширения — отладчик, которому она
отдаёт управление перед каждым узлом и после него:

    debugger = StepDebugger(mode="step", on_event=print)
    session = Session("s1", pipeline, debugger=debugger)
    task = asyncio.create_task(session.run())
    debugger.step()                      # пустить ровно один узел
    debugger.set_vars({"n": 42})         # правка применится на следующем узле
    debugger.resume()                    # дальше без остановок

Хук возвращает контекст, поэтому правка фрейма — это не мутация чужого
состояния, а тот же способ, которым его меняют узлы: новый ``Context``.

Управление приходит не из цикла событий (в UI — из HTTP-обработчика, то есть
из другого потока), поэтому команды потокобезопасны: счётчики под локом, а
будильник дёргается через ``loop.call_soon_threadsafe``.

Отладчик наследуется дочерними сессиями (``run_subpipeline``), так что шаг
работает и внутри субпайплайна, и внутри тела ``try``, и в ветках ``parallel``
— все они исполняют узлы через ту же точку.
"""
from __future__ import annotations

import asyncio
import threading
from typing import Any, Callable

from .context import Context

RUN = "run"
STEP = "step"


class StepDebugger:
    """Отладчик сессии: пауза перед каждым узлом, задержка, правка фрейма.

    ``mode="run"`` — идём без остановок (но соблюдаем ``delay``);
    ``mode="step"`` — перед каждым узлом ждём команды :meth:`step`.
    """

    def __init__(
        self,
        *,
        mode: str = RUN,
        delay: float = 0.0,
        on_event: Callable[[dict], None] | None = None,
    ):
        self.mode = STEP if mode == STEP else RUN
        self.delay = max(0.0, float(delay))
        self._on_event = on_event or (lambda event: None)

        self._lock = threading.Lock()
        self._go = asyncio.Event()
        self._steps = 0
        self._pending_set: dict[str, Any] = {}
        self._pending_drop: set[str] = set()
        self._loop: asyncio.AbstractEventLoop | None = None

        self.node: str | None = None       # узел, на котором стоим
        self.vars: dict[str, Any] = {}      # фрейм в точке остановки
        self.waiting = False                # ждём команду step

    # ------------------------------------------------------------ состояние

    @property
    def state(self) -> dict:
        return {
            "mode": self.mode,
            "delay": self.delay,
            "node": self.node,
            "vars": dict(self.vars),
            "waiting": self.waiting,
        }

    # ------------------------------------------------- точки расширения Session

    async def before_node(self, session, node, ctx: Context) -> Context:
        """Вызывается перед узлом: сообщает, где мы, при необходимости ждёт
        команду и применяет накопленные правки фрейма."""
        self._loop = asyncio.get_running_loop()
        self.node = node.id
        self.vars = dict(ctx.vars)
        self._emit("node_enter", node=node.id, node_type=node.type, vars=self.vars)

        if self.mode == STEP:
            await self._wait_step()
        elif self.delay:
            # задержка ПЕРЕД узлом, а не после: подсветка на графе должна
            # успеть показать, что сейчас исполняется, а не что уже прошло
            await asyncio.sleep(self.delay)

        ctx = self._apply_pending(session, ctx)
        self.vars = dict(ctx.vars)
        return ctx

    async def after_node(self, session, node, ctx: Context) -> Context:
        """Вызывается после узла: фрейм уже новый, его и показываем."""
        self.vars = dict(ctx.vars)
        self._emit("node_exit", node=node.id, node_type=node.type, vars=self.vars)
        return ctx

    # ------------------------------------------------------------ управление

    def step(self, count: int = 1) -> None:
        """Пустить ``count`` узлов и снова встать."""
        with self._lock:
            self.mode = STEP
            self._steps += max(1, int(count))
        self._wake()

    def resume(self) -> None:
        """Дальше без остановок."""
        with self._lock:
            self.mode = RUN
            self._steps = 0
        self._wake()

    def pause(self) -> None:
        """Встать перед следующим узлом (текущий доигрывает: прерывать шаг
        посередине — это уже ``Session.stop()``, другая операция)."""
        with self._lock:
            self.mode = STEP
            self._steps = 0

    def set_delay(self, seconds: float) -> None:
        self.delay = max(0.0, float(seconds))

    def set_vars(self, values: dict[str, Any] | None = None, drop=()) -> None:
        """Записать/удалить переменные фрейма. Применится перед следующим
        узлом: между узлами фрейм принадлежит исполнению, и вклиниваться в
        середину шага нельзя — иначе узел увидел бы половину правки."""
        with self._lock:
            self._pending_set.update(values or {})
            self._pending_drop.update(drop or ())
            for name in (values or {}):
                self._pending_drop.discard(name)
            for name in drop or ():
                self._pending_set.pop(name, None)

    # ------------------------------------------------------------- внутреннее

    async def _wait_step(self) -> None:
        while True:
            with self._lock:
                if self.mode == RUN:
                    return
                if self._steps > 0:
                    self._steps -= 1
                    self._go.clear()
                    return
                self.waiting = True
            self._emit("paused", node=self.node, vars=dict(self.vars))
            await self._go.wait()
            with self._lock:
                self.waiting = False
                self._go.clear()

    def _apply_pending(self, session, ctx: Context) -> Context:
        with self._lock:
            values = self._pending_set
            drops = self._pending_drop
            self._pending_set = {}
            self._pending_drop = set()
        if not values and not drops:
            return ctx

        for name in sorted(drops):
            ctx = ctx.without_var(name)
            self._emit("var_dropped", name=name)
        types = getattr(session.pipeline, "typesystem", None)
        for name, value in values.items():
            try:
                if types is not None:
                    types.check_write(name, value, f"отладчик перед узлом '{self.node}'")
            except Exception as exc:  # noqa: BLE001 - правка отвергается, сессия живёт
                # уронить сессию из-за опечатки в панели переменных нельзя:
                # отладчик для того и нужен, чтобы попробовать ещё раз
                self._emit("var_rejected", name=name, error=str(exc))
                continue
            ctx = ctx.with_var(name, value)
            self._emit("var_set", name=name, value=value)
        return ctx

    def _wake(self) -> None:
        loop = self._loop
        if loop is None or loop.is_closed():
            self._go.set()
            return
        try:
            loop.call_soon_threadsafe(self._go.set)
        except RuntimeError:  # pragma: no cover - цикл уже остановлен
            self._go.set()

    def _emit(self, type_: str, **payload: Any) -> None:
        # `type` — за событием; тип узла едет отдельным ключом `node_type`,
        # иначе payload перебивал бы род события своим значением
        self._on_event({**payload, "type": type_})
