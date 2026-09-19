from __future__ import annotations

import asyncio
import threading
from typing import Any, Callable

from .context import Context

RUN = "run"
STEP = "step"


class StepDebugger:
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

        self.node: str | None = None
        self.vars: dict[str, Any] = {}
        self.waiting = False

    @property
    def state(self) -> dict:
        return {
            "mode": self.mode,
            "delay": self.delay,
            "node": self.node,
            "vars": dict(self.vars),
            "waiting": self.waiting,
        }

    async def before_node(self, session, node, ctx: Context) -> Context:
        self._loop = asyncio.get_running_loop()
        self.node = node.id
        self.vars = dict(ctx.vars)
        self._emit("node_enter", node=node.id, node_type=node.type, vars=self.vars)

        if self.mode == STEP:
            await self._wait_step()
        elif self.delay:
            await asyncio.sleep(self.delay)

        ctx = self._apply_pending(session, ctx)
        self.vars = dict(ctx.vars)
        return ctx

    async def after_node(self, session, node, ctx: Context) -> Context:
        self.vars = dict(ctx.vars)
        self._emit("node_exit", node=node.id, node_type=node.type, vars=self.vars)
        return ctx

    def step(self, count: int = 1) -> None:
        with self._lock:
            self.mode = STEP
            self._steps += max(1, int(count))
        self._wake()

    def resume(self) -> None:
        with self._lock:
            self.mode = RUN
            self._steps = 0
        self._wake()

    def pause(self) -> None:
        with self._lock:
            self.mode = STEP
            self._steps = 0

    def set_delay(self, seconds: float) -> None:
        self.delay = max(0.0, float(seconds))

    def set_vars(self, values: dict[str, Any] | None = None, drop=()) -> None:
        with self._lock:
            self._pending_set.update(values or {})
            self._pending_drop.update(drop or ())
            for name in (values or {}):
                self._pending_drop.discard(name)
            for name in drop or ():
                self._pending_set.pop(name, None)

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
                    types.check_write(name, value, f"debugger before node '{self.node}'")
            except Exception as exc:  # noqa: BLE001
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
        except RuntimeError:  # pragma: no cover
            self._go.set()

    def _emit(self, type_: str, **payload: Any) -> None:
        self._on_event({**payload, "type": type_})
