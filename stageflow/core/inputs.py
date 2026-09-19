"""Ожидание пользовательского ввода — выделено из Session.

InputHub решает одну задачу: свести асинхронного поставщика сообщений
(``deliver``) с асинхронными потребителями (``start_wait``/``finish_wait``).
Сообщение, пришедшее раньше слушателя, буферизуется в ``pending`` и будет
отдано первому подписчику; сообщение при живых слушателях раздаётся ВСЕМ
ожидающим этого типа (broadcast).
"""
from __future__ import annotations

import asyncio
from typing import Any, Callable

#: Колбэк телеметрии: (тип события, payload) -> None.
EventSink = Callable[[str, dict], None]


class InputHub:
    def __init__(self, on_event: EventSink):
        self._on_event = on_event
        self._waiting: dict[str, list[asyncio.Future]] = {}
        self._pending: dict[str, list[dict[str, Any]]] = {}
        self.history: list[dict[str, Any]] = []

    # ------------------------------------------------------- поставщик

    def deliver(self, entry: dict[str, Any]) -> None:
        """Раздаёт сообщение ожидающим его типа или буферизует до подписки.

        Живой получатель — только НЕразрешённая футура: уже done-футура
        (получила прошлое сообщение, но ещё не снята через ``finish_wait``)
        принять ничего не может, и без буферизации сообщение бы терялось.
        """
        self.history.append(entry)
        live = [fut for fut in self._waiting.get(entry["type"], []) if not fut.done()]
        if live:
            for fut in live:
                fut.set_result(entry)
        else:
            self._pending.setdefault(entry["type"], []).append(entry)

    # ------------------------------------------------------ потребители

    def start_wait(self, type_: str) -> asyncio.Future:
        """Подписка на ввод типа ``type_``. Если сообщение уже в буфере —
        футура возвращается сразу разрешённой."""
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        pending = self._pending.get(type_)
        if pending:
            fut.set_result(pending.pop(0))
            return fut
        self._waiting.setdefault(type_, []).append(fut)
        self._on_event("waiting_for_input", {"type": type_})
        return fut

    async def finish_wait(
        self, type_: str, fut: asyncio.Future, timeout: float | None = None
    ) -> dict[str, Any] | None:
        """Дожидается результата подписки; по таймауту возвращает None и
        эмитит ``input_timeout``. Подписка снимается в любом случае."""
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            self._on_event("input_timeout", {"type": type_})
            return None
        finally:
            self._discard(type_, fut)

    def _discard(self, type_: str, fut: asyncio.Future) -> None:
        waiters = self._waiting.get(type_)
        if waiters and fut in waiters:
            waiters.remove(fut)
            if not waiters:
                del self._waiting[type_]

    # -------------------------------------------------------- состояние

    def is_waiting(self, type_: str) -> bool:
        return bool(self._waiting.get(type_))

    def last(self, type_: str | None = None) -> dict[str, Any] | None:
        if type_ is None:
            return self.history[-1] if self.history else None
        return next(
            (entry for entry in reversed(self.history) if entry["type"] == type_),
            None,
        )
