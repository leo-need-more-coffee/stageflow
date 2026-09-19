from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Awaitable, Callable

from ..context import Context

if TYPE_CHECKING:  # pragma: no cover
    from ..session import Session
    from .base import Node


def error_name(exc: BaseException) -> str:
    return type(exc).__name__


def error_full_name(exc: BaseException) -> str:
    cls = type(exc)
    return f"{cls.__module__}.{cls.__qualname__}"


def matches_error(error_equals: list[str], exc: BaseException) -> bool:
    if "*" in error_equals:
        return True
    return error_name(exc) in error_equals or error_full_name(exc) in error_equals


@dataclass(slots=True)
class Retrier:
    error_equals: list[str]
    max_attempts: int = 3
    interval_seconds: float = 1.0
    backoff_rate: float = 2.0
    max_delay_seconds: float | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "Retrier":
        return cls(
            error_equals=list(data.get("error_equals", ["*"])),
            max_attempts=data.get("max_attempts", 3),
            interval_seconds=data.get("interval_seconds", 1.0),
            backoff_rate=data.get("backoff_rate", 2.0),
            max_delay_seconds=data.get("max_delay_seconds"),
        )

    def matches(self, exc: BaseException) -> bool:
        return matches_error(self.error_equals, exc)

    def delay_for(self, attempt: int) -> float:
        delay = self.interval_seconds * (self.backoff_rate**attempt)
        if self.max_delay_seconds is not None:
            delay = min(delay, self.max_delay_seconds)
        return delay


NodeStep = Callable[[], Awaitable[tuple["Node | None", Context]]]


async def run_with_retry(
    node: "Node",
    session: "Session",
    ctx: Context,
    body: NodeStep,
) -> tuple["Node | None", Context]:
    attempts: dict[int, int] = {}
    while True:
        try:
            return await body()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            index, retrier = next(
                ((i, r) for i, r in enumerate(node.retry) if r.matches(exc)), (-1, None)
            )
            if retrier is None:
                raise
            attempt = attempts.get(index, 0) + 1
            if attempt >= retrier.max_attempts:
                raise
            attempts[index] = attempt
            session.emit_node_event(
                "node_retry",
                node,
                {
                    "attempt": attempt,
                    "of": retrier.max_attempts,
                    "error": str(exc),
                    "type": error_name(exc),
                },
            )
            await asyncio.sleep(retrier.delay_for(attempt - 1))
