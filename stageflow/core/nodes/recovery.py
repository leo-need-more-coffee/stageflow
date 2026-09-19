"""Повторы при ошибках: политика ``retry`` узла.

См. MEMORY_MODEL.md §6. Retry — свойство конкретной операции («этот HTTP-вызов
стоит повторить трижды»), поэтому живёт на узле. Обработка ошибок, наоборот,
блочная и описана в ``nodes/try_block.py``: узел ``try`` накрывает область
графа, как ``try/except`` в Python.

Порядок такой: сначала исчерпываются повторы узла, и только если он всё-таки
упал, исключение всплывает наружу — к ближайшему объемлющему ``try``.
"""
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
    """``*`` ловит всё; остальное сверяется и с коротким именем класса, и с
    полным путём — короткое удобно писать, полное снимает коллизии имён."""
    if "*" in error_equals:
        return True
    return error_name(exc) in error_equals or error_full_name(exc) in error_equals


@dataclass(slots=True)
class Retrier:
    """Политика повторов: какие ошибки ловим и с какой выдержкой повторяем.

    ``max_attempts`` — сколько раз узел будет запущен ВСЕГО, вместе с первым
    запуском: ``max_attempts: 3`` — это один запуск и два повтора. Имя поля
    читается буквально, чтобы «три попытки» в описании пайплайна означали
    ровно три обращения к внешнему сервису.
    """

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
    """Обвязка для узлов, которые реально что-то исполняют.

    ``body()`` замыкается на ИСХОДНЫЙ ``ctx``, поэтому каждая попытка стартует
    с чистого фрейма, а не с недоделанного предыдущей. Когда повторы
    исчерпаны, исключение уходит наверх — его поймает объемлющий ``try``.

    Счётчик попыток — у каждого retrier'а свой (ключ словаря — его индекс в
    ``node.retry``). Иначе политика, сработавшая первой, съедала бы повторы
    соседней: два ``TimeoutError`` подряд обнуляли бы лимит для первого же
    ``ValueError``, хотя тот не повторялся ещё ни разу.
    """
    attempts: dict[int, int] = {}
    while True:
        try:
            return await body()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - это и есть граница повторов
            index, retrier = next(
                ((i, r) for i, r in enumerate(node.retry) if r.matches(exc)), (-1, None)
            )
            if retrier is None:
                raise
            # запуск, который только что упал, — по счёту именно этого retrier'а
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
