"""Running child tasks concurrently, shared by `parallel` and parallel `map`.

- The parent lends its concurrency unit out while waiting for its children
  (see `Budget.released`); otherwise nested fan-out deadlocks once it is wider
  than the concurrency limit.
- A terminal in one child ends the run: the other children are cancelled and
  the parent does not continue to its `next`.
"""
from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Awaitable, Callable, Hashable, TypeVar

if TYPE_CHECKING:  # pragma: no cover
    from ..session import Session

K = TypeVar("K", bound=Hashable)
T = TypeVar("T")


async def run_children(
    session: "Session",
    children: dict[K, Callable[[], Awaitable[T]]],
    cancel_on_error: bool,
) -> tuple[dict[K, asyncio.Task], list[K]]:
    """Start all children and wait until they finish or are no longer needed
    (a failure with `cancel_on_error`, or a terminal ended the run).

    Returns the tasks by key and the keys of the cancelled ones.
    """
    async with session.budget.released("concurrency"):
        tasks = {key: asyncio.create_task(make()) for key, make in children.items()}
        pending: set[asyncio.Task] = set(tasks.values())
        try:
            while pending:
                done, pending = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED
                )
                if session.finished:
                    break
                if cancel_on_error and any(
                    not task.cancelled() and task.exception() is not None
                    for task in done
                ):
                    break
        except asyncio.CancelledError:
            for task in tasks.values():
                task.cancel()
            await asyncio.gather(*tasks.values(), return_exceptions=True)
            raise

        cancelled = [key for key, task in tasks.items() if task in pending]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
    # Retrieve every child's exception; asyncio logs unretrieved ones.
    for task in tasks.values():
        if not task.cancelled():
            task.exception()
    return tasks, cancelled


def first_failure(tasks: dict[K, asyncio.Task]) -> tuple[K, BaseException] | None:
    """The first child (in order) that raised, ignoring cancelled ones."""
    for key, task in tasks.items():
        if not task.cancelled() and task.exception() is not None:
            return key, task.exception()
    return None
