"""Running children concurrently: what `parallel` and a parallel `map` share.

Two things both of them have to get right, and used to get right separately
or not at all.

The parent gives its unit of concurrency back while it waits. A branch of a
`parallel` that holds a unit and then waits for a `map` whose items each need
one is a deadlock the moment the nesting is wider than the allowance — see
`Budget.released`. The children are created inside that block, so they start
out holding nothing.

A terminal in one child ends the run, the way a terminal inside a `try` or a
`map` does. The siblings are cancelled rather than left to run on into a
graph that has finished, and the parent does not carry on to its `next`.
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
    """Start every child, wait until they are done or no longer wanted.

    @returns the tasks by key, and the keys of those that were cancelled.
    The caller reads results and exceptions off the tasks.
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
    return tasks, cancelled


def first_failure(tasks: dict[K, asyncio.Task]) -> tuple[K, BaseException] | None:
    """The first child, in the order given, that raised — not one cancelled."""
    for key, task in tasks.items():
        if not task.cancelled() and task.exception() is not None:
            return key, task.exception()
    return None
