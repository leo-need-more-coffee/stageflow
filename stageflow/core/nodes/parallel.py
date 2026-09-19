from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from ...exceptions import BranchError
from ..context import Context
from .base import Node, register_node
from .recovery import run_with_retry

if TYPE_CHECKING:  # pragma: no cover
    from ..pipeline import Pipeline
    from ..session import Session


def _differs(left, right) -> bool:
    if left is right:
        return False
    try:
        return bool(left != right)
    except Exception:  # noqa: BLE001
        return True


@register_node("parallel")
class ParallelNode(Node):
    def __init__(
        self,
        id: str,
        branches: list[dict],
        next: str | None = None,
        cancel_on_error: bool = True,
        **common,
    ):
        super().__init__(id, **common)
        self.branches = branches
        self.next = next
        self.cancel_on_error = cancel_on_error

    @classmethod
    def _parse(cls, data: dict) -> "ParallelNode":
        normalized = [
            {"id": b.get("id", b["entry"]), "entry": b["entry"]} if isinstance(b, dict)
            else {"id": b, "entry": b}
            for b in data.get("branches", [])
        ]
        return cls(
            branches=normalized,
            next=data.get("next"),
            cancel_on_error=data.get("cancel_on_error", True),
            **Node._common(data),
        )

    def order_targets(self) -> list[str]:
        targets = [b["entry"] for b in self.branches if b.get("entry")]
        return targets + ([self.next] if self.next else [])

    def validate(self, pipeline: "Pipeline") -> list[str]:
        errors = self._validate_common(pipeline)
        if not self.branches:
            errors.append(f"{self.id}: нужна хотя бы одна ветка")
        for branch in self.branches:
            if not pipeline.has_node(branch["entry"]):
                errors.append(
                    f"{self.id}: вход ветки '{branch['id']}' -> "
                    f"'{branch['entry']}' не найден в графе"
                )
        if self.next and not pipeline.has_node(self.next):
            errors.append(f"{self.id}: next '{self.next}' не найден в графе")
        return errors

    async def execute(self, session: "Session", ctx: Context) -> tuple[Node | None, Context]:
        async def body() -> tuple[Node | None, Context]:
            baseline_keys = ctx.var_names()

            tasks = {
                branch["id"]: asyncio.create_task(
                    session.run_subgraph(branch["entry"], ctx)
                )
                for branch in self.branches
            }
            await self._wait_branches(session, tasks)

            failure = next(
                (
                    (branch_id, task.exception())
                    for branch_id, task in tasks.items()
                    if not task.cancelled() and task.exception() is not None
                ),
                None,
            )
            if failure is not None:
                branch_id, exc = failure
                raise BranchError(f"ветка '{branch_id}' упала: {exc}") from exc

            merged = self._merge(
                ctx, baseline_keys, {bid: task.result() for bid, task in tasks.items()}
            )
            merged = self.apply_expose(session, merged)
            session.emit_node_event(
                "parallel_completed",
                self,
                {
                    "merged": sorted(merged.var_names() - baseline_keys),
                    "dropped": self._dropped(
                        ctx, baseline_keys, {bid: task.result() for bid, task in tasks.items()}
                    ),
                },
            )
            return self._goto(session, self.next), merged

        return await run_with_retry(self, session, ctx, body)

    async def _wait_branches(self, session: "Session", tasks: dict[str, asyncio.Task]) -> None:
        mode = asyncio.FIRST_EXCEPTION if self.cancel_on_error else asyncio.ALL_COMPLETED
        try:
            _, pending = await asyncio.wait(tasks.values(), return_when=mode)
        except asyncio.CancelledError:
            for task in tasks.values():
                task.cancel()
            raise
        if pending:
            cancelled = [branch_id for branch_id, task in tasks.items() if task in pending]
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            session.emit_node_event("parallel_cancelled", self, {"branches": sorted(cancelled)})

    def _merge(
        self,
        base: Context,
        baseline_keys: frozenset[str],
        branch_contexts: dict[str, Context],
    ) -> Context:
        merged = base
        owner: dict[str, str] = {}
        for branch_id, branch_ctx in branch_contexts.items():
            for key in sorted(branch_ctx.var_names() - baseline_keys):
                if key in owner:
                    raise BranchError(
                        f"{self.id}: ветки '{owner[key]}' и '{branch_id}' "
                        f"обе пишут {key}"
                    )
                owner[key] = branch_id
                merged = merged.with_var(key, branch_ctx.get_var(key))
        return merged

    @staticmethod
    def _dropped(
        base: Context,
        baseline_keys: frozenset[str],
        branch_contexts: dict[str, Context],
    ) -> list[str]:
        dropped = []
        for branch_id, branch_ctx in branch_contexts.items():
            for key in sorted(branch_ctx.var_names() & baseline_keys):
                if _differs(branch_ctx.get_var(key), base.get_var(key)):
                    dropped.append(f"{branch_id}.{key}")
        return sorted(dropped)
