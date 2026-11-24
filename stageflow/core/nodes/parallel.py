from __future__ import annotations

from typing import TYPE_CHECKING

from ...exceptions import BranchError
from ...i18n import _
from ..budget import BudgetExceeded
from ..context import Context
from .base import Node, register_node
from .fanout import first_failure, run_children
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
            errors.append(_("{node}: at least one branch is required", node=self.id))
        for branch in self.branches:
            if not pipeline.has_node(branch["entry"]):
                errors.append(
                    _("{node}: entry of branch '{branch}' -> "
                      "'{target}' not found in the graph",
                      node=self.id, branch=branch["id"], target=branch["entry"])
                )
        if self.next and not pipeline.has_node(self.next):
            errors.append(
                _("{node}: next '{target}' not found in the graph",
                  node=self.id, target=self.next)
            )
        return errors

    async def execute(self, session: "Session", ctx: Context) -> tuple[Node | None, Context]:
        async def body() -> tuple[Node | None, Context]:
            baseline_keys = ctx.var_names()

            tasks, cancelled = await run_children(
                session,
                {
                    branch["id"]: (lambda entry=branch["entry"]: self._branch(session, entry, ctx))
                    for branch in self.branches
                },
                self.cancel_on_error,
            )
            if cancelled:
                session.emit_node_event(
                    "parallel_cancelled", self, {"branches": sorted(cancelled)}
                )
            failure = first_failure(tasks)
            if failure is not None:
                branch_id, exc = failure
                # BranchError is an ordinary exception and a `try` block can
                # catch it. Wrapping the budget in one would hand the tenant
                # the way out the budget exists to close
                if isinstance(exc, BudgetExceeded):
                    raise exc
                raise BranchError(
                    _("branch '{branch}' failed: {reason}", branch=branch_id, reason=exc)
                ) from exc
            # a terminal in a branch has ended the run: the other branches are
            # cancelled, and there is no `next` to go on to
            if session.finished:
                return None, ctx

            results = {bid: task.result() for bid, task in tasks.items()}
            merged = self._merge(ctx, baseline_keys, results)
            merged = self.apply_expose(session, merged)
            session.emit_node_event(
                "parallel_completed",
                self,
                {
                    "merged": sorted(merged.var_names() - baseline_keys),
                    "dropped": self._dropped(ctx, baseline_keys, results),
                },
            )
            return self._goto(session, self.next), merged

        return await run_with_retry(self, session, ctx, body)

    @staticmethod
    async def _branch(session: "Session", entry: str, ctx: Context) -> Context:
        """One branch, holding a unit of whatever concurrency is allowed.

        More branches than the allowance is not an error: they take turns.
        """
        async with session.budget.slot("concurrency"):
            return await session.run_subgraph(entry, ctx)

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
                        _("{node}: branches '{first}' and '{second}' both write {name}",
                          node=self.id, first=owner[key], second=branch_id, name=key)
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
