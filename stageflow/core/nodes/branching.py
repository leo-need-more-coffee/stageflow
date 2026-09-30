from __future__ import annotations

from typing import TYPE_CHECKING

from ...i18n import _
from ..context import Context
from .base import Node, register_node

if TYPE_CHECKING:  # pragma: no cover
    from ..pipeline import Pipeline
    from ..session import Session


@register_node("condition")
class ConditionNode(Node):
    def __init__(self, id: str, condition: str, then: str, else_: str | None = None, **common):
        super().__init__(id, **common)
        self.condition = condition
        self.then = then
        self.else_ = else_

    @classmethod
    def _parse(cls, data: dict) -> "ConditionNode":
        return cls(
            condition=data["condition"],
            then=data["then"],
            else_=data.get("else"),
            **Node._common(data),
        )

    def order_targets(self) -> list[str]:
        return [t for t in (self.then, self.else_) if t]

    def validate(self, pipeline: "Pipeline") -> list[str]:
        errors = self._validate_common(pipeline)
        if not pipeline.has_node(self.then):
            errors.append(
                _("{node}: then '{target}' not found in the graph",
                  node=self.id, target=self.then)
            )
        if self.else_ and not pipeline.has_node(self.else_):
            errors.append(
                _("{node}: else '{target}' not found in the graph",
                  node=self.id, target=self.else_)
            )
        return errors

    async def execute(self, session: "Session", ctx: Context) -> tuple[Node | None, Context]:
        taken = bool(session.cel.eval(self.condition, ctx))
        target = self.then if taken else self.else_
        ctx = self.apply_expose(session, ctx)
        session.emit_node_event("condition_evaluated", self, {"result": taken, "next": target})
        return self._goto(session, target), ctx


@register_node("switch")
class SwitchNode(Node):
    def __init__(self, id: str, cases: list[dict], default: str | None = None, **common):
        super().__init__(id, **common)
        self.cases = cases
        self.default = default

    @classmethod
    def _parse(cls, data: dict) -> "SwitchNode":
        return cls(
            cases=data.get("cases", []),
            default=data.get("default"),
            **Node._common(data),
        )

    def order_targets(self) -> list[str]:
        targets = [case["next"] for case in self.cases if case.get("next")]
        return targets + ([self.default] if self.default else [])

    def validate(self, pipeline: "Pipeline") -> list[str]:
        errors = self._validate_common(pipeline)
        for case in self.cases:
            if "when" not in case or "next" not in case:
                errors.append(
                    _("{node}: every case needs 'when' and 'next'", node=self.id)
                )
                continue
            if not pipeline.has_node(case["next"]):
                errors.append(
                    _("{node}: case next '{target}' not found in the graph",
                      node=self.id, target=case["next"])
                )
        if self.default and not pipeline.has_node(self.default):
            errors.append(
                _("{node}: default '{target}' not found in the graph",
                  node=self.id, target=self.default)
            )
        return errors

    async def execute(self, session: "Session", ctx: Context) -> tuple[Node | None, Context]:
        target = self.default
        matched: str | None = None
        for case in self.cases:
            if session.cel.eval(case["when"], ctx):
                target, matched = case["next"], case["when"]
                break
        ctx = self.apply_expose(session, ctx)
        session.emit_node_event("switch_evaluated", self, {"matched": matched, "next": target})
        return self._goto(session, target), ctx
