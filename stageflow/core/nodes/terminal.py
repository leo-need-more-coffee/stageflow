"""Узел ``terminal`` — конец исполнения: фиксирует result и артефакты."""
from __future__ import annotations

from typing import TYPE_CHECKING

from ..context import Context
from .base import Node, register_node

if TYPE_CHECKING:  # pragma: no cover
    from ..session import Session


@register_node("terminal")
class TerminalNode(Node):
    def __init__(
        self,
        id: str,
        artifacts: list[str] | None = None,
        result: dict | None = None,
        **common,
    ):
        super().__init__(id, **common)
        self.artifacts = artifacts or []
        self.result = result

    @classmethod
    def _parse(cls, data: dict) -> "TerminalNode":
        return cls(
            artifacts=data.get("artifacts", []),
            result=data.get("result"),
            **Node._common(data),
        )

    async def execute(self, session: "Session", ctx: Context) -> tuple[Node | None, Context]:
        ctx = self.apply_expose(session, ctx)
        session.finish(self, ctx)
        return None, ctx
