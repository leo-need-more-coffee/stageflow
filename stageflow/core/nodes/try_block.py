from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ...exceptions import PipelineDefinitionError
from ..context import Context
from .base import Node, register_node
from .recovery import error_full_name, error_name, matches_error

if TYPE_CHECKING:  # pragma: no cover
    from ..pipeline import Pipeline
    from ..session import Session


@dataclass(slots=True)
class ExceptHandler:
    error_equals: list[str]
    next: str
    result_var: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "ExceptHandler":
        if "next" not in data:
            raise PipelineDefinitionError("except: field 'next' is required")
        return cls(
            error_equals=list(data.get("error_equals", ["*"])),
            next=data["next"],
            result_var=data.get("result_var"),
        )

    def matches(self, exc: BaseException) -> bool:
        return matches_error(self.error_equals, exc)

    def error_payload(self, exc: BaseException, node_id: str) -> dict:
        return {
            "type": error_name(exc),
            "full_type": error_full_name(exc),
            "message": str(exc),
            "node": node_id,
        }


@register_node("try")
class TryNode(Node):
    def __init__(
        self,
        id: str,
        body: str,
        handlers: list[ExceptHandler] | None = None,
        next: str | None = None,
        **common,
    ):
        super().__init__(id, **common)
        self.body = body
        self.handlers = handlers or []
        self.next = next
        self._scope: frozenset[str] | None = None

    @classmethod
    def _parse(cls, data: dict) -> "TryNode":
        body = data.get("body")
        if not body:
            raise PipelineDefinitionError(f"Node '{data.get('id')}': field 'body' is required")
        return cls(
            body=body,
            handlers=[ExceptHandler.from_dict(h) for h in data.get("except", [])],
            next=data.get("next"),
            **Node._common(data),
        )

    def scope(self, pipeline: "Pipeline") -> frozenset[str]:
        if self._scope is None:
            after = pipeline.reachable([self.next]) if self.next else frozenset()
            self._scope = pipeline.reachable([self.body], stop_at=after) - {self.id}
        return self._scope

    def order_targets(self) -> list[str]:
        targets = [self.body, *(h.next for h in self.handlers)]
        if self.next:
            targets.append(self.next)
        return [t for t in targets if t]

    def validate(self, pipeline: "Pipeline") -> list[str]:
        errors = self._validate_common(pipeline)
        if not pipeline.has_node(self.body):
            errors.append(f"{self.id}: body '{self.body}' not found in the graph")
        if not self.handlers:
            errors.append(f"{self.id}: at least one handler is required in 'except'")
        for handler in self.handlers:
            if not pipeline.has_node(handler.next):
                errors.append(f"{self.id}: except.next '{handler.next}' not found in the graph")
            elif pipeline.has_node(self.body) and handler.next in self.scope(pipeline):
                errors.append(
                    f"{self.id}: handler '{handler.next}' is inside the block body"
                )
        if self.next and not pipeline.has_node(self.next):
            errors.append(f"{self.id}: next '{self.next}' not found in the graph")
        return errors

    async def execute(self, session: "Session", ctx: Context) -> tuple[Node | None, Context]:
        scope = self.scope(session.pipeline)
        ctx = self.apply_expose(session, ctx)
        session.emit_node_event("try_entered", self, {"body": self.body, "scope": sorted(scope)})

        from ..session import ScopeFrame

        frame = ScopeFrame(ctx)
        try:
            node, ctx = await session.run_scope(
                session.pipeline.get_node(self.body), ctx, scope, frame
            )
        except Exception as exc:  # noqa: BLE001
            handler = next((h for h in self.handlers if h.matches(exc)), None)
            if handler is None:
                raise
            ctx = frame.ctx
            session.emit_node_event(
                "try_caught",
                self,
                {"error": str(exc), "type": error_name(exc), "next": handler.next},
            )
            if handler.result_var:
                ctx = ctx.with_var(handler.result_var, handler.error_payload(exc, self.id))
            return session.pipeline.get_node(handler.next), ctx

        if node is not None:
            return node, ctx
        session.emit_node_event("try_completed", self, {"body": self.body})
        return self._goto(session, self.next), ctx
