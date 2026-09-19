"""Узел ``try`` — блочная обработка ошибок, как ``try/except`` в Python.

Вместо того чтобы вешать обработчик на каждый узел, ``try`` накрывает целую
ОБЛАСТЬ графа. Любой узел внутри области, упавший с подходящей ошибкой,
уводит исполнение в свой ``except``::

    {
      "id": "safe",
      "type": "try",
      "body": "fetch",
      "except": [
        {"error_equals": ["TimeoutError"], "next": "on_timeout", "result_var": "error"},
        {"error_equals": ["*"], "next": "on_any"}
      ],
      "next": "after"
    }

Область — узлы, достижимые из ``body`` по рёбрам управления, но НЕ достижимые
из ``next`` (точки выхода). Тот же принцип, что у веток ``parallel``: состав
выводится из графа, а не перечисляется руками, поэтому область не может
разойтись со структурой.

Вложенные ``try`` работают сами собой: внутренний — обычный узел внутри
области внешнего, и ошибка всплывает к ближайшему подходящему обработчику.
"""
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
    """Одна ветка ``except``: какие ошибки ловим, куда прыгаем и куда положить
    объект ошибки."""

    error_equals: list[str]
    next: str
    result_var: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "ExceptHandler":
        if "next" not in data:
            raise PipelineDefinitionError("except: поле 'next' обязательно")
        return cls(
            error_equals=list(data.get("error_equals", ["*"])),
            next=data["next"],
            result_var=data.get("result_var"),
        )

    def matches(self, exc: BaseException) -> bool:
        return matches_error(self.error_equals, exc)

    def error_payload(self, exc: BaseException, node_id: str) -> dict:
        """Объект ошибки, который кладётся во фрейм под именем ``result_var``."""
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
            raise PipelineDefinitionError(f"Node '{data.get('id')}': поле 'body' обязательно")
        return cls(
            body=body,
            handlers=[ExceptHandler.from_dict(h) for h in data.get("except", [])],
            next=data.get("next"),
            **Node._common(data),
        )

    # ------------------------------------------------------------- область

    def scope(self, pipeline: "Pipeline") -> frozenset[str]:
        """Узлы тела блока. Считается один раз: граф после разбора неизменен."""
        if self._scope is None:
            after = pipeline.reachable([self.next]) if self.next else frozenset()
            self._scope = pipeline.reachable([self.body], stop_at=after) - {self.id}
        return self._scope

    def order_targets(self) -> list[str]:
        targets = [self.body, *(h.next for h in self.handlers)]
        if self.next:
            targets.append(self.next)
        return [t for t in targets if t]

    # ---------------------------------------------------------- валидация

    def validate(self, pipeline: "Pipeline") -> list[str]:
        errors = self._validate_common(pipeline)
        if not pipeline.has_node(self.body):
            errors.append(f"{self.id}: body '{self.body}' не найден в графе")
        if not self.handlers:
            errors.append(f"{self.id}: нужен хотя бы один обработчик в 'except'")
        for handler in self.handlers:
            if not pipeline.has_node(handler.next):
                errors.append(f"{self.id}: except.next '{handler.next}' не найден в графе")
            elif pipeline.has_node(self.body) and handler.next in self.scope(pipeline):
                # обработчик внутри собственного тела: его же ошибки снова
                # приведут сюда — молчаливая петля
                errors.append(
                    f"{self.id}: обработчик '{handler.next}' находится внутри тела блока"
                )
        if self.next and not pipeline.has_node(self.next):
            errors.append(f"{self.id}: next '{self.next}' не найден в графе")
        return errors

    # --------------------------------------------------------- исполнение

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
        except Exception as exc:  # noqa: BLE001 - это и есть граница блока
            handler = next((h for h in self.handlers if h.matches(exc)), None)
            if handler is None:
                raise  # не наш тип ошибки — пусть ловит объемлющий try
            # переменные, записанные телом до падения, остаются видны
            ctx = frame.ctx
            session.emit_node_event(
                "try_caught",
                self,
                {"error": str(exc), "type": error_name(exc), "next": handler.next},
            )
            if handler.result_var:
                ctx = ctx.with_var(handler.result_var, handler.error_payload(exc, self.id))
            return session.pipeline.get_node(handler.next), ctx

        # тело отработало без ошибок
        if node is not None:
            return node, ctx  # управление само ушло за пределы блока — не мешаем
        session.emit_node_event("try_completed", self, {"body": self.body})
        return self._goto(session, self.next), ctx
