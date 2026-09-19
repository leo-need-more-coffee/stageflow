"""Базовый узел графа и реестр типов узлов.

Никаких isinstance-цепочек: тип узла разрешается ровно один раз, при разборе
JSON, через реестр типов; дальше работают методы самого узла — ``execute()``
и ``validate()``. Добавить новый тип = новый класс с декоратором
``@register_node``; session.py и pipeline.py не трогаются.

Контекст ходит явным параметром: ``execute(session, ctx) -> (next_node, ctx)``.
Общего мутабельного ``session.context`` нет специально — иначе конкурентные
ветки ``parallel`` затирали бы друг другу фрейм.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from ...exceptions import PipelineDefinitionError
from ..context import Context
from ..registry import Registry
from .recovery import Retrier

if TYPE_CHECKING:  # pragma: no cover
    from ..pipeline import Pipeline
    from ..session import Session

_node_types: Registry[type["Node"]] = Registry("node type")


def register_node(type_str: str):
    """Декоратор регистрации типа узла под значением поля ``type`` в JSON."""

    def decorator(cls: type["Node"]) -> type["Node"]:
        cls.type = type_str
        _node_types.add(type_str, cls)
        return cls

    return decorator


def get_node_types() -> dict[str, type["Node"]]:
    """Снимок реестра типов узлов."""
    return _node_types.as_dict()


class Node:
    """База для всех узлов. Поля, общие для любого типа, живут здесь."""

    type: str = "node"

    def __init__(
        self,
        id: str,
        metadata: dict | None = None,
        retry: list[Retrier] | None = None,
        expose: dict[str, str] | None = None,
    ):
        self.id = id
        self.metadata = metadata or {}
        self.retry = retry or []
        self.expose = expose or {}

    # ---------------------------------------------------------- разбор

    @staticmethod
    def from_dict(data: dict) -> "Node":
        node_type = data.get("type")
        if node_type not in _node_types:
            raise PipelineDefinitionError(f"Unknown node type: {node_type!r}")
        return _node_types.get(node_type)._parse(data)

    @classmethod
    def _parse(cls, data: dict) -> "Node":  # pragma: no cover - абстрактный
        raise NotImplementedError

    @staticmethod
    def _common(data: dict) -> dict:
        """Поля, общие для всех типов узлов, — kwargs для ``Node.__init__``."""
        return dict(
            id=data["id"],
            metadata=data.get("metadata", {}),
            retry=[Retrier.from_dict(r) for r in data.get("retry", [])],
            expose=data.get("expose", {}),
        )

    # ------------------------------------------------------- исполнение

    async def execute(
        self, session: "Session", ctx: Context
    ) -> tuple["Node | None", Context]:  # pragma: no cover - абстрактный
        raise NotImplementedError

    def order_targets(self) -> list[str]:
        """Куда узел может передать управление. Переопределяется теми, у кого
        переходов больше одного; на этом держится обход графа (области try,
        достижимость)."""
        return [self.next] if getattr(self, "next", None) else []

    def validate(self, pipeline: "Pipeline") -> list[str]:
        return self._validate_common(pipeline)

    def _validate_common(self, pipeline: "Pipeline") -> list[str]:
        errors: list[str] = []
        for src, dst in self.expose.items():
            malformed = False
            for path, side in ((src, "источник"), (dst, "назначение")):
                if not isinstance(path, str) or not path.isidentifier():
                    errors.append(
                        f"{self.id}: expose {side} '{path}' должен быть именем переменной"
                    )
                    malformed = True
            if malformed:
                continue
            # expose — чистая копия, поэтому объявленные типы источника и
            # назначения обязаны совпадать
            ts = pipeline.typesystem
            src_type = ts.declared(src)
            dst_type = ts.declared(dst)
            if src_type and dst_type and not ts.expose_compatible(src_type, dst_type):
                errors.append(
                    f"{self.id}: expose {src} -> {dst}: несовместимые типы "
                    f"'{src_type}' и '{dst_type}'"
                )
        return errors

    def apply_expose(self, session: "Session", ctx: Context) -> Context:
        """Чистое переименование/копирование во фрейме, без стадии.
        По умолчанию копия: старое имя остаётся (убирается через ``consume``)."""
        types = session.pipeline.typesystem
        for src, dst in self.expose.items():
            value = ctx.get_var(src)
            types.check_write(dst, value, self.id)
            ctx = ctx.with_var(dst, value)
        return ctx

    def _goto(self, session: "Session", node_id: str | None) -> "Node | None":
        return session.pipeline.get_node(node_id) if node_id else None

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.id}>"
