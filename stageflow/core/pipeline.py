"""Граф пайплайна: разбор JSON и статическая валидация.

Валидация двухслойная: JSON Schema (форма, ``docs/schemas/pipeline.json``) +
перекрёстные проверки графа. Вторая — чистая полиморфная раздача: каждый узел
проверяет себя сам (см. MEMORY_MODEL.md §7), здесь остаются только
общеграфовые проверки.
"""
from __future__ import annotations

from jsonschema import ValidationError as _SchemaError
from jsonschema import validate as _json_validate

from stageflow.docs.schema import load_pipeline_schema

from ..exceptions import (
    PipelineDefinitionError,
    PipelineValidationError,
    TypeDeclarationError,
)
from .nodes import EntryNode, Node
from .typesys import TypeSystem


class Pipeline:
    def __init__(
        self,
        entry: str,
        nodes: list[Node],
        metadata: dict | None = None,
        raw_json: dict | None = None,
        subpipelines: dict | None = None,
        typesystem: TypeSystem | None = None,
    ):
        self.entry = entry
        self.nodes = nodes
        self._nodes_map = {node.id: node for node in nodes}
        self.metadata = metadata or {}
        self.raw_json = raw_json or {}
        self.subpipelines = subpipelines or {}
        self.typesystem = typesystem or TypeSystem.empty()

    # ------------------------------------------------------------ разбор

    @classmethod
    def from_dict(cls, data: dict) -> "Pipeline":
        schema = load_pipeline_schema()
        cls._check_schema(data, schema, "Pipeline")

        subpipelines = data.get("subpipelines", {})
        for sub_id, sub_data in subpipelines.items():
            cls._check_schema(sub_data, schema, f"Subpipeline '{sub_id}'")

        try:
            typesystem = TypeSystem.from_dict(data.get("types"), data.get("variables"))
        except TypeDeclarationError as exc:
            raise PipelineDefinitionError(f"Объявления типов некорректны: {exc}") from exc

        nodes = [Node.from_dict(node) for node in data.get("nodes", [])]
        return cls(
            entry=data.get("entry") or _sole_entry_node(nodes),
            nodes=nodes,
            metadata=data.get("metadata", {}),
            raw_json=data,
            subpipelines=subpipelines,
            typesystem=typesystem,
        )

    @staticmethod
    def _check_schema(data: dict, schema: dict, what: str) -> None:
        try:
            _json_validate(instance=data, schema=schema)
        except _SchemaError as exc:
            raise PipelineDefinitionError(
                f"{what} schema validation failed: {exc.message}"
            ) from exc

    # ------------------------------------------------------------ доступ

    def has_node(self, node_id: str) -> bool:
        return node_id in self._nodes_map

    def get_node(self, node_id: str) -> Node:
        try:
            return self._nodes_map[node_id]
        except KeyError:
            raise PipelineDefinitionError(
                f"Node with id '{node_id}' not found in pipeline"
            ) from None

    def get_entry_node(self) -> Node:
        return self.get_node(self.entry)

    def reachable(
        self, starts: list[str | None], stop_at: frozenset[str] = frozenset()
    ) -> frozenset[str]:
        """Узлы, достижимые по рёбрам управления от ``starts``, не заходя в
        ``stop_at``. На этом строятся области: тело ``try`` — то, что
        достижимо из body, но не из точки выхода."""
        seen: set[str] = set()
        queue = [node_id for node_id in starts if node_id]
        while queue:
            node_id = queue.pop()
            if node_id in seen or node_id in stop_at or not self.has_node(node_id):
                continue
            seen.add(node_id)
            queue.extend(self.get_node(node_id).order_targets())
        return frozenset(seen)

    # -------------------------------------------------------- валидация

    def entry_nodes(self) -> list[EntryNode]:
        return [node for node in self.nodes if isinstance(node, EntryNode)]

    def _entry_node_errors(self) -> list[str]:
        """Узел ``entry`` — это НАЧАЛО графа, а не обычный узел.

        Отсюда три ограничения, без которых «точка входа» перестаёт что-либо
        значить: он один, он и есть точка входа пайплайна, и вернуться в него
        нельзя (посев с семантикой «по умолчанию» на втором проходе просто
        ничего не сделает, зато в графе нарисуется ложный старт).
        """
        entries = self.entry_nodes()
        if not entries:
            return []

        errors: list[str] = []
        if len(entries) > 1:
            ids = ", ".join(sorted(node.id for node in entries))
            errors.append(f"узлов entry больше одного: {ids} — точка входа должна быть одна")
        for node in entries:
            if self.entry and self.entry != node.id:
                errors.append(
                    f"{node.id}: узел entry не является точкой входа пайплайна "
                    f"(entry = '{self.entry}')"
                )
            for other in self.nodes:
                if other.id != node.id and node.id in other.order_targets():
                    errors.append(f"{other.id}: переход в точку входа '{node.id}' недопустим")
        return errors

    def collect_errors(self) -> list[str]:
        errors: list[str] = []
        entry_nodes = self.entry_nodes()
        if not self.entry:
            # узлы entry есть, но точка входа не вывелась — значит их больше
            # одного; про это скажет _entry_node_errors, и повторять здесь
            # общее «не задан entry» только сбивало бы с толку
            if not entry_nodes:
                errors.append("В пайплайне не задан entry")
        elif self.entry not in self._nodes_map:
            errors.append(f"Entry node '{self.entry}' не найден в графе")

        errors.extend(self.typesystem.collect_errors())
        errors.extend(self._entry_node_errors())

        seen: set[str] = set()
        for node in self.nodes:
            if node.id in seen:
                errors.append(f"Дублирующийся id узла: '{node.id}'")
            seen.add(node.id)
            errors.extend(node.validate(self))
        return errors

    def validate(self) -> None:
        """Кидает :class:`PipelineValidationError` со всеми нарушениями разом —
        а не по одному, чтобы автор пайплайна чинил их одним заходом."""
        errors = self.collect_errors()
        if errors:
            raise PipelineValidationError(errors)


def _sole_entry_node(nodes: list[Node]) -> str | None:
    """Точка входа, выведенная из графа: если есть ровно один узел ``entry``,
    поле ``entry`` пайплайна необязательно — начало видно по самому графу.
    Нескольких таких узлов не бывает (``_entry_node_errors``), поэтому
    неоднозначность разрешать не нужно: вывод молчит, ошибку даёт валидация."""
    found = [node.id for node in nodes if isinstance(node, EntryNode)]
    return found[0] if len(found) == 1 else None
