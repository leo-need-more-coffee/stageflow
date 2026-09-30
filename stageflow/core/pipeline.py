from __future__ import annotations

from jsonschema import ValidationError as _SchemaError
from jsonschema import validate as _json_validate

from stageflow.docs.schema import load_pipeline_schema

from ..exceptions import (
    PipelineDefinitionError,
    PipelineValidationError,
    TypeDeclarationError,
)
from ..i18n import _
from .nodes import EntryNode, Node
from .policy import Policy
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

    @classmethod
    def from_dict(cls, data: dict) -> "Pipeline":
        schema = load_pipeline_schema()
        cls._check_schema(data, schema)

        subpipelines = data.get("subpipelines", {})
        for sub_id, sub_data in subpipelines.items():
            cls._check_schema(sub_data, schema, sub_id)

        try:
            typesystem = TypeSystem.from_dict(data.get("types"), data.get("variables"))
        except TypeDeclarationError as exc:
            raise PipelineDefinitionError(
                _("Invalid type declarations: {reason}", reason=exc)
            ) from exc

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
    def _check_schema(data: dict, schema: dict, sub_id: str | None = None) -> None:
        try:
            _json_validate(instance=data, schema=schema)
        except _SchemaError as exc:
            # a whole sentence per case rather than a noun filled into one —
            # the reason is in `registry.py`
            if sub_id is None:
                message = _("Pipeline schema validation failed: {reason}",
                            reason=exc.message)
            else:
                message = _("Subpipeline '{name}' schema validation failed: {reason}",
                            name=sub_id, reason=exc.message)
            raise PipelineDefinitionError(message) from exc

    def has_node(self, node_id: str) -> bool:
        return node_id in self._nodes_map

    def get_node(self, node_id: str) -> Node:
        try:
            return self._nodes_map[node_id]
        except KeyError:
            raise PipelineDefinitionError(
                _("Node with id '{node}' not found in pipeline", node=node_id)
            ) from None

    def get_entry_node(self) -> Node:
        return self.get_node(self.entry)

    def reachable(
        self, starts: list[str | None], stop_at: frozenset[str] = frozenset()
    ) -> frozenset[str]:
        seen: set[str] = set()
        queue = [node_id for node_id in starts if node_id]
        while queue:
            node_id = queue.pop()
            if node_id in seen or node_id in stop_at or not self.has_node(node_id):
                continue
            seen.add(node_id)
            queue.extend(self.get_node(node_id).order_targets())
        return frozenset(seen)

    def entry_nodes(self) -> list[EntryNode]:
        return [node for node in self.nodes if isinstance(node, EntryNode)]

    def _entry_node_errors(self) -> list[str]:
        entries = self.entry_nodes()
        if not entries:
            return []

        errors: list[str] = []
        if len(entries) > 1:
            ids = ", ".join(sorted(node.id for node in entries))
            errors.append(
                _("more than one entry node: {nodes} — there must be exactly one", nodes=ids)
            )
        for node in entries:
            if self.entry and self.entry != node.id:
                errors.append(
                    _("{node}: this entry node is not the pipeline entry point "
                      "(entry = '{entry}')", node=node.id, entry=self.entry)
                )
            for other in self.nodes:
                if other.id != node.id and node.id in other.order_targets():
                    errors.append(
                        _("{node}: jumping into the entry node '{entry}' is not allowed",
                          node=other.id, entry=node.id)
                    )
        return errors

    def collect_errors(self, policy: "Policy | None" = None) -> list[str]:
        errors: list[str] = []
        if policy is not None:
            errors.extend(policy.errors_for(self))
        entry_nodes = self.entry_nodes()
        if not self.entry:
            if not entry_nodes:
                errors.append(_("The pipeline has no entry"))
        elif self.entry not in self._nodes_map:
            errors.append(
                _("Entry node '{entry}' not found in the graph", entry=self.entry)
            )

        errors.extend(self.typesystem.collect_errors())
        errors.extend(self._entry_node_errors())

        seen: set[str] = set()
        for node in self.nodes:
            if node.id in seen:
                errors.append(_("Duplicate node id: '{node}'", node=node.id))
            seen.add(node.id)
            errors.extend(node.validate(self))
        errors.extend(self._cycle_errors())
        return errors

    def _cycle_errors(self) -> list[str]:
        """A road that comes back to where it has been.

        The language has a loop of its own — the `map` node — and it is
        bounded by a list. A cycle in the order edges is not: it spins for as
        long as the process lives, at tens of thousands of nodes a second,
        and it is almost always a `next` pointed at the wrong id. Saying so
        at validation costs one walk of the graph and saves finding out by
        watching a server fall over.
        """
        colour: dict[str, int] = {}  # 0 — on the current path, 1 — done with
        found: list[str] = []

        def walk(node_id: str, path: list[str]) -> None:
            state = colour.get(node_id)
            if state == 1:
                return
            if state == 0:
                cycle = path[path.index(node_id):] + [node_id]
                found.append(" -> ".join(cycle))
                return
            colour[node_id] = 0
            path.append(node_id)
            for target in self.get_node(node_id).order_targets():
                if self.has_node(target):
                    walk(target, path)
            path.pop()
            colour[node_id] = 1

        for node in self.nodes:
            if node.id not in colour:
                walk(node.id, [])
        return [_("cycle in the graph: {cycle}", cycle=cycle)
                for cycle in dict.fromkeys(found)]

    def validate(self, policy: "Policy | None" = None) -> None:
        """Everything wrong with this pipeline, as one error.

        A policy is checked here rather than at run time so that a tenant
        saving a graph is told what to change before anything executes; the
        session checks again while running, for a pipeline built by hand.
        """
        errors = self.collect_errors(policy)
        if errors:
            raise PipelineValidationError(errors)


def _sole_entry_node(nodes: list[Node]) -> str | None:
    found = [node.id for node in nodes if isinstance(node, EntryNode)]
    return found[0] if len(found) == 1 else None
