from __future__ import annotations

import copy
from dataclasses import dataclass
from functools import cache
from typing import Any, Iterator

from jsonschema import Draft202012Validator as _VALIDATOR

try:  # a Rust validator for the fast "is it valid" check; optional
    import jsonschema_rs as _rs
except ImportError:  # pragma: no cover - depends on the environment
    _rs = None

from stageflow.docs.schema import load_pipeline_schema

from ..exceptions import (
    PipelineDefinitionError,
    PipelineValidationError,
    TypeDeclarationError,
)
from ..i18n import _
from .migrations import migrate
from .nodes import EntryNode, Node
from .policy import Policy
from .typesys import TypeSystem


#: Maximum number of schema errors listed in one message.
_MAX_SCHEMA_ERRORS = 10


@dataclass(frozen=True)
class _SchemaError:
    """A schema error with its path made absolute (see `_schema_errors`)."""

    absolute_path: list
    message: str
    cause: Exception


@cache
def _validators() -> tuple[Any, dict[str, Any]]:
    """Validators for the pipeline schema, split for speed.

    The schema selects a node's rules with nine `if type == ... then ...`
    blocks, which a generic validator evaluates for every node. Instead, a
    shell schema without those blocks checks the document, and each node is
    checked against the schema for its own `type`. The errors are the same.
    """
    schema = load_pipeline_schema()
    shell = copy.deepcopy(schema)
    node = shell["$defs"]["node"]
    per_type: dict[str, Any] = {}
    for branch in node.pop("allOf"):
        type_ = branch["if"]["properties"]["type"]["const"]
        per_type[type_] = _VALIDATOR({"$ref": branch["then"]["$ref"],
                                      "$defs": schema["$defs"]})
    return _VALIDATOR(shell), per_type


@cache
def _fast_validator() -> Any:
    """The whole schema compiled by jsonschema-rs, or None without it."""
    if _rs is None:
        return None
    return _rs.Draft202012Validator(load_pipeline_schema(), validate_formats=False)


def _node_lists(data: Any, prefix: list) -> Iterator[tuple[list, list]]:
    """Every `nodes` array in a document, subpipelines included, with its path."""
    if not isinstance(data, dict):
        return
    if isinstance(data.get("nodes"), list):
        yield prefix + ["nodes"], data["nodes"]
    subpipelines = data.get("subpipelines")
    if isinstance(subpipelines, dict):
        for name, sub in subpipelines.items():
            yield from _node_lists(sub, prefix + ["subpipelines", name])


def _schema_errors(data: Any) -> list[_SchemaError]:
    """All schema errors of a document, with absolute paths.

    Valid documents (the common case) are confirmed by jsonschema-rs, which
    is much faster. Errors are always collected by the Python validator, so
    messages and paths do not depend on which library is installed.
    """
    fast = _fast_validator()
    if fast is not None and fast.is_valid(data):
        return []
    shell, per_type = _validators()
    found = [_SchemaError(list(e.absolute_path), e.message, e) for e in shell.iter_errors(data)]
    for path, nodes in _node_lists(data, []):
        for index, node in enumerate(nodes):
            type_ = node.get("type") if isinstance(node, dict) else None
            if not isinstance(type_, str):
                continue  # the shell schema reports a missing or invalid type
            validator = per_type.get(type_)
            if validator is None:
                continue
            for e in validator.iter_errors(node):
                found.append(_SchemaError(path + [index] + list(e.absolute_path),
                                          e.message, e))
    return sorted(found, key=lambda e: [(0, p, "") if isinstance(p, int) else (1, 0, str(p))
                                        for p in e.absolute_path])


def _located(data: dict, exc) -> str:
    """Format a schema error with its location, using the node id instead of
    `nodes[i]` where possible."""
    path = list(exc.absolute_path)
    where = ""
    if len(path) >= 2 and path[0] == "nodes" and isinstance(path[1], int):
        node = (data.get("nodes") or [])[path[1]] if path[1] < len(data.get("nodes") or []) else {}
        named = node.get("id") if isinstance(node, dict) else None
        rest = "".join(f"[{p}]" if isinstance(p, int) else f".{p}" for p in path[2:])
        where = f"{named or f'nodes[{path[1]}]'}{rest}"
    elif path:
        where = "".join(f"[{p}]" if isinstance(p, int) else (f".{p}" if i else str(p))
                        for i, p in enumerate(path))
    return f"{where}: {exc.message}" if where else exc.message


class Pipeline:
    """A parsed pipeline graph. Build it with `Pipeline.from_dict(data)`;
    `validate(policy)` reports everything wrong with it at once.
    """

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
        """Parse a pipeline document. An older format version is upgraded
        first (see `core/migrations.py`); `raw_json` holds the upgraded one."""
        if isinstance(data, dict):
            data = migrate(data)
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
        """Check the pipeline against the JSON Schema.

        Reports all errors at once (up to `_MAX_SCHEMA_ERRORS`), each with its
        location, e.g. `classify.except[0]: 'next' is a required property`.
        """
        found = _schema_errors(data)
        if not found:
            return
        reason = "; ".join(_located(data, exc) for exc in found[:_MAX_SCHEMA_ERRORS])
        if len(found) > _MAX_SCHEMA_ERRORS:
            reason += _("; and {more} more", more=len(found) - _MAX_SCHEMA_ERRORS)
        # Separate messages per case rather than one template with a noun
        # inserted, so that translations stay grammatical (see registry.py).
        if sub_id is None:
            message = _("Pipeline schema validation failed: {reason}", reason=reason)
        else:
            message = _("Subpipeline '{name}' schema validation failed: {reason}",
                        name=sub_id, reason=reason)
        raise PipelineDefinitionError(message) from found[0].cause

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
        # Cycles are refused unless the policy allows them.
        if policy is None or not policy.allow_cycles:
            errors.extend(self._cycle_errors())
        return errors

    def _cycle_errors(self) -> list[str]:
        """Report cycles in the order edges.

        Loops over data use `map`, which is bounded by its list. A cycle is
        unbounded and is usually a `next` pointing at the wrong node, so it
        is an error unless the host allows it with `Policy(allow_cycles=True)`.
        """
        # Iterative DFS: a recursive one hits the recursion limit on graphs
        # of about a thousand nodes.
        on_path: dict[str, int] = {}  # node -> its index in `path`
        done: set[str] = set()
        found: list[str] = []

        for node in self.nodes:
            if node.id in done:
                continue
            path: list[str] = [node.id]
            on_path[node.id] = 0
            stack = [iter(self.get_node(node.id).order_targets())]
            while stack:
                target = next(stack[-1], None)
                if target is None:
                    finished = path.pop()
                    del on_path[finished]
                    done.add(finished)
                    stack.pop()
                    continue
                if not self.has_node(target) or target in done:
                    continue
                if target in on_path:
                    found.append(" -> ".join(path[on_path[target]:] + [target]))
                    continue
                on_path[target] = len(path)
                path.append(target)
                stack.append(iter(self.get_node(target).order_targets()))
        return [_("cycle in the graph: {cycle}", cycle=cycle)
                for cycle in dict.fromkeys(found)]

    def validate(self, policy: "Policy | None" = None) -> None:
        """Raise PipelineValidationError listing everything wrong.

        With a policy, also reports what the policy forbids, so a graph can be
        checked before it is saved or run.
        """
        errors = self.collect_errors(policy)
        if errors:
            raise PipelineValidationError(errors)


def _sole_entry_node(nodes: list[Node]) -> str | None:
    found = [node.id for node in nodes if isinstance(node, EntryNode)]
    return found[0] if len(found) == 1 else None
