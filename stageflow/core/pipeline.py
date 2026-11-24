from __future__ import annotations

import copy
from dataclasses import dataclass
from functools import cache
from typing import Any, Iterator

from jsonschema import Draft202012Validator as _VALIDATOR

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


#: How many shape problems are worth listing before the list stops helping.
_MAX_SCHEMA_ERRORS = 10


@dataclass(frozen=True)
class _SchemaError:
    """A schema error with its path made absolute (see `_schema_errors`)."""

    absolute_path: list
    message: str
    cause: Exception


@cache
def _validators() -> tuple[Any, dict[str, Any]]:
    """The pipeline schema split for speed: a shell, and one schema per node type.

    The schema picks a node's rules with nine `if type == ... then ...` blocks,
    and a generic validator evaluates all nine for every node. Here the shell
    checks everything except those blocks, and each node is then checked
    against the one schema its `type` names. Same errors, same paths, several
    times faster on a large graph.
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
    shell, per_type = _validators()
    found = [_SchemaError(list(e.absolute_path), e.message, e) for e in shell.iter_errors(data)]
    for path, nodes in _node_lists(data, []):
        for index, node in enumerate(nodes):
            type_ = node.get("type") if isinstance(node, dict) else None
            if not isinstance(type_, str):
                continue  # the shell already says what is wrong with it
            validator = per_type.get(type_)
            if validator is None:
                continue
            for e in validator.iter_errors(node):
                found.append(_SchemaError(path + [index] + list(e.absolute_path),
                                          e.message, e))
    return sorted(found, key=lambda e: [(0, p, "") if isinstance(p, int) else (1, 0, str(p))
                                        for p in e.absolute_path])


def _located(data: dict, exc) -> str:
    """One schema complaint with the place it belongs to in front of it.

    The place is the node's `id` when the path runs through `nodes`, because an
    index is something the author has to count and an id is something they
    wrote. Anything else keeps the dotted path it came with.
    """
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
        """Everything wrong with the SHAPE of a pipeline, at once and located.

        Two things the first-error-only version could not do, and both of them
        are what the author of a graph actually needs.

        It says WHERE. `'next' is a required property` is a true sentence about
        a graph with forty nodes and a useless one; `nodes[4].except[0]: 'next'
        is a required property` names the thing to go and fix. The path is the
        node's id where there is one, because that is what the author typed and
        what the editor shows — an index means counting.

        And it says everything, the way the cross-graph checks already do (see
        `collect_errors`). A shape is usually wrong in the same way in several
        places, and fixing them one round trip at a time is the difference
        between a correction and a conversation.
        """
        found = _schema_errors(data)
        if not found:
            return
        reason = "; ".join(_located(data, exc) for exc in found[:_MAX_SCHEMA_ERRORS])
        if len(found) > _MAX_SCHEMA_ERRORS:
            reason += _("; and {more} more", more=len(found) - _MAX_SCHEMA_ERRORS)
        # a whole sentence per case rather than a noun filled into one —
        # the reason is in `registry.py`
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
        # a road that comes back is refused unless the caller's policy says it
        # has something to stop it with (see Policy.allow_cycles)
        if policy is None or not policy.allow_cycles:
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

        A host with something to stop one with — a budget, a timeout a layer
        above — says so with `Policy(allow_cycles=True)`, and then this is not
        called at all. See `collect_errors`.
        """
        # iterative rather than recursive: a long straight graph is a deep
        # walk, and recursion stops at about a thousand nodes
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
