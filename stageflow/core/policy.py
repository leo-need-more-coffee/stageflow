from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from ..exceptions import PolicyViolationError
from .budget import UNLIMITED, Limits


def _freeze(names: Iterable[str] | None) -> frozenset[str] | None:
    return None if names is None else frozenset(names)


@dataclass(frozen=True)
class Policy:
    """The allowance of one session: which blocks, and how much of anything.

    The registry answers what the *process* can execute, which is not the
    question a platform running pipelines for several tenants has. Each of
    them gets a subset — these stages, these node types — and the subset is
    the boundary the author of a graph cannot argue with, because they only
    write the graph. Until this existed the subset could only be expressed by
    running a separate process per set of stages, the registry being a
    module-level global.

    Given to `Session` by the host, next to the debugger. Nothing is read
    from the pipeline JSON and nothing in it can widen a policy.

    `None` and the empty set mean opposite things, and the difference is the
    whole point: `None` is "no opinion, whatever the process has registered",
    `frozenset()` is "nothing at all". A policy that forgot to list its stages
    must not accidentally grant every stage the process happens to import.
    """

    stages: frozenset[str] | None = None
    node_types: frozenset[str] | None = None
    #: how much a session under this policy may spend. A plan is one object:
    #: which blocks, and how many of anything
    limits: Limits = field(default=UNLIMITED)

    def __post_init__(self) -> None:
        object.__setattr__(self, "stages", _freeze(self.stages))
        object.__setattr__(self, "node_types", _freeze(self.node_types))

    @property
    def unrestricted(self) -> bool:
        return (self.stages is None and self.node_types is None
                and self.limits.unlimited)

    def allows_stage(self, name: str) -> bool:
        return self.stages is None or name in self.stages

    def allows_node_type(self, type_: str) -> bool:
        return self.node_types is None or type_ in self.node_types

    def check_stage(self, name: str, where: str) -> None:
        if not self.allows_stage(name):
            raise PolicyViolationError(
                f"{where}: stage '{name}' is not allowed by the policy"
            )

    def check_node_type(self, type_: str, where: str) -> None:
        if not self.allows_node_type(type_):
            raise PolicyViolationError(
                f"{where}: node type '{type_}' is not allowed by the policy"
            )

    def errors_for(self, pipeline) -> list[str]:
        """Everything this policy refuses about a pipeline, at once.

        Collected rather than raised one at a time: a tenant saving a graph
        wants the list of what to change, not the first thing that offended.

        Subpipelines are walked too, and they have to be. A child graph
        becomes a `Pipeline` only when it runs, so without this the answer to
        "may I save this" would be yes and the answer to "may I run it" no —
        the same refusal, hours apart, at the worst moment.
        """
        if self.stages is None and self.node_types is None:
            return []  # nothing is restricted about composition
        errors = [
            message
            for node in pipeline.nodes
            for message in self._node_errors(node.type, node.id, getattr(node, "stage", None))
        ]
        for sub_id, graph in (getattr(pipeline, "subpipelines", None) or {}).items():
            errors.extend(self._graph_errors(graph, f"[{sub_id}] "))
        return errors

    def _graph_errors(self, graph: dict, where: str) -> list[str]:
        """The same check over a graph that is still raw JSON."""
        errors: list[str] = []
        for node in graph.get("nodes") or []:
            errors.extend(self._node_errors(
                node.get("type"), node.get("id", "?"), node.get("stage"), where
            ))
        for sub_id, nested in (graph.get("subpipelines") or {}).items():
            errors.extend(self._graph_errors(nested, f"{where}[{sub_id}] "))
        return errors

    def _node_errors(
        self, type_: str | None, node_id: str, stage: str | None, where: str = ""
    ) -> list[str]:
        errors: list[str] = []
        if type_ is not None and not self.allows_node_type(type_):
            errors.append(
                f"{where}{node_id}: node type '{type_}' is not allowed by the policy"
            )
        if stage is not None and not self.allows_stage(stage):
            errors.append(
                f"{where}{node_id}: stage '{stage}' is not allowed by the policy"
            )
        return errors


#: Everything the process has registered — the default, and what a host that
#: has no tenants to separate should keep using.
OPEN = Policy()
