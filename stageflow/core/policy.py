"""What a pipeline is allowed to be made of.

The registry answers what the *process* can execute. That is not the same
question as what a given pipeline may use: a platform running pipelines on
behalf of several tenants hands each of them a subset — these stages, these
node types — and the subset is the boundary the tenant cannot argue with,
because the tenant only writes the graph.

Until now that subset could only be expressed by running a separate process
per set of stages, since the registry is a module-level global. A `Policy`
makes it a value: one process, one registry, a different allowance per
session.

Nothing here is read from the pipeline JSON, and nothing in the JSON can
widen it. A policy is given to `Session` by the host, next to the debugger.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from ..exceptions import PolicyViolationError


def _freeze(names: Iterable[str] | None) -> frozenset[str] | None:
    return None if names is None else frozenset(names)


@dataclass(frozen=True)
class Policy:
    """The allowance of one session.

    `None` and the empty set mean opposite things, and the difference is the
    whole point: `None` is "no opinion, whatever the process has registered",
    `frozenset()` is "nothing at all". A policy that forgot to list its stages
    must not accidentally grant every stage the process happens to import.
    """

    stages: frozenset[str] | None = None
    node_types: frozenset[str] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "stages", _freeze(self.stages))
        object.__setattr__(self, "node_types", _freeze(self.node_types))

    @property
    def unrestricted(self) -> bool:
        return self.stages is None and self.node_types is None

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
        """
        errors: list[str] = []
        for node in pipeline.nodes:
            if not self.allows_node_type(node.type):
                errors.append(
                    f"{node.id}: node type '{node.type}' is not allowed by the policy"
                )
            name = getattr(node, "stage", None)
            if name is not None and not self.allows_stage(name):
                errors.append(
                    f"{node.id}: stage '{name}' is not allowed by the policy"
                )
        return errors


#: Everything the process has registered — the default, and what a host that
#: has no tenants to separate should keep using.
OPEN = Policy()
