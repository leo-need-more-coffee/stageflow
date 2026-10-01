from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from ..exceptions import PolicyViolationError
from ..i18n import _
from .budget import Limits


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
    #: Whether a road may come back to where it has been.
    #:
    #: Off, and the only field here whose default is a restriction rather than
    #: an absence of one. The reason is that a cycle in the order edges is not
    #: a narrower version of something — it is a shape the language does not
    #: otherwise have, and one nothing in the graph bounds. `map` loops over a
    #: list and stops when the list does; a cycle stops when something outside
    #: the graph stops it, and it is far more often a `next` pointed at the
    #: wrong id than a loop somebody meant.
    #:
    #: Turning it on is a host saying "I have something that will stop this".
    #: Usually that is `limits=Limits(counters={"steps": …})` — a run that
    #: cannot be ended by its own graph is ended by its budget. It can also be
    #: a timeout a layer above, which is why this does not refuse to be set
    #: without a ceiling: a platform that already bounds its work should not
    #: have to argue with a framework about it.
    allow_cycles: bool = False
    #: how much a session under this policy may spend. A plan is one object:
    #: which blocks, and how many of anything. Built fresh rather than shared
    #: with every other default policy — one `Limits` handed round and then
    #: written into would change the allowance of everything at once
    limits: Limits = field(default_factory=Limits)

    def __post_init__(self) -> None:
        object.__setattr__(self, "stages", _freeze(self.stages))
        object.__setattr__(self, "node_types", _freeze(self.node_types))

    @property
    def unrestricted(self) -> bool:
        """Nothing is narrowed. `allow_cycles` is not part of this: it widens
        what may be written rather than narrowing it, and a policy that permits
        a loop is not thereby an absence of a policy."""
        return (self.stages is None and self.node_types is None
                and self.limits.unlimited)

    def allows_stage(self, name: str) -> bool:
        return self.stages is None or name in self.stages

    def allows_node_type(self, type_: str) -> bool:
        return self.node_types is None or type_ in self.node_types

    def check_stage(self, name: str, where: str) -> None:
        if not self.allows_stage(name):
            raise PolicyViolationError(
                _("{where}: stage '{name}' is not allowed by the policy",
                  where=where, name=name)
            )

    def check_node_type(self, type_: str, where: str) -> None:
        if not self.allows_node_type(type_):
            raise PolicyViolationError(
                _("{where}: node type '{type}' is not allowed by the policy",
                  where=where, type=type_)
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
        errors = self._limit_errors(pipeline)
        if self.stages is None and self.node_types is None:
            return errors  # nothing is restricted about composition
        errors += [
            message
            for node in pipeline.nodes
            for message in self._node_errors(node.type, node.id, getattr(node, "stage", None))
        ]
        for sub_id, graph in (getattr(pipeline, "subpipelines", None) or {}).items():
            errors.extend(self._graph_errors(graph, f"[{sub_id}] "))
        return errors

    def _limit_errors(self, pipeline) -> list[str]:
        """What the limits refuse about a graph before it runs.

        Only what is soundly knowable from the JSON. A loop's cost is not:
        the number of passes comes from the data and the reservation of a
        pass from its arguments, so multiplying anything here would refuse
        graphs that fit. What *is* knowable is the shape — how many nodes a
        run must pass at the very least, how deep the subpipelines go, and
        what the retries ask for.
        """
        limits = self.limits
        errors: list[str] = []

        floor = limits.counters.get("steps")
        if floor is not None:
            least = _shortest_run(pipeline)
            if least is not None and least > floor:
                errors.append(
                    _("the shortest way through this graph is {nodes} nodes, "
                      "and the policy allows {steps:g} steps",
                      nodes=least, steps=floor)
                )

        deepest = limits.gauges.get("depth")
        if deepest is not None:
            depth = _subpipeline_depth(pipeline)
            if depth > deepest:
                errors.append(
                    _("subpipelines nest {depth} deep, "
                      "and the policy allows {allowed:g}",
                      depth=depth, allowed=deepest)
                )

        for node in pipeline.nodes:
            for retrier in getattr(node, "retry", None) or []:
                if (limits.max_retries is not None
                        and retrier.max_attempts > limits.max_retries):
                    errors.append(
                        _("{node}: retry asks for {asked} attempts, "
                          "and the policy allows {allowed}",
                          node=node.id, asked=retrier.max_attempts,
                          allowed=limits.max_retries)
                    )
                if (limits.max_delay_seconds is not None
                        and retrier.interval_seconds > limits.max_delay_seconds):
                    errors.append(
                        _("{node}: retry waits {asked:g}s between attempts, "
                          "and the policy allows {allowed:g}s",
                          node=node.id, asked=retrier.interval_seconds,
                          allowed=limits.max_delay_seconds)
                    )
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
                _("{where}{node}: node type '{type}' is not allowed by the policy",
                  where=where, node=node_id, type=type_)
            )
        if stage is not None and not self.allows_stage(stage):
            errors.append(
                _("{where}{node}: stage '{stage}' is not allowed by the policy",
                  where=where, node=node_id, stage=stage)
            )
        return errors


#: Everything the process has registered — the default, and what a host that
#: has no tenants to separate should keep using.
OPEN = Policy()


def _shortest_run(pipeline) -> int | None:
    """The fewest nodes a run can pass before it can end.

    A lower bound, and that is what makes it safe to refuse by: every real
    run does at least this much, so a graph whose cheapest path does not fit
    cannot finish at all. Ending means a terminal or a road that stops.
    """
    entry = pipeline.entry
    if not entry or not pipeline.has_node(entry):
        return None
    seen = {entry}
    frontier = [entry]
    steps = 1
    while frontier:
        nxt: list[str] = []
        for node_id in frontier:
            node = pipeline.get_node(node_id)
            targets = [t for t in node.order_targets() if pipeline.has_node(t)]
            if node.type == "terminal" or not targets:
                return steps
            for target in targets:
                if target not in seen:
                    seen.add(target)
                    nxt.append(target)
        frontier = nxt
        steps += 1
    return None


def _subpipeline_depth(pipeline) -> int:
    """How deep the declared subpipelines nest, counting from the root as 0."""

    def depth_of(graph: dict, seen: frozenset[str]) -> int:
        declared = graph.get("subpipelines") or {}
        best = 0
        for node in graph.get("nodes") or []:
            if node.get("type") != "subpipeline":
                continue
            child_id = node.get("subpipeline_id")
            if child_id in seen:
                continue  # a self-reference is caught elsewhere; do not spin here
            child = declared.get(child_id)
            inner = 1 if child is None else 1 + depth_of(
                {**child, "subpipelines": {**declared, **(child.get("subpipelines") or {})}},
                seen | {child_id},
            )
            best = max(best, inner)
        return best

    return depth_of(pipeline.raw_json, frozenset())
