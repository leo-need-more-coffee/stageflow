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
    """What a session may use and how much it may spend. Set by the host.

    `stages` and `node_types` restrict the registry for one session, so
    tenants can be given different sets of stages within one process. Nothing
    in the pipeline JSON can widen a policy.

    `None` means "no restriction"; an empty set means "nothing allowed".
    """

    stages: frozenset[str] | None = None
    node_types: frozenset[str] | None = None
    #: Allow cycles in the order edges. Off by default: a cycle is not bounded
    #: by anything in the graph and is usually a mistake. Enable it when
    #: something else stops runaway runs, typically a `steps` limit.
    allow_cycles: bool = False
    #: How much a session may spend. A fresh `Limits` per policy, so that
    #: mutating one cannot affect others.
    limits: Limits = field(default_factory=Limits)

    def __post_init__(self) -> None:
        object.__setattr__(self, "stages", _freeze(self.stages))
        object.__setattr__(self, "node_types", _freeze(self.node_types))

    @property
    def unrestricted(self) -> bool:
        """True when nothing is restricted (`allow_cycles` is not considered)."""
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
        """Everything this policy forbids in a pipeline, as a list of messages.

        Subpipelines are checked too, so that a graph that can be saved can
        also be run.
        """
        errors = self._limit_errors(pipeline)
        if self.stages is None and self.node_types is None:
            return errors  # no restrictions on stages or node types
        errors += [
            message
            for node in pipeline.nodes
            for message in self._node_errors(node.type, node.id, getattr(node, "stage", None))
        ]
        for sub_id, graph in (getattr(pipeline, "subpipelines", None) or {}).items():
            errors.extend(self._graph_errors(graph, f"[{sub_id}] "))
        return errors

    def _limit_errors(self, pipeline) -> list[str]:
        """Limit violations that can be detected without running the graph:
        the minimum number of steps, subpipeline depth, and retry settings.
        Loop costs depend on data and are not checked here."""
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
        """Same check on a subpipeline that is still raw JSON."""
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


#: No restrictions: everything registered in the process is allowed.
OPEN = Policy()


def _shortest_run(pipeline) -> int | None:
    """The minimum number of nodes any run must pass through (a lower bound:
    a graph whose shortest run exceeds the step limit cannot finish)."""
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
    """How deeply subpipelines nest (the root is 0)."""

    def depth_of(graph: dict, seen: frozenset[str]) -> int:
        declared = graph.get("subpipelines") or {}
        best = 0
        for node in graph.get("nodes") or []:
            if node.get("type") != "subpipeline":
                continue
            child_id = node.get("subpipeline_id")
            if child_id in seen:
                continue  # self-reference; reported elsewhere
            child = declared.get(child_id)
            inner = 1 if child is None else 1 + depth_of(
                {**child, "subpipelines": {**declared, **(child.get("subpipelines") or {})}},
                seen | {child_id},
            )
            best = max(best, inner)
        return best

    return depth_of(pipeline.raw_json, frozenset())
