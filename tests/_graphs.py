"""Random pipelines for the property tests.

A pipeline is built from blocks, the way a person composes one: a stage, two
blocks in a row, a condition or a switch whose roads meet again, a parallel,
a map, a try. Every block is built "towards" the node that comes after it, so
the result is always a valid graph — the property tests are about running
graphs, not about the validator, which is fuzzed separately.

Every stage writes a variable named after itself. Names never collide, so a
parallel merge never conflicts, and the final frame says exactly which stages
ran: that is what lets a test compare two runs of the same graph.
"""
from __future__ import annotations

import asyncio
import itertools
from dataclasses import dataclass, field

from hypothesis import strategies as st

from stageflow import BaseStage, register_stage


@register_stage("PropWork")
class PropWork(BaseStage):
    """
    description: "Records that it ran, yields to the loop, maybe fails"
    arguments:
      tag: string
      fail: boolean
    outputs:
      tag: string
    """

    timeout = None

    async def run(self):
        probe = getattr(self.session, "probe", None) or self.session.__dict__.setdefault(
            "probe", Probe())
        probe.live += 1
        probe.peak = max(probe.peak, probe.live)
        probe.ran += 1
        try:
            # give every other task a turn, so overlapping really overlaps
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            if self.get_arguments().get("fail"):
                raise PropFailure(self.get_arguments()["tag"])
        finally:
            probe.live -= 1
        self.set_outputs({"tag": self.get_arguments()["tag"]})


class PropFailure(Exception):
    pass


@dataclass
class Probe:
    live: int = 0
    peak: int = 0
    ran: int = 0


# ------------------------------------------------------------------ blocks
# A block is a small tree; `build` turns it into nodes.

def blocks(max_leaves: int = 12) -> st.SearchStrategy:
    """Graphs whose stages all succeed."""
    leaf = st.just(("stage", False))

    def extend(children):
        return st.one_of(
            st.tuples(st.just("seq"), children, children),
            st.tuples(st.just("if"), st.booleans(), children, children),
            st.tuples(st.just("switch"), st.integers(0, 3),
                      st.lists(children, min_size=1, max_size=3), children),
            st.tuples(st.just("parallel"), st.lists(children, min_size=1, max_size=3)),
            st.tuples(st.just("map"), st.integers(0, 3),
                      st.sampled_from(["sequential", "parallel"]), children),
            st.tuples(st.just("try"), children, children),
        )

    return st.recursive(leaf, extend, max_leaves=max_leaves)


def failing_blocks(max_leaves: int = 10) -> st.SearchStrategy:
    """Like `blocks`, but some stages fail."""
    leaf = st.builds(lambda fail: ("stage", fail), st.booleans())

    def extend(children):
        return st.one_of(
            st.tuples(st.just("seq"), children, children),
            st.tuples(st.just("parallel"), st.lists(children, min_size=1, max_size=3)),
            st.tuples(st.just("map"), st.integers(0, 3),
                      st.sampled_from(["sequential", "parallel"]), children),
            st.tuples(st.just("try"), children, children),
        )

    return st.recursive(leaf, extend, max_leaves=max_leaves)


@dataclass
class Built:
    nodes: list[dict] = field(default_factory=list)
    stages: list[str] = field(default_factory=list)
    _ids: itertools.count = field(default_factory=itertools.count)

    def fresh(self, prefix: str) -> str:
        return f"{prefix}{next(self._ids)}"


def build(block, after: str | None, out: Built) -> str:
    """Add the nodes of `block`, ending in `after`; return the block's first id."""
    kind = block[0]
    if kind == "stage":
        node_id = out.fresh("s")
        out.stages.append(node_id)
        out.nodes.append({
            "id": node_id, "type": "stage", "stage": "PropWork",
            "arguments": {"const": {"tag": node_id, "fail": block[1]}},
            "outputs": {"tag": node_id},
            **({"next": after} if after else {}),
        })
        return node_id
    if kind == "seq":
        second = build(block[2], after, out)
        return build(block[1], second, out)
    if kind == "if":
        node_id = out.fresh("c")
        then = build(block[2], after, out)
        other = build(block[3], after, out)
        out.nodes.append({"id": node_id, "type": "condition",
                          "condition": "true" if block[1] else "false",
                          "then": then, "else": other})
        return node_id
    if kind == "switch":
        node_id = out.fresh("w")
        picked, cases, default = block[1], block[2], block[3]
        out.nodes.append({
            "id": node_id, "type": "switch",
            "cases": [{"when": f"{i} == {picked}", "next": build(case, after, out)}
                      for i, case in enumerate(cases)],
            "default": build(default, after, out),
        })
        return node_id
    if kind == "parallel":
        node_id = out.fresh("p")
        branches = [build(branch, None, out) for branch in block[1]]
        out.nodes.append({"id": node_id, "type": "parallel",
                          "branches": [{"id": b, "entry": b} for b in branches],
                          **({"next": after} if after else {})})
        return node_id
    if kind == "map":
        node_id = out.fresh("m")
        body = build(block[3], None, out)
        out.nodes.append({"id": node_id, "type": "map",
                          "items": f"[{', '.join(str(i) for i in range(block[1]))}]",
                          "body": body, "mode": block[2],
                          **({"next": after} if after else {})})
        return node_id
    if kind == "try":
        node_id = out.fresh("t")
        body = build(block[1], None, out)
        handler = build(block[2], None, out)
        out.nodes.append({"id": node_id, "type": "try", "body": body,
                          "except": [{"error_equals": ["PropFailure"], "next": handler}],
                          **({"next": after} if after else {})})
        return node_id
    raise AssertionError(kind)


def pipeline_of(block) -> tuple[dict, Built]:
    out = Built()
    end = {"id": "end", "type": "terminal", "result": {"status": "done"}}
    first = build(block, "end", out)
    nodes = [{"id": "start", "type": "entry", "next": first}, *out.nodes, end]
    return {"nodes": nodes}, out
