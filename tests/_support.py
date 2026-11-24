"""What the unit tests share: a stand-in for a session.

A unit test checks one class without the rest of the runtime around it. Nodes
and stages talk to their session through a handful of attributes — the CEL
engine, the pipeline, the budget, an event sink — so a stub that has exactly
those, and records what it was told, is enough to run them in isolation. What
a unit test then sees going wrong is the unit, not the walker, the debugger or
the policy that a real `Session` would bring along.

Not a test module (no `test_` prefix), so discovery leaves it alone; the tests
import it as a sibling.
"""
from __future__ import annotations

import asyncio
from typing import Any

from stageflow import Budget, CelEngine, Limits, Pipeline


class StubSession:
    """The attributes a node or a stage reaches for, and a log of events."""

    def __init__(self, pipeline: Pipeline | None = None, limits: Limits | None = None):
        self.id = "stub"
        self.pipeline = pipeline or Pipeline(entry="", nodes=[])
        self.cel = CelEngine()
        self.budget = Budget(limits)
        self.finished = False
        self.events: list[tuple[str, str, dict]] = []
        self.emitted: list[Any] = []

    def emit_node_event(self, type_: str, node, payload: dict | None = None) -> None:
        self.events.append((type_, node.id, payload or {}))

    def emit(self, event) -> None:
        self.emitted.append(event)

    def event_types(self) -> list[str]:
        return [type_ for type_, _, _ in self.events]


async def run_stage(stage_cls, session: StubSession | None = None, **arguments) -> dict:
    """Run one stage class on its own and return what it set as outputs."""
    stage = stage_cls(stage_id="unit", arguments=arguments,
                      session=session or StubSession())
    await stage.run()
    return stage.collected_outputs


def run(coro):
    return asyncio.run(coro)
