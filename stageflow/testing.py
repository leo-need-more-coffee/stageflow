"""Helpers for testing stages and pipelines.

    from stageflow.testing import run_stage, isolated_stages, run_pipeline_test

- `run_stage` runs one stage class through a real `Session` (timeouts,
  budget, events, inputs) and checks it kept the contract in its docstring.
- `isolated_stages` undoes stage registrations made inside it, so a test can
  register a stage of its own and be run twice in one process.
- `run_pipeline_test` runs a whole pipeline, feeds it inputs and compares the
  result with what was expected.
"""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Iterator, Sequence

from .core.context import Context
from .core.event import Event
from .core.nodes import StageNode, TerminalNode
from .core.pipeline import Pipeline
from .core.policy import Limits, Policy
from .core.session import Session, SessionResult
from .core.spec import parse_docstring_spec, parse_fields
from .core.stage import BaseStage, _stages
from .core.typesys import TypeRegistry, parse_type
from .exceptions import TypeCheckError, TypeDeclarationError

_LISTENER_TIMEOUT = 1.0
_POLL_INTERVAL = 0.01

__all__ = [
    "ContractViolation",
    "PipelineTestSpec",
    "StageRun",
    "isolated_stages",
    "run_pipeline_test",
    "run_stage",
]


# ---------------------------------------------------------------- one stage

class ContractViolation(AssertionError):
    """A stage did not do what its docstring says: an argument or output it
    does not declare, a required one missing, or a value of the wrong type."""


@dataclass(slots=True)
class StageRun:
    """What one stage run produced."""

    outputs: dict[str, Any]
    #: Every event of the run: the stage's own (`emit`) and the lifecycle
    #: ones (`stage_started`, `stage_completed`, `waiting_for_input`, ...).
    events: list[Event] = field(default_factory=list)
    #: What the stage reported with `charge()`.
    charged: dict[str, float] = field(default_factory=dict)
    #: The budget after the run, by meter (reservations included).
    meters: dict[str, Any] = field(default_factory=dict)

    def emitted(self, type_: str) -> list[dict]:
        """Payloads of the events of one type, in order."""
        return [event.payload for event in self.events if event.type == type_]


async def run_stage(
    stage: type[BaseStage] | str,
    arguments: dict[str, Any] | None = None,
    *,
    inputs: Sequence[dict] = (),
    limits: Limits | None = None,
    policy: Policy | None = None,
    check_contract: bool = True,
) -> StageRun:
    """Run one stage the way the engine does and return what it produced.

    `stage` is a registered name or a registered class. `inputs` are queued
    before the stage starts, as `{"type": ..., "payload": ...}`, so a stage
    that waits for input gets them straight away. Exceptions from the stage
    propagate unchanged; a budget refusal raises `BudgetExceeded`.

    With `check_contract` (the default), arguments and outputs are checked
    against the docstring spec: nothing undeclared, nothing required
    missing, and values of the declared type. A failure is a
    `ContractViolation`.
    """
    name = stage if isinstance(stage, str) else stage.stage_name
    stage_cls = _stages.get(name)
    arguments = dict(arguments or {})
    if check_contract:
        _check_fields(stage_cls, "arguments", arguments)

    events: list[Event] = []
    session = Session(
        id="stage-test",
        pipeline=Pipeline(entry="end", nodes=[TerminalNode(id="end")]),
        event_handler=events.append,
        policy=policy,
        limits=limits,
    )
    for entry in inputs:
        session.inputs.deliver({"type": entry["type"], "payload": entry.get("payload", {})})

    outputs = await session.run_stage(StageNode(id=name, stage=name), arguments)

    if check_contract:
        _check_fields(stage_cls, "outputs", outputs)
    return StageRun(
        outputs=outputs,
        events=events,
        charged=next(({k: v for k, v in event.payload.items() if k != "stage"}
                      for event in events if event.type == "stage_charged"), {}),
        meters=session.budget.report(),
    )


#: Loose names stage specs use, as type expressions.
_HINT_ALIASES = {"str": "string", "list": "list<any>", "dict": "map<any>",
                 "object": "map<any>"}


def _check_fields(stage_cls: type, section: str, values: dict) -> None:
    fields = parse_fields(parse_docstring_spec(stage_cls.__doc__).get(section))
    if not fields:
        return  # no spec: nothing to hold the stage to
    declared = {f.name: f for f in fields}
    kind = "argument" if section == "arguments" else "output"
    stage = stage_cls.stage_name

    problems = [f"{stage}: {kind} '{name}' is not declared"
                for name in values if name not in declared]
    problems += [f"{stage}: required {kind} '{f.name}' is missing"
                 for f in fields if not f.optional and f.name not in values]
    registry = TypeRegistry.from_dict(None)
    for name, value in values.items():
        spec = declared.get(name)
        if spec is None or not isinstance(spec.type, str) or value is None:
            continue
        try:
            expected = parse_type(_HINT_ALIASES.get(spec.type.lower(), spec.type))
            expected.check(value, f"{kind} {name}", registry)
        except TypeDeclarationError:
            continue  # a hint the type system does not know; not ours to judge
        except TypeCheckError as exc:
            problems.append(f"{stage}: {exc}")
    if problems:
        raise ContractViolation("; ".join(problems))


@contextmanager
def isolated_stages() -> Iterator[None]:
    """Forget every stage registered inside the block when it ends.

        def setUp(self):
            self.enterContext(isolated_stages())

    Without it, a test that registers a stage in its body fails the second
    time it runs in the same process (mutation testers, `--count`, a REPL).
    """
    saved = dict(_stages._items)
    try:
        yield
    finally:
        _stages._items.clear()
        _stages._items.update(saved)


# ---------------------------------------------------------------- a pipeline

@dataclass(slots=True)
class PipelineTestSpec:
    pipeline: dict | Pipeline
    inputs: Sequence[dict]
    payload: dict | None = None
    expected_result: dict | None = None
    expected_artifacts: dict | None = None
    expected_history: list[str] | None = None


async def run_pipeline_test(spec: PipelineTestSpec) -> tuple[SessionResult, list[Event]]:
    """Run a pipeline, feed it `spec.inputs` as it asks for them, and check
    the expectations. Returns the result and every event."""
    pipeline = (
        spec.pipeline
        if isinstance(spec.pipeline, Pipeline)
        else Pipeline.from_dict(spec.pipeline)
    )
    events: list[Event] = []
    session = Session(
        id="test",
        pipeline=pipeline,
        context=Context(vars=spec.payload or {}),
        event_handler=events.append,
    )

    run_task = asyncio.create_task(session.run())
    await asyncio.gather(run_task, _feed_inputs(session, spec.inputs))
    result = run_task.result()

    _check_expectations(spec, result, events)
    return result, events


async def _feed_inputs(session: Session, inputs: Sequence[dict]) -> None:
    for inp in inputs:
        delay = inp.get("delay", 0)
        if delay:
            await asyncio.sleep(delay)
        if inp.get("wait_for_listener", True):
            await _wait_for_listener(session, inp["type"])
        await session.input(inp["type"], inp.get("payload", {}))


async def _wait_for_listener(session: Session, type_: str) -> None:
    waited = 0.0
    while not session.is_waiting_for(type_) and waited < _LISTENER_TIMEOUT:
        await asyncio.sleep(_POLL_INTERVAL)
        waited += _POLL_INTERVAL


def _check_expectations(
    spec: PipelineTestSpec, result: SessionResult, events: list[Event]
) -> None:
    if spec.expected_result is not None and result.result != spec.expected_result:
        raise AssertionError(f"Expected result {spec.expected_result}, got {result.result}")

    if spec.expected_artifacts is not None:
        for key, expected in spec.expected_artifacts.items():
            actual = result.artifacts.get(key)
            if actual != expected:
                raise AssertionError(f"Expected artifact {key}={expected}, got {actual}")

    if spec.expected_history is not None:
        seen: list[str] = []
        for event in events:
            node_id = event.stage_id or event.node_id
            if node_id and (not seen or seen[-1] != node_id):
                seen.append(node_id)
        if seen != spec.expected_history:
            raise AssertionError(f"Expected history {spec.expected_history}, got {seen}")
