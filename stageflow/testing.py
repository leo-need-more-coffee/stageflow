import asyncio
from dataclasses import dataclass
from typing import Sequence

from stageflow.core.context import Context
from stageflow.core.pipeline import Pipeline
from stageflow.core.session import Session, SessionResult
from stageflow.core.event import Event


@dataclass
class PipelineTestSpec:
    pipeline: dict | Pipeline
    inputs: Sequence[dict]
    payload: dict | None = None
    expected_result: dict | None = None
    expected_artifacts: dict | None = None
    expected_history: list[str] | None = None  # sequence of stage_id/node_id seen in events


async def run_pipeline_test(spec: PipelineTestSpec) -> tuple[SessionResult, list[Event]]:
    pipeline = spec.pipeline if isinstance(spec.pipeline, Pipeline) else Pipeline.from_dict(spec.pipeline)
    events: list[Event] = []

    def handler(ev: Event):
        events.append(ev)

    session = Session(id="test", pipeline=pipeline, context=Context(payload=spec.payload or {}), event_handler=handler)

    async def feed_inputs():
        for inp in spec.inputs:
            delay = inp.get("delay", 0)
            if delay:
                await asyncio.sleep(delay)
            wait_for_listener = inp.get("wait_for_listener", True)
            if wait_for_listener:
                waited = 0.0
                while inp["type"] not in session._waiting and waited < 1.0:
                    await asyncio.sleep(0.01)
                    waited += 0.01
            await session.input(inp["type"], inp.get("payload", {}))

    run_task = asyncio.create_task(session.run())
    await asyncio.gather(run_task, feed_inputs())
    result = run_task.result()

    if spec.expected_result is not None:
        if result.result != spec.expected_result:
            raise AssertionError(f"Expected result {spec.expected_result}, got {result.result}")
    if spec.expected_artifacts is not None:
        for k, v in spec.expected_artifacts.items():
            if result.artifacts.get(k) != v:
                raise AssertionError(f"Expected artifact {k}={v}, got {result.artifacts.get(k)}")
    if spec.expected_history is not None:
        seen = [ev.stage_id or ev.node_id for ev in events if (ev.stage_id or ev.node_id)]
        if seen != spec.expected_history:
            raise AssertionError(f"Expected history {spec.expected_history}, got {seen}")

    return result, events
