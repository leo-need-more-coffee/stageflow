"""Хелпер для декларативного тестирования пайплайнов.

Тест описывается данными (:class:`PipelineTestSpec`): пайплайн, скармливаемые
входы и ожидания по result/artifacts/истории пройденных узлов.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Sequence

from .core.context import Context
from .core.event import Event
from .core.pipeline import Pipeline
from .core.session import Session, SessionResult

#: Сколько ждать появления слушателя ввода, прежде чем отправить input вслепую.
_LISTENER_TIMEOUT = 1.0
_POLL_INTERVAL = 0.01


@dataclass(slots=True)
class PipelineTestSpec:
    pipeline: dict | Pipeline
    inputs: Sequence[dict]
    payload: dict | None = None
    expected_result: dict | None = None
    expected_artifacts: dict | None = None
    #: Ожидаемая последовательность УЗЛОВ по событиям (повторы подряд схлопываются).
    expected_history: list[str] | None = None


async def run_pipeline_test(spec: PipelineTestSpec) -> tuple[SessionResult, list[Event]]:
    """Прогоняет пайплайн, скармливая ``spec.inputs``, и сверяет результат
    с ожиданиями; на расхождении — AssertionError."""
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
        # у одного узла несколько событий (started/completed/emit), а спека
        # описывает последовательность УЗЛОВ — схлопываем повторы подряд
        seen: list[str] = []
        for event in events:
            node_id = event.stage_id or event.node_id
            if node_id and (not seen or seen[-1] != node_id):
                seen.append(node_id)
        if seen != spec.expected_history:
            raise AssertionError(f"Expected history {spec.expected_history}, got {seen}")
