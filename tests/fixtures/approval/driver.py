"""A host in miniature, run as its own process by the system tests.

    python driver.py run <lines.json> <state.json>
        runs the approval; prints the outcome as JSON. If the run stops to
        ask a person, the session is snapshotted into <state.json>, the
        process exits, and the outcome is {"status": "waiting"}.

    python driver.py resume <state.json> <decision>
        a new process: restores the snapshot under the host's policy, gives
        the decision and prints the outcome.

Everything a host does is here — its stages, its policy, loading a pipeline
from disk, storing a session between two processes — and nothing else.
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import stages  # noqa: E402,F401 - registers the host's stages

from stageflow import Context, Limits, Pipeline, Policy, Session  # noqa: E402

POLICY = Policy(
    stages={"Total", "CheckLine", "Notify", "AskApproval"},
    limits=Limits(counters={"steps": 60, "messages": 5, "checks": 20},
                  gauges={"concurrency": 2, "depth": 1}),
)


def outcome(result) -> dict:
    return {"status": "finished", "result": result.result,
            "artifacts": result.artifacts, "meters": result.meters,
            "events": [e.type for e in result.history]}


async def run(lines_path: str, state_path: str) -> dict:
    pipeline = Pipeline.from_dict(json.loads((HERE / "pipeline.json").read_text()))
    pipeline.validate(POLICY)
    lines = json.loads(Path(lines_path).read_text())
    session = Session(id="expense-1", pipeline=pipeline,
                      context=Context(vars={"lines": lines}), policy=POLICY)
    task = asyncio.create_task(session.run())
    while not task.done():
        if session.is_waiting_for("approval"):
            snapshot = session.snapshot()
            Path(state_path).write_text(json.dumps(snapshot, default=str))
            session.stop()
            await task
            return {"status": "waiting", "at": snapshot["current_node_id"],
                    "meters": snapshot["meters"]}
        await asyncio.sleep(0.01)
    return outcome(task.result())


async def resume(state_path: str, decision: str) -> dict:
    snapshot = json.loads(Path(state_path).read_text())
    session = Session.from_snapshot(snapshot, policy=POLICY)
    task = asyncio.create_task(session.run())
    while not session.is_waiting_for("approval"):
        if task.done():
            return outcome(task.result())
        await asyncio.sleep(0.01)
    await session.input("approval", {"decision": decision})
    return outcome(await task)


if __name__ == "__main__":
    command, *args = sys.argv[1:]
    action = {"run": run, "resume": resume}[command]
    print(json.dumps(asyncio.run(action(*args)), default=str))
