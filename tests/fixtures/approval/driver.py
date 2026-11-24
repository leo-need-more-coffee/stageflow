"""A host in miniature, run as its own process by the system tests.

    python driver.py run <lines.json> <runs.db>
        runs the approval; prints the outcome as JSON. If the run reaches
        the `wait` node and nobody has answered, it is saved in <runs.db>,
        the process exits, and the outcome is {"status": "waiting"}.

    python driver.py resume <runs.db> <run id> <decision>
        a new process: restores the run under the host's policy, gives the
        decision and prints the outcome.

Everything a host does is here — its stages, its policy, loading a pipeline
from disk, keeping a run between two processes — and nothing else.
"""
from __future__ import annotations

import asyncio
import json
import sys
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import stages  # noqa: E402,F401 - registers the host's stages

from stageflow import (  # noqa: E402
    Context,
    Limits,
    Pipeline,
    Policy,
    Session,
    SQLiteStore,
)

POLICY = Policy(
    stages={"Total", "CheckLine", "Notify"},
    limits=Limits(counters={"steps": 60, "messages": 5, "checks": 20},
                  gauges={"concurrency": 2, "depth": 1}),
)


def outcome(result, run_id: str = "") -> dict:
    if result.waiting:
        return {"status": "waiting", "at": result.waiting["node"], "meters": result.meters,
                "run": run_id}
    return {"status": "finished", "result": result.result,
            "artifacts": result.artifacts, "meters": result.meters,
            "events": [e.type for e in result.history]}


async def run(lines_path: str, store_path: str) -> dict:
    pipeline = Pipeline.from_dict(json.loads((HERE / "pipeline.json").read_text()))
    pipeline.validate(POLICY)
    lines = json.loads(Path(lines_path).read_text())
    session = Session(id=f"expense-{uuid.uuid4().hex[:8]}", pipeline=pipeline,
                      context=Context(vars={"lines": lines}), policy=POLICY,
                      store=SQLiteStore(store_path))
    return outcome(await session.run(), session.id)


async def resume(store_path: str, run_id: str, decision: str) -> dict:
    session = await Session.restore(SQLiteStore(store_path), run_id, policy=POLICY)
    await session.input("approval", {"decision": decision})
    return outcome(await session.run())


if __name__ == "__main__":
    command, *args = sys.argv[1:]
    action = {"run": run, "resume": resume}[command]
    print(json.dumps(asyncio.run(action(*args)), default=str))
