import asyncio
import unittest

from stageflow.core.stage import BaseStage, register_stage
from stageflow.core.pipeline import Pipeline
from stageflow.core.context import Context
from stageflow.core.session import Session


@register_stage("SnapshotWaitStage")
class SnapshotWaitStage(BaseStage):
    async def run(self):
        await self.wait_input("go", timeout=0.5)


class SessionSnapshotTests(unittest.IsolatedAsyncioTestCase):
    async def test_snapshot_round_trip_and_resume(self):
        pipeline_json = {
            "entry": "wait",
            "nodes": [
                {"id": "wait", "type": "stage", "stage": "SnapshotWaitStage", "next": "end"},
                {"id": "end", "type": "terminal", "result": {"status": "done"}},
            ],
        }
        pipeline = Pipeline.from_dict(pipeline_json)
        session = Session(id="snap", pipeline=pipeline, context=Context())

        # Run until waiting for input, then snapshot state.
        task = asyncio.create_task(session.run())
        while "go" not in session._waiting:
            await asyncio.sleep(0.01)
        snap = session.snapshot()
        # Finish original session to avoid leak.
        await session.input("go", {"v": 1})
        await task

        self.assertEqual(snap["current_node_id"], "wait")
        self.assertEqual(snap.get("context", {}).get("payload", {}), {})

        # Restore from snapshot and resume.
        restored = Session.from_snapshot(snap)
        resume_task = asyncio.create_task(restored.run())
        while "go" not in restored._waiting:
            await asyncio.sleep(0.01)
        await restored.input("go", {"v": 2})
        result = await resume_task
        self.assertEqual(result.result, {"status": "done"})


if __name__ == "__main__":
    unittest.main()
