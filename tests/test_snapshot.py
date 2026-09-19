import asyncio
import unittest

from stageflow import BaseStage, Context, Pipeline, Session, register_stage


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

        task = asyncio.create_task(session.run())
        while not session.is_waiting_for("go"):
            await asyncio.sleep(0.01)
        snap = session.snapshot()
        await session.input("go", {"v": 1})
        await task

        self.assertEqual(snap["current_node_id"], "wait")
        self.assertEqual(snap.get("context", {}).get("vars", {}), {})

        restored = Session.from_snapshot(snap)
        resume_task = asyncio.create_task(restored.run())
        while not restored.is_waiting_for("go"):
            await asyncio.sleep(0.01)
        await restored.input("go", {"v": 2})
        result = await resume_task
        self.assertEqual(result.result, {"status": "done"})


if __name__ == "__main__":
    unittest.main()
