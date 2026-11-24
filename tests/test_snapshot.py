import asyncio
import unittest

from stageflow import BaseStage, Context, Limits, Pipeline, Policy, Session, register_stage
from stageflow.core.policy import OPEN


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

        restored = Session.from_snapshot(snap, policy=OPEN)
        resume_task = asyncio.create_task(restored.run())
        while not restored.is_waiting_for("go"):
            await asyncio.sleep(0.01)
        await restored.input("go", {"v": 2})
        result = await resume_task
        self.assertEqual(result.result, {"status": "done"})


@register_stage("SnapshotSetStage")
class SnapshotSetStage(BaseStage):
    """
    description: "Writes a value"
    arguments:
      v: any
    outputs:
      v: any
    """

    async def run(self):
        self.set_outputs({"v": self.get_arguments()["v"]})


class SnapshotMidRunTests(unittest.IsolatedAsyncioTestCase):
    PIPELINE = {
        "entry": "set",
        "nodes": [
            {"id": "set", "type": "stage", "stage": "SnapshotSetStage",
             "arguments": {"const": {"v": 42}}, "outputs": {"v": "v"}, "next": "wait"},
            {"id": "wait", "type": "stage", "stage": "SnapshotWaitStage", "next": "end"},
            {"id": "end", "type": "terminal", "artifacts": ["v"]},
        ],
    }

    async def _paused_snapshot(self, **kwargs):
        session = Session(id="mid", pipeline=Pipeline.from_dict(self.PIPELINE), **kwargs)
        task = asyncio.create_task(session.run())
        while not session.is_waiting_for("go"):
            await asyncio.sleep(0.01)
        snap = session.snapshot()
        session.stop()
        await task
        return snap

    async def test_the_frame_is_the_current_one(self):
        snap = await self._paused_snapshot()
        self.assertEqual(snap["current_node_id"], "wait")
        self.assertEqual(snap["context"]["vars"], {"v": 42})

        restored = Session.from_snapshot(snap, policy=OPEN)
        task = asyncio.create_task(restored.run())
        while not restored.is_waiting_for("go"):
            await asyncio.sleep(0.01)
        await restored.input("go", {})
        result = await task
        self.assertEqual(result.artifacts, {"v": 42})

    async def test_a_policy_must_be_given(self):
        snap = await self._paused_snapshot()
        with self.assertRaises(TypeError):
            Session.from_snapshot(snap)

    async def test_the_policy_given_is_the_one_enforced(self):
        snap = await self._paused_snapshot()
        narrow = Policy(stages={"SnapshotSetStage"})
        from stageflow.exceptions import PipelineValidationError
        with self.assertRaises(PipelineValidationError):
            Session.from_snapshot(snap, policy=narrow)

    async def test_spending_carries_over(self):
        limits = Limits(counters={"steps": 100})
        snap = await self._paused_snapshot(limits=limits)
        spent = snap["meters"]["steps"]
        self.assertGreater(spent, 0)
        restored = Session.from_snapshot(snap, policy=Policy(limits=limits))
        self.assertEqual(restored.budget.spent("steps"), spent)
        # a restore cannot be a way to a fresh allowance
        # set and wait are spent; one more step fits validation (the shortest
        # road is three nodes) and not the run that is left
        self.assertEqual(spent, 2)
        tight = Limits(counters={"steps": spent + 1})
        restored = Session.from_snapshot(snap, policy=Policy(limits=tight))
        task = asyncio.create_task(restored.run())
        result = await asyncio.wait_for(task, 2)
        self.assertEqual(result.result["status"], "budget_exceeded")

    async def test_the_event_log_stays_bounded(self):
        from collections import deque
        snap = await self._paused_snapshot()
        restored = Session.from_snapshot(snap, policy=OPEN)
        self.assertIsInstance(restored.event_history, deque)
        self.assertIsNotNone(restored.event_history.maxlen)


if __name__ == "__main__":
    unittest.main()
