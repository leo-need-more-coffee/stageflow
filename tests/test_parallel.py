import asyncio
import unittest

from stageflow import BaseStage, Context, Pipeline, Session, register_stage
from stageflow.exceptions import BranchError


@register_stage("WriteStage")
class WriteStage(BaseStage):
    async def run(self):
        self.set_outputs({"value": self.get_arguments().get("value")})


@register_stage("SlowTraceStage")
class SlowTraceStage(BaseStage):
    """Спит и оставляет след в разделяемом списке — по нему видно,
    доиграла ветка до конца или была отменена."""

    trace: list[str] = []

    async def run(self):
        args = self.get_arguments()
        await asyncio.sleep(args.get("seconds", 0.2))
        type(self).trace.append(args.get("mark", "done"))
        self.set_outputs({"mark": args.get("mark", "done")})


def _parallel_pipeline(branch_outputs: dict[str, str]) -> Pipeline:
    """Две ветки, каждая пишет константный аргумент в свою (или общую) переменную."""
    return Pipeline.from_dict({
        "entry": "fan_out",
        "nodes": [
            {"id": "fan_out", "type": "parallel",
             "branches": [{"id": "left", "entry": "left"}, {"id": "right", "entry": "right"}],
             "next": "end"},
            {"id": "left", "type": "stage", "stage": "WriteStage", "arguments": {"const": {"value": 1}},
             "outputs": {"value": branch_outputs["left"]}},
            {"id": "right", "type": "stage", "stage": "WriteStage", "arguments": {"const": {"value": 2}},
             "outputs": {"value": branch_outputs["right"]}},
            {"id": "end", "type": "terminal", "result": {"status": "ok"},
             "artifacts": sorted(set(branch_outputs.values()))},
        ],
    })


class ParallelTests(unittest.IsolatedAsyncioTestCase):
    async def test_branches_merge_disjoint_writes(self):
        pipeline = _parallel_pipeline({"left": "a", "right": "b"})
        result = await Session(id="p", pipeline=pipeline, context=Context()).run()
        self.assertEqual(result.artifacts["a"], 1)
        self.assertEqual(result.artifacts["b"], 2)

    async def test_conflicting_writes_raise(self):
        pipeline = _parallel_pipeline({"left": "same", "right": "same"})
        session = Session(id="p", pipeline=pipeline, context=Context())
        with self.assertRaises(BranchError):
            await session.run()

    async def test_branch_write_to_existing_name_is_reported(self):
        """Diff берётся против бейзлайна, поэтому запись ветки в имя, жившее
        до ``parallel``, остаётся branch-local (MEMORY_MODEL.md §5). Молча
        терять её нельзя: имена уходят в ``parallel_completed``."""
        pipeline = _parallel_pipeline({"left": "n", "right": "fresh"})
        session = Session(id="p", pipeline=pipeline, context=Context(vars={"n": 0}))
        result = await session.run()
        self.assertEqual(result.artifacts["n"], 0)
        self.assertEqual(result.artifacts["fresh"], 2)
        completed = next(e for e in session.event_history if e.type == "parallel_completed")
        self.assertEqual(completed.payload["merged"], ["fresh"])
        self.assertEqual(completed.payload["dropped"], ["left.n"])

    async def test_untouched_baseline_is_not_reported_as_dropped(self):
        pipeline = _parallel_pipeline({"left": "a", "right": "b"})
        session = Session(id="p", pipeline=pipeline, context=Context(vars={"n": 0}))
        await session.run()
        completed = next(e for e in session.event_history if e.type == "parallel_completed")
        self.assertEqual(completed.payload["dropped"], [])

    async def test_cancel_on_error_cancels_siblings(self):
        """cancel_on_error=true (default): падение ветки отменяет остальных —
        медленная ветка не должна доиграть до конца."""
        SlowTraceStage.trace = []
        pipeline = Pipeline.from_dict({
            "entry": "fan_out",
            "nodes": [
                {"id": "fan_out", "type": "parallel",
                 "branches": [{"id": "boom", "entry": "boom"}, {"id": "slow", "entry": "slow"}],
                 "next": "end"},
                {"id": "boom", "type": "stage", "stage": "FailStage",
                 "arguments": {"const": {"message": "fast failure"}}},
                {"id": "slow", "type": "stage", "stage": "SlowTraceStage",
                 "arguments": {"const": {"seconds": 0.3, "mark": "slow finished"}},
                 "outputs": {"mark": "mark"}},
                {"id": "end", "type": "terminal", "result": {"status": "ok"}},
            ],
        })
        session = Session(id="p", pipeline=pipeline, context=Context())
        with self.assertRaises(BranchError):
            await session.run()
        self.assertEqual(SlowTraceStage.trace, [])
        self.assertIn("parallel_cancelled", [e.type for e in session.event_history])

    async def test_cancel_on_error_false_lets_siblings_finish(self):
        SlowTraceStage.trace = []
        pipeline = Pipeline.from_dict({
            "entry": "fan_out",
            "nodes": [
                {"id": "fan_out", "type": "parallel", "cancel_on_error": False,
                 "branches": [{"id": "boom", "entry": "boom"}, {"id": "slow", "entry": "slow"}],
                 "next": "end"},
                {"id": "boom", "type": "stage", "stage": "FailStage",
                 "arguments": {"const": {"message": "fast failure"}}},
                {"id": "slow", "type": "stage", "stage": "SlowTraceStage",
                 "arguments": {"const": {"seconds": 0.1, "mark": "slow finished"}},
                 "outputs": {"mark": "mark"}},
                {"id": "end", "type": "terminal", "result": {"status": "ok"}},
            ],
        })
        session = Session(id="p", pipeline=pipeline, context=Context())
        with self.assertRaises(BranchError):
            await session.run()
        self.assertEqual(SlowTraceStage.trace, ["slow finished"])

    async def test_branch_failure_raises_branch_error(self):
        pipeline = Pipeline.from_dict({
            "entry": "fan_out",
            "nodes": [
                {"id": "fan_out", "type": "parallel",
                 "branches": [{"id": "boom", "entry": "boom"}], "next": "end"},
                {"id": "boom", "type": "stage", "stage": "FailStage",
                 "arguments": {"const": {"message": "branch down"}}},
                {"id": "end", "type": "terminal", "result": {"status": "ok"}},
            ],
        })
        session = Session(id="p", pipeline=pipeline, context=Context())
        with self.assertRaises(BranchError):
            await session.run()


if __name__ == "__main__":
    unittest.main()
