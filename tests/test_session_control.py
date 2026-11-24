import asyncio
import unittest

from stageflow import BaseStage, Context, Pipeline, Session, register_stage


@register_stage("SlowStage")
class SlowStage(BaseStage):
    skipable = True

    async def run(self):
        await asyncio.sleep(self.get_arguments().get("seconds", 10))
        self.set_outputs({"done": True})


@register_stage("StubbornStage")
class StubbornStage(BaseStage):
    """Cannot be skipped: `skipable` is False by default."""

    async def run(self):
        self.set_outputs({"done": True})


def _slow_pipeline() -> Pipeline:
    return Pipeline.from_dict({
        "entry": "slow",
        "nodes": [
            {"id": "slow", "type": "stage", "stage": "SlowStage", "next": "end"},
            {"id": "end", "type": "terminal", "result": {"status": "ok"}},
        ],
    })


class SessionControlTests(unittest.IsolatedAsyncioTestCase):
    async def test_stop_interrupts_running_node(self):
        session = Session(id="ctl", pipeline=_slow_pipeline(), context=Context())
        task = asyncio.create_task(session.run())
        await asyncio.sleep(0.05)

        await session.input("command", {"name": "stop"})
        result = await asyncio.wait_for(task, timeout=1.0)

        self.assertEqual(result.result, {"result": "stopped"})
        self.assertIn("session_stopped", [e.type for e in session.event_history])

    async def test_pause_blocks_next_node_until_resume(self):
        pipeline = Pipeline.from_dict({
            "entry": "first",
            "nodes": [
                {"id": "first", "type": "stage", "stage": "SlowStage",
                 "arguments": {"const": {"seconds": 0.01}}, "next": "second"},
                {"id": "second", "type": "stage", "stage": "SlowStage",
                 "arguments": {"const": {"seconds": 0.01}},
                 "outputs": {"done": "done"}, "next": "end"},
                {"id": "end", "type": "terminal", "result": {"status": "ok"}, "artifacts": ["done"]},
            ],
        })
        session = Session(id="pause", pipeline=pipeline, context=Context())
        await session.input("command", {"name": "pause"})

        task = asyncio.create_task(session.run())
        await asyncio.sleep(0.05)
        self.assertFalse(task.done())

        await session.input("command", {"name": "resume"})
        result = await asyncio.wait_for(task, timeout=1.0)
        self.assertEqual(result.result, {"status": "ok"})
        self.assertIs(result.artifacts["done"], True)

    async def test_stop_while_paused(self):
        session = Session(id="ps", pipeline=_slow_pipeline(), context=Context())
        await session.input("command", {"name": "pause"})
        task = asyncio.create_task(session.run())
        await asyncio.sleep(0.02)

        await session.input("command", {"name": "stop"})
        result = await asyncio.wait_for(task, timeout=1.0)
        self.assertEqual(result.result, {"result": "stopped"})

    async def test_stop_is_idempotent(self):
        session = Session(id="idem", pipeline=_slow_pipeline(), context=Context())
        session.stop()
        session.stop()
        await session.input("command", {"name": "stop"})
        stopped_events = [e for e in session.event_history if e.type == "session_stopped"]
        self.assertEqual(len(stopped_events), 1)

    async def test_skip_skipable_stage(self):
        session = Session(id="skip", pipeline=_slow_pipeline(), context=Context())
        session.request_skip()
        result = await asyncio.wait_for(session.run(), timeout=1.0)
        self.assertEqual(result.result, {"status": "ok"})
        self.assertIn("stage_skipped", [e.type for e in session.event_history])

    async def test_a_skip_says_which_node_and_stage(self):
        session = Session(id="skip2", pipeline=_slow_pipeline(), context=Context())
        session.request_skip()
        await asyncio.wait_for(session.run(), timeout=1.0)
        skipped = [e for e in session.event_history if e.type == "stage_skipped"]
        self.assertEqual([(e.stage_id, e.payload) for e in skipped],
                         [("slow", {"stage": "SlowStage"})])

    async def test_skipping_the_last_node_ends_the_run(self):
        # a stage with no `next` is the end of the graph, skipped or not
        session = Session(id="skip3", pipeline=Pipeline.from_dict({
            "entry": "slow",
            "nodes": [{"id": "slow", "type": "stage", "stage": "SlowStage"}],
        }), context=Context())
        session.request_skip()
        result = await asyncio.wait_for(session.run(), timeout=1.0)
        self.assertIsNone(result.result)
        self.assertIn("stage_skipped", [e.type for e in session.event_history])

    async def test_a_stage_that_cannot_be_skipped_runs_and_says_so(self):
        session = Session(id="deny", pipeline=Pipeline.from_dict({
            "entry": "s",
            "nodes": [
                {"id": "s", "type": "stage", "stage": "StubbornStage",
                 "outputs": {"done": "done"}, "next": "end"},
                {"id": "end", "type": "terminal", "artifacts": ["done"]},
            ],
        }), context=Context())
        session.request_skip()
        result = await asyncio.wait_for(session.run(), timeout=1.0)
        self.assertIs(result.artifacts["done"], True)
        denied = [e for e in session.event_history if e.type == "skip_denied"]
        self.assertEqual([(e.stage_id, e.payload) for e in denied],
                         [("s", {"stage": "StubbornStage"})])
        # the request is used up: it does not skip a later stage instead
        self.assertNotIn("stage_skipped", [e.type for e in session.event_history])

    async def test_one_request_skips_one_stage(self):
        session = Session(id="once", pipeline=Pipeline.from_dict({
            "entry": "a",
            "nodes": [
                {"id": "a", "type": "stage", "stage": "SlowStage", "next": "b"},
                {"id": "b", "type": "stage", "stage": "SlowStage",
                 "arguments": {"const": {"seconds": 0}}, "outputs": {"done": "done"},
                 "next": "end"},
                {"id": "end", "type": "terminal", "artifacts": ["done"]},
            ],
        }), context=Context())
        session.request_skip()
        result = await asyncio.wait_for(session.run(), timeout=1.0)
        skipped = [e.stage_id for e in session.event_history if e.type == "stage_skipped"]
        self.assertEqual(skipped, ["a"])
        self.assertIs(result.artifacts["done"], True)


if __name__ == "__main__":
    unittest.main()
