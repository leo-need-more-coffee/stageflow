"""The `wait` node and durable runs: suspend at a wait, restore anywhere."""
import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from stageflow import (
    BaseStage,
    CheckpointConflict,
    CheckpointError,
    Limits,
    MemoryStore,
    Pipeline,
    Policy,
    Session,
    SQLiteStore,
    register_stage,
)
from stageflow.exceptions import PipelineValidationError
from stageflow.observability import Recorder
from stageflow.testing import isolated_stages

OPEN = Policy()
CALLS: dict[str, list[str]] = {}


@register_stage("WaitCountStage")
class CountStage(BaseStage):
    """
    description: "Records that it ran, and its idempotency key"
    arguments:
      tag:
        type: string
    outputs:
      tag:
        type: string
    """

    timeout = None

    async def run(self):
        tag = self.get_arguments()["tag"]
        CALLS.setdefault(tag, []).append(self.idempotency_key)
        self.charge(units=1)
        self.set_outputs({"tag": tag})


def approval(timeout=None):
    """start -> before -> ask (wait) -> after -> done; late on timeout."""
    ask = {"id": "ask", "type": "wait", "input": "approval",
           "outputs": {"ok": "approved", "by": "approver"}, "next": "after"}
    if timeout is not None:
        ask.update(timeout=timeout, on_timeout="late")
    return Pipeline.from_dict({"entry": "start", "nodes": [
        {"id": "start", "type": "entry", "variables": {"amount": 120}, "next": "before"},
        {"id": "before", "type": "stage", "stage": "WaitCountStage",
         "arguments": {"const": {"tag": "before"}}, "next": "ask"},
        ask,
        {"id": "after", "type": "stage", "stage": "WaitCountStage",
         "arguments": {"const": {"tag": "after"}}, "next": "done"},
        {"id": "late", "type": "terminal", "result": {"status": "late"}},
        {"id": "done", "type": "terminal", "result": {"status": "done"},
         "artifacts": ["amount", "approved", "approver"]},
    ]})


class InMemoryTests(unittest.IsolatedAsyncioTestCase):
    """Without a store, a wait is an ordinary wait in memory."""

    def setUp(self):
        CALLS.clear()

    async def test_the_input_fills_variables_and_the_run_goes_on(self):
        session = Session(id="m1", pipeline=approval())
        task = asyncio.create_task(session.run())
        await asyncio.sleep(0.01)
        self.assertTrue(session.is_waiting_for("approval"))
        await session.input("approval", {"ok": True, "by": "ann"})
        result = await asyncio.wait_for(task, 1)
        self.assertEqual(result.result, {"status": "done"})
        self.assertEqual(result.artifacts, {"amount": 120, "approved": True, "approver": "ann"})
        types = [e.type for e in result.history]
        self.assertLess(types.index("waiting_started"), types.index("wait_completed"))

    async def test_an_input_sent_early_is_not_lost(self):
        session = Session(id="m2", pipeline=approval())
        await session.input("approval", {"ok": False, "by": "bo"})
        result = await asyncio.wait_for(session.run(), 1)
        self.assertEqual(result.artifacts["approver"], "bo")

    async def test_the_timeout_takes_the_other_road(self):
        result = await asyncio.wait_for(Session(id="m3", pipeline=approval(0.05)).run(), 1)
        self.assertEqual(result.result, {"status": "late"})
        self.assertIn("wait_timed_out", [e.type for e in result.history])

    async def test_waiting_is_idle_time_for_the_budget(self):
        session = Session(id="m4", pipeline=approval(0.2),
                          limits=Limits(counters={"seconds": 0.1}))
        result = await asyncio.wait_for(session.run(), 1)
        self.assertEqual(result.result, {"status": "late"})


class SuspendTests(unittest.IsolatedAsyncioTestCase):
    """With a store, a run with nobody to answer is saved and returns."""

    def setUp(self):
        CALLS.clear()
        self.store = MemoryStore()

    async def start(self, run_id="r", pipeline=None, **kwargs):
        session = Session(id=run_id, pipeline=pipeline or approval("7d"),
                          store=self.store, **kwargs)
        return session, await asyncio.wait_for(session.run(), 1)

    async def test_the_run_is_saved_and_continued_by_another_session(self):
        _, first = await self.start(limits=Limits(counters={"units": 10}))
        self.assertEqual(first.waiting["node"], "ask")
        self.assertEqual(first.waiting["input"], "approval")
        saved = await self.store.load("r")
        self.assertEqual(saved.state, "waiting")
        self.assertEqual(saved.waiting_for["input"], "approval")
        self.assertIsNotNone(saved.deadline)

        # a new process: nothing but the store and the run id
        later = await Session.restore(self.store, "r", policy=OPEN,
                                      limits=Limits(counters={"units": 10}))
        await later.input("approval", {"ok": True, "by": "ann"})
        result = await asyncio.wait_for(later.run(), 1)
        self.assertEqual(result.result, {"status": "done"})
        self.assertEqual(result.artifacts, {"amount": 120, "approved": True, "approver": "ann"})
        self.assertEqual(result.meters["units"], 2)  # spent before and after
        self.assertEqual(sorted(CALLS), ["after", "before"])
        self.assertEqual(len(CALLS["before"]), 1)  # not run again
        self.assertEqual((await self.store.load("r")).state, "finished")
        self.assertIsNone(result.waiting)

    async def test_an_answer_already_there_needs_no_suspension(self):
        session = Session(id="r", pipeline=approval("7d"), store=self.store)
        await session.input("approval", {"ok": True, "by": "x"})
        result = await session.run()
        self.assertEqual(result.result, {"status": "done"})
        self.assertEqual((await self.store.load("r")).state, "finished")

    async def test_restored_without_the_input_it_waits_again(self):
        await self.start()
        version = (await self.store.load("r")).version
        again = await (await Session.restore(self.store, "r", policy=OPEN)).run()
        self.assertEqual(again.waiting["node"], "ask")
        saved = await self.store.load("r")
        self.assertEqual(saved.state, "waiting")
        self.assertGreater(saved.version, version)
        self.assertEqual(len(CALLS["before"]), 1)

    async def test_the_deadline_survives_and_then_takes_the_other_road(self):
        with mock.patch("stageflow.core.session.time.time", return_value=1000.0):
            await self.start(pipeline=approval("2h"))
        self.assertEqual((await self.store.load("r")).deadline, 1000.0 + 7200)
        self.assertEqual(await self.store.due(now=1000.0 + 7199), [])
        self.assertEqual(await self.store.due(now=1000.0 + 7200), ["r"])

        with mock.patch("stageflow.core.session.time.time", return_value=1000.0 + 7201):
            result = await (await Session.restore(self.store, "r", policy=OPEN)).run()
        self.assertEqual(result.result, {"status": "late"})
        self.assertEqual(await self.store.due(now=10**10), [])

    async def test_a_different_input_is_kept_for_later(self):
        await self.start()
        session = await Session.restore(self.store, "r", policy=OPEN)
        await session.input("comment", {"text": "looking"})
        await session.run()
        saved = await self.store.load("r")
        self.assertEqual(saved.snapshot["pending_inputs"],
                         {"comment": [{"type": "comment", "payload": {"text": "looking"}}]})

    async def test_two_processes_cannot_both_continue_a_run(self):
        await self.start()
        one = await Session.restore(self.store, "r", policy=OPEN)
        two = await Session.restore(self.store, "r", policy=OPEN)
        for session in (one, two):
            await session.input("approval", {"ok": True, "by": "z"})
        await one.run()
        with self.assertRaises(CheckpointConflict):
            await two.run()
        self.assertEqual(len(CALLS["after"]), 1)  # the loser ran nothing

    async def test_a_run_id_is_taken_once(self):
        await self.start()
        with self.assertRaises(CheckpointConflict):
            await self.start()

    async def test_only_a_run_in_progress_can_be_restored(self):
        with self.assertRaisesRegex(CheckpointError, "not in the store"):
            await Session.restore(self.store, "nope", policy=OPEN)
        session = Session(id="r", pipeline=approval("7d"), store=self.store)
        await session.input("approval", {"ok": True, "by": "x"})
        await session.run()
        with self.assertRaisesRegex(CheckpointError, "already finished"):
            await Session.restore(self.store, "r", policy=OPEN)

    async def test_a_failure_is_recorded(self):
        pipeline = Pipeline.from_dict({"entry": "a", "nodes": [
            {"id": "a", "type": "stage", "stage": "WaitCountStage", "arguments": {}}]})
        with self.assertRaises(Exception):
            await Session(id="f", pipeline=pipeline, store=self.store).run()
        self.assertEqual((await self.store.load("f")).state, "failed")

    async def test_the_policy_is_the_hosts_not_the_checkpoints(self):
        await self.start()
        narrow = Policy(node_types={"entry", "stage", "terminal"})
        with self.assertRaises(PipelineValidationError):
            await Session.restore(self.store, "r", policy=narrow)

    async def test_observability_calls_it_waiting_not_failed(self):
        recorder = Recorder()
        await self.start(instrument=recorder)
        summary = recorder.summary()
        self.assertEqual(summary["sessions"]["session"]["errors"], 0)
        self.assertEqual(summary["nodes"]["wait"]["errors"], 0)
        self.assertEqual(summary["events"]["session_suspended"], 1)


class CrashTests(unittest.IsolatedAsyncioTestCase):
    """`checkpoint="every_node"`: a run outlives a process that died."""

    def setUp(self):
        CALLS.clear()
        self.enterContext(isolated_stages())

    async def test_a_crashed_run_goes_on_from_the_node_it_died_in(self):
        crashed = asyncio.Event()

        @register_stage("CrashOnceStage")
        class CrashOnce(BaseStage):
            timeout = None

            async def run(self):
                CALLS.setdefault("crash", []).append(self.idempotency_key)
                if not crashed.is_set():
                    crashed.set()
                    await asyncio.sleep(3600)  # the process dies here
                self.set_outputs({})

        pipeline = Pipeline.from_dict({"entry": "a", "nodes": [
            {"id": "a", "type": "stage", "stage": "WaitCountStage",
             "arguments": {"const": {"tag": "a"}}, "next": "b"},
            {"id": "b", "type": "stage", "stage": "CrashOnceStage", "next": "c"},
            {"id": "c", "type": "stage", "stage": "WaitCountStage",
             "arguments": {"const": {"tag": "c"}}, "next": "end"},
            {"id": "end", "type": "terminal", "result": {"status": "ok"}},
        ]})
        store = MemoryStore()
        task = asyncio.create_task(
            Session(id="c1", pipeline=pipeline, store=store, checkpoint="every_node").run())
        await asyncio.wait_for(crashed.wait(), 1)
        task.cancel()  # as good as a kill: nothing is saved on the way out
        await asyncio.gather(task, return_exceptions=True)
        self.assertEqual((await store.load("c1")).state, "running")

        with self.assertRaisesRegex(CheckpointConflict, "takeover"):
            await (await Session.restore(store, "c1", policy=OPEN)).run()

        session = await Session.restore(store, "c1", policy=OPEN, takeover=True,
                                        checkpoint="every_node")
        result = await asyncio.wait_for(session.run(), 1)
        self.assertEqual(result.result, {"status": "ok"})
        self.assertEqual(len(CALLS["a"]), 1)  # finished before the crash
        first, second = CALLS["crash"]
        self.assertEqual(first, second)  # the repeat can be recognised
        self.assertEqual(first, "c1:b:1")


class PlacementTests(unittest.TestCase):
    """A saved run resumes at a node of the main graph, so that is where a
    wait may be."""

    WAIT = {"id": "w", "type": "wait", "input": "x", "next": "end"}
    END = {"id": "end", "type": "terminal"}

    def errors(self, nodes, **extra):
        try:
            Pipeline.from_dict({"nodes": nodes, **extra}).validate()
        except PipelineValidationError as exc:
            return "; ".join(exc.errors)
        return ""

    def test_inside_a_block_is_refused(self):
        blocks = {
            "try": {"id": "b", "type": "try", "body": "w", "next": "end",
                    "except": [{"error_equals": ["*"], "next": "end"}]},
            "map": {"id": "b", "type": "map", "items": "[1]", "body": "w2", "next": "end"},
            "parallel": {"id": "b", "type": "parallel", "branches": ["w2"], "next": "end"},
        }
        for kind, block in blocks.items():
            with self.subTest(kind=kind):
                inner = dict(self.WAIT, id=block.get("body", "w2"), next=None) \
                    if kind != "try" else dict(self.WAIT, next=None)
                found = self.errors([{"id": "s", "type": "entry", "next": "b"},
                                     block, inner, self.END])
                self.assertIn(f"cannot be inside the {kind} node 'b'", found)

    def test_in_a_subpipeline_is_refused(self):
        found = self.errors(
            [{"id": "s", "type": "entry", "next": "sub"},
             {"id": "sub", "type": "subpipeline", "subpipeline_id": "inner", "next": "end"},
             self.END],
            subpipelines={"inner": {"entry": "w", "nodes": [dict(self.WAIT), dict(self.END)]}})
        self.assertIn("has a wait node", found)

    def test_after_a_block_is_fine(self):
        found = self.errors([
            {"id": "s", "type": "entry", "next": "b"},
            {"id": "b", "type": "try", "body": "t", "next": "w",
             "except": [{"error_equals": ["*"], "next": "end"}]},
            {"id": "t", "type": "terminal"},
            self.WAIT, self.END])
        self.assertEqual(found, "")

    def test_a_timeout_needs_somewhere_to_go(self):
        found = self.errors([{"id": "s", "type": "entry", "next": "w"},
                             dict(self.WAIT, timeout="1d"), self.END])
        self.assertIn("timeout and on_timeout go together", found)

    def test_a_timeout_is_a_duration(self):
        from stageflow import WaitNode, TerminalNode
        pipeline = Pipeline(entry="w", nodes=[
            WaitNode(id="w", input="x", timeout="soon", on_timeout="end", next="end"),
            TerminalNode(id="end")])
        with self.assertRaisesRegex(PipelineValidationError, "not a duration"):
            pipeline.validate()


class SQLiteStoreTests(unittest.IsolatedAsyncioTestCase):
    """The same promises from a file, which separate processes share."""

    def setUp(self):
        CALLS.clear()
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "runs.db"

    async def test_suspend_in_one_store_object_and_finish_in_another(self):
        first = await Session(id="s1", pipeline=approval("1d"),
                              store=SQLiteStore(self.path)).run()
        self.assertEqual(first.waiting["node"], "ask")

        other = SQLiteStore(self.path)  # as if opened by another process
        session = await Session.restore(other, "s1", policy=OPEN)
        await session.input("approval", {"ok": True, "by": "дана"})
        result = await session.run()
        self.assertEqual(result.artifacts["approver"], "дана")
        self.assertEqual((await other.load("s1")).state, "finished")

    async def test_versions_are_checked_by_the_database(self):
        store = SQLiteStore(self.path)
        await Session(id="s2", pipeline=approval("1d"), store=store).run()
        one = await Session.restore(store, "s2", policy=OPEN)
        two = await Session.restore(SQLiteStore(self.path), "s2", policy=OPEN)
        await one.run()
        with self.assertRaises(CheckpointConflict):
            await two.run()
        await store.delete("s2")
        self.assertIsNone(await store.load("s2"))
        self.assertEqual(await store.due(now=10**12), [])


if __name__ == "__main__":
    unittest.main()
