"""Integration: the step debugger driving a run that branches.

The debugger is what the editor's debug panel talks to. Here it steps a real
session node by node, and a variable written between two steps changes which
road the run takes — the thing a person in the debugger actually does.
"""
import asyncio
import unittest

from stageflow import BaseStage, Context, Pipeline, Session, StepDebugger, register_stage


@register_stage("DbgEcho")
class DbgEcho(BaseStage):
    """
    description: "Returns its argument"
    arguments:
      value: any
    outputs:
      value: any
    """

    async def run(self):
        self.set_outputs({"value": self.get_arguments()["value"]})


PIPELINE = {"nodes": [
    {"id": "start", "type": "entry", "variables": {"n": 1}, "next": "pick"},
    {"id": "pick", "type": "switch",
     "cases": [{"when": "vars.n == 1", "next": "one"},
               {"when": "vars.n == 2", "next": "two"}],
     "default": "many"},
    {"id": "one", "type": "stage", "stage": "DbgEcho",
     "arguments": {"const": {"value": "one"}}, "outputs": {"value": "said"}, "next": "end"},
    {"id": "two", "type": "stage", "stage": "DbgEcho",
     "arguments": {"const": {"value": "two"}}, "outputs": {"value": "said"}, "next": "end"},
    {"id": "many", "type": "stage", "stage": "DbgEcho",
     "arguments": {"const": {"value": "many"}}, "outputs": {"value": "said"}, "next": "end"},
    {"id": "end", "type": "terminal", "artifacts": ["said"]},
]}


class SteppingTests(unittest.IsolatedAsyncioTestCase):
    async def _until_paused_at(self, debugger, node_id):
        for _ in range(200):
            if debugger.waiting and debugger.node == node_id:
                return
            await asyncio.sleep(0.005)
        self.fail(f"never paused at {node_id}; at {debugger.node}")

    async def test_stepping_visits_the_nodes_in_order(self):
        events = []
        debugger = StepDebugger(mode="step", on_event=events.append)
        session = Session(id="dbg", pipeline=Pipeline.from_dict(PIPELINE),
                          debugger=debugger)
        task = asyncio.create_task(session.run())
        for node_id in ("start", "pick", "one", "end"):
            await self._until_paused_at(debugger, node_id)
            debugger.step()
        result = await asyncio.wait_for(task, 2)
        self.assertEqual(result.artifacts, {"said": "one"})
        entered = [e["node"] for e in events if e["type"] == "node_enter"]
        self.assertEqual(entered, ["start", "pick", "one", "end"])

    async def test_a_variable_set_while_paused_changes_the_road(self):
        events = []
        debugger = StepDebugger(mode="step", on_event=events.append)
        session = Session(id="dbg", pipeline=Pipeline.from_dict(PIPELINE),
                          debugger=debugger)
        task = asyncio.create_task(session.run())
        await self._until_paused_at(debugger, "start")
        debugger.step()
        await self._until_paused_at(debugger, "pick")
        debugger.set_vars({"n": 2})
        debugger.resume()
        result = await asyncio.wait_for(task, 2)
        self.assertEqual(result.artifacts, {"said": "two"})
        self.assertIn({"type": "var_set", "name": "n", "value": 2},
                      [{k: e[k] for k in ("type", "name", "value") if k in e}
                       for e in events])

    async def test_a_value_of_the_wrong_type_is_refused_and_the_run_goes_on(self):
        typed = {**PIPELINE, "variables": {"n": "int"}}
        events = []
        debugger = StepDebugger(mode="step", on_event=events.append)
        session = Session(id="dbg", pipeline=Pipeline.from_dict(typed),
                          debugger=debugger, context=Context(vars={"n": 1}))
        task = asyncio.create_task(session.run())
        await self._until_paused_at(debugger, "start")
        debugger.set_vars({"n": "two"})
        debugger.resume()
        result = await asyncio.wait_for(task, 2)
        self.assertEqual(result.artifacts, {"said": "one"}, "the old value stood")
        self.assertIn("var_rejected", [e["type"] for e in events])

    async def test_stop_while_paused_ends_the_run_with_a_result(self):
        debugger = StepDebugger(mode="step")
        session = Session(id="dbg", pipeline=Pipeline.from_dict(PIPELINE),
                          debugger=debugger)
        task = asyncio.create_task(session.run())
        await self._until_paused_at(debugger, "start")
        session.stop()
        debugger.resume()
        result = await asyncio.wait_for(task, 2)
        self.assertEqual(result.result, {"result": "stopped"})
        self.assertEqual(result.artifacts, {})


SLEEPY = {"nodes": [
    {"id": "start", "type": "entry", "next": "nap"},
    {"id": "nap", "type": "stage", "stage": "SleepStage",
     "arguments": {"const": {"seconds": 5}}, "next": "end"},
    {"id": "end", "type": "terminal", "result": {"status": "slept"}},
]}


class StoppingTests(unittest.TestCase):
    def test_stop_from_another_thread_interrupts_the_running_node(self):
        import threading
        import time
        session = Session(id="x", pipeline=Pipeline.from_dict(SLEEPY))
        out = {}
        worker = threading.Thread(target=lambda: out.update(r=asyncio.run(session.run())))
        started = time.monotonic()
        worker.start()
        time.sleep(0.1)
        session.stop()
        worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertEqual(out["r"].result, {"result": "stopped"})
        self.assertLess(time.monotonic() - started, 2)

    def test_cancelling_run_is_a_cancellation_not_a_stop(self):
        async def go():
            session = Session(id="x", pipeline=Pipeline.from_dict(SLEEPY))
            task = asyncio.create_task(session.run())
            await asyncio.sleep(0.05)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertFalse(session.stopped)
        asyncio.run(go())


if __name__ == "__main__":
    unittest.main()
