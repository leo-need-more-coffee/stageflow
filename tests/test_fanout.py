"""Nested fan-out under a concurrency limit, and a terminal inside a branch."""
import asyncio
import unittest

from stageflow import BaseStage, Limits, Pipeline, Session, register_stage


@register_stage("FanoutTick")
class FanoutTick(BaseStage):
    """
    description: "Takes a moment and says it ran"
    arguments:
      tag: any
    outputs:
      seen: any
    """

    live = 0
    peak = 0

    async def run(self):
        cls = type(self)
        cls.live += 1
        cls.peak = max(cls.peak, cls.live)
        try:
            await asyncio.sleep(0.01)
        finally:
            cls.live -= 1
        self.set_outputs({"seen": self.get_arguments().get("tag")})


@register_stage("FanoutSlow")
class FanoutSlow(BaseStage):
    """
    description: "Never finishes on its own"
    """

    timeout = None
    cancelled = False

    async def run(self):
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            type(self).cancelled = True
            raise


def _run(data, **kwargs):
    async def go():
        return await asyncio.wait_for(
            Session(id="s", pipeline=Pipeline.from_dict(data), **kwargs).run(), 5
        )
    return asyncio.run(go())


def _map_in_branches(branches: int, mode: str) -> dict:
    nodes = [{"id": "start", "type": "entry", "variables": {"xs": [1, 2, 3]},
              "next": "par"},
             {"id": "par", "type": "parallel",
              "branches": [f"m{i}" for i in range(branches)], "next": "end"}]
    for i in range(branches):
        nodes += [
            {"id": f"m{i}", "type": "map", "items": "vars.xs", "body": f"b{i}",
             "mode": mode, "collect": {"seen": f"out{i}"}},
            {"id": f"b{i}", "type": "stage", "stage": "FanoutTick",
             "arguments": {"vars": {"tag": "item"}}, "outputs": {"seen": "seen"}},
        ]
    nodes.append({"id": "end", "type": "terminal",
                  "artifacts": [f"out{i}" for i in range(branches)]})
    return {"nodes": nodes}


class NestedConcurrencyTests(unittest.TestCase):
    def setUp(self):
        FanoutTick.live = FanoutTick.peak = 0

    def test_a_map_inside_a_branch_does_not_deadlock_at_one(self):
        for mode in ("sequential", "parallel"):
            with self.subTest(mode=mode):
                FanoutTick.peak = 0
                result = _run(_map_in_branches(1, mode),
                              limits=Limits(gauges={"concurrency": 1}))
                self.assertEqual(result.artifacts, {"out0": [1, 2, 3]})
                self.assertEqual(FanoutTick.peak, 1)

    def test_nesting_wider_than_the_allowance_still_finishes(self):
        result = _run(_map_in_branches(4, "parallel"),
                      limits=Limits(gauges={"concurrency": 2}))
        self.assertEqual(result.artifacts,
                         {f"out{i}": [1, 2, 3] for i in range(4)})
        # what the limit bounds is the work in the air, nested or not
        self.assertLessEqual(FanoutTick.peak, 2)
        self.assertLessEqual(result.meters["peak_concurrency"], 2)

    def test_waiting_for_a_unit_is_bounded_by_the_deadline(self):
        data = {"nodes": [
            {"id": "start", "type": "entry", "next": "par"},
            {"id": "par", "type": "parallel", "branches": ["a", "b"], "next": "end"},
            {"id": "a", "type": "stage", "stage": "FanoutSlow"},
            {"id": "b", "type": "stage", "stage": "FanoutSlow"},
            {"id": "end", "type": "terminal"},
        ]}
        result = _run(data, limits=Limits(counters={"seconds": 0.2},
                                          gauges={"concurrency": 1}))
        self.assertEqual(result.result["status"], "budget_exceeded")
        self.assertEqual(result.result["meter"], "seconds")


class TerminalInBranchTests(unittest.TestCase):
    def test_a_terminal_in_a_branch_ends_the_run(self):
        FanoutSlow.cancelled = False
        data = {"nodes": [
            {"id": "start", "type": "entry", "next": "par"},
            {"id": "par", "type": "parallel", "branches": ["first", "slow"],
             "next": "after"},
            # a moment first, so that the slow branch is already running
            {"id": "first", "type": "stage", "stage": "FanoutTick", "next": "t"},
            {"id": "t", "type": "terminal", "result": {"status": "from_branch"}},
            {"id": "slow", "type": "stage", "stage": "FanoutSlow"},
            {"id": "after", "type": "stage", "stage": "FanoutTick",
             "next": "end"},
            {"id": "end", "type": "terminal", "result": {"status": "main"}},
        ]}
        result = _run(data)
        self.assertEqual(result.result, {"status": "from_branch"})
        self.assertTrue(FanoutSlow.cancelled)
        types = [event.type for event in result.history]
        self.assertIn("parallel_cancelled", types)
        self.assertNotIn("parallel_completed", types)

    def test_a_terminal_in_a_parallel_map_item_ends_the_run(self):
        data = {"nodes": [
            {"id": "start", "type": "entry", "variables": {"xs": [0, 1]},
             "next": "m"},
            {"id": "m", "type": "map", "items": "vars.xs", "body": "pick",
             "mode": "parallel", "next": "end"},
            {"id": "pick", "type": "condition", "condition": "vars.item == 0",
             "then": "stop", "else": "slow"},
            {"id": "stop", "type": "terminal", "result": {"status": "early"}},
            {"id": "slow", "type": "stage", "stage": "FanoutSlow"},
            {"id": "end", "type": "terminal", "result": {"status": "main"}},
        ]}
        self.assertEqual(_run(data).result, {"status": "early"})


if __name__ == "__main__":
    unittest.main()
