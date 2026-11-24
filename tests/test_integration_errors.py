"""Integration: a failure travelling through blocks of the graph.

`retry` on a node, `try` around a region, `map` and `parallel` with and without
`cancel_on_error`, subpipelines — each is tested on its own elsewhere. This is
about the combinations: which of them sees an error first, what reaches a
handler, and what nothing in a graph is allowed to catch.
"""
import asyncio
import unittest

from stageflow import (
    BaseStage,
    Context,
    Limits,
    Pipeline,
    Policy,
    Session,
    register_stage,
)
from stageflow.exceptions import PipelineValidationError


class Flaky(Exception):
    pass


class Broken(Exception):
    pass


@register_stage("ErrFlaky")
class ErrFlaky(BaseStage):
    """
    description: "Fails the first `fail_times` calls per key, then succeeds"
    arguments:
      key: string
      fail_times: integer
    outputs:
      attempts: integer
    """

    calls: dict = {}

    async def run(self):
        args = self.get_arguments()
        n = ErrFlaky.calls[args["key"]] = ErrFlaky.calls.get(args["key"], 0) + 1
        if n <= args["fail_times"]:
            raise Flaky(f"attempt {n}")
        self.set_outputs({"attempts": n})


@register_stage("ErrBroken")
class ErrBroken(BaseStage):
    """
    description: "Fails when `value` is negative, echoes it otherwise"
    arguments:
      value: integer
    outputs:
      value: integer
    """

    async def run(self):
        value = self.get_arguments()["value"]
        if value < 0:
            raise Broken(f"negative: {value}")
        self.set_outputs({"value": value})


@register_stage("ErrStep")
class ErrStep(BaseStage):
    """
    description: "Does nothing, costs a step"
    outputs:
      ok: boolean
    """

    async def run(self):
        self.set_outputs({"ok": True})


def go(data, policy=None, **vars):
    return asyncio.run(Session(id="err", pipeline=Pipeline.from_dict(data),
                               context=Context(vars=vars), policy=policy).run())


def no_retry_delay(node):
    for retrier in node.get("retry", []):
        retrier.setdefault("interval_seconds", 0)
    return node


class RetryInsideTryTests(unittest.TestCase):
    def setUp(self):
        ErrFlaky.calls = {}

    def _pipeline(self, fail_times):
        return {"nodes": [
            {"id": "start", "type": "entry", "next": "guard"},
            {"id": "guard", "type": "try", "body": "call",
             "except": [{"error_equals": ["Flaky"], "next": "fallback",
                         "result_var": "error"}],
             "next": "end"},
            no_retry_delay({"id": "call", "type": "stage", "stage": "ErrFlaky",
                            "arguments": {"const": {"key": "k", "fail_times": fail_times}},
                            "outputs": {"attempts": "attempts"},
                            "retry": [{"error_equals": ["Flaky"], "max_attempts": 3}]}),
            {"id": "fallback", "type": "stage", "stage": "ErrStep",
             "outputs": {"ok": "fell_back"}},
            {"id": "end", "type": "terminal", "artifacts": ["attempts", "fell_back", "error"]},
        ]}

    def test_retry_gets_through_before_try_sees_anything(self):
        result = go(self._pipeline(fail_times=2))
        self.assertEqual(result.artifacts["attempts"], 3)
        self.assertIsNone(result.artifacts["fell_back"])
        self.assertNotIn("try_caught", [e.type for e in result.history])

    def test_when_retry_gives_up_try_catches_and_names_the_node(self):
        result = go(self._pipeline(fail_times=5))
        self.assertEqual(ErrFlaky.calls["k"], 3, "three attempts, then the handler")
        self.assertTrue(result.artifacts["fell_back"])
        error = result.artifacts["error"]
        self.assertEqual((error["type"], error["node"]), ("Flaky", "call"))
        self.assertEqual(error["message"], "attempt 3")


class FailuresInLoopsTests(unittest.TestCase):
    def _map(self, *, guarded, cancel_on_error, mode="sequential"):
        body = "guard" if guarded else "check"
        nodes = [
            {"id": "start", "type": "entry", "next": "each"},
            {"id": "each", "type": "map", "items": "vars.values", "body": body,
             "item_var": "v", "collect": {"out": "results"}, "mode": mode,
             "cancel_on_error": cancel_on_error, "next": "end"},
            {"id": "check", "type": "stage", "stage": "ErrBroken",
             "arguments": {"vars": {"value": "v"}}, "outputs": {"value": "out"}},
            {"id": "end", "type": "terminal", "artifacts": ["results"]},
        ]
        if guarded:
            nodes += [
                {"id": "guard", "type": "try", "body": "check",
                 "except": [{"error_equals": ["Broken"], "next": "zero"}]},
                # the handler writes `out` too, so every item has something
                # to collect
                {"id": "zero", "type": "stage", "stage": "ErrStep",
                 "outputs": {"out.$": "0"}},
            ]
        return {"nodes": nodes}

    def test_a_try_inside_the_body_catches_per_item(self):
        for mode in ("sequential", "parallel"):
            with self.subTest(mode=mode):
                result = go(self._map(guarded=True, cancel_on_error=True, mode=mode),
                            values=[1, -2, 3, -4])
                self.assertEqual(result.artifacts["results"], [1, 0, 3, 0])

    def test_without_a_try_the_first_failure_stops_the_loop(self):
        with self.assertRaises(Broken):
            go(self._map(guarded=False, cancel_on_error=True), values=[1, -2, 3])

    def test_cancel_on_error_false_runs_every_item_then_raises(self):
        events = []
        data = self._map(guarded=False, cancel_on_error=False)
        session = Session(id="all", pipeline=Pipeline.from_dict(data),
                          context=Context(vars={"values": [-1, 2, -3]}),
                          event_handler=events.append)
        with self.assertRaisesRegex(Broken, "negative: -1"):
            asyncio.run(session.run())
        failed = [e.payload["index"] for e in events if e.type == "map_item_failed"]
        done = [e.payload["index"] for e in events if e.type == "map_item_completed"]
        self.assertEqual((failed, done), ([0, 2], [1]))


class FailuresInBranchesTests(unittest.TestCase):
    def test_a_branch_failure_reaches_a_try_as_branch_error(self):
        data = {"nodes": [
            {"id": "start", "type": "entry", "next": "guard"},
            {"id": "guard", "type": "try", "body": "both",
             "except": [{"error_equals": ["BranchError"], "next": "handled",
                         "result_var": "error"}],
             "next": "end"},
            {"id": "both", "type": "parallel", "branches": ["good", "bad"],
             "cancel_on_error": False},
            {"id": "good", "type": "stage", "stage": "ErrBroken",
             "arguments": {"const": {"value": 1}}, "outputs": {"value": "good"}},
            {"id": "bad", "type": "stage", "stage": "ErrBroken",
             "arguments": {"const": {"value": -1}}, "outputs": {"value": "bad"}},
            {"id": "handled", "type": "stage", "stage": "ErrStep",
             "outputs": {"ok": "handled"}},
            {"id": "end", "type": "terminal", "artifacts": ["handled", "error"]},
        ]}
        result = go(data)
        self.assertTrue(result.artifacts["handled"])
        self.assertEqual(result.artifacts["error"]["type"], "BranchError")
        self.assertIn("branch 'bad' failed", result.artifacts["error"]["message"])

    def test_two_branches_writing_one_name_is_an_error(self):
        data = {"nodes": [
            {"id": "start", "type": "entry", "next": "both"},
            {"id": "both", "type": "parallel", "branches": ["a", "b"], "next": "end"},
            {"id": "a", "type": "stage", "stage": "ErrStep", "outputs": {"ok": "x"}},
            {"id": "b", "type": "stage", "stage": "ErrStep", "outputs": {"ok": "x"}},
            {"id": "end", "type": "terminal"},
        ]}
        from stageflow.exceptions import BranchError
        with self.assertRaisesRegex(BranchError, "both write x"):
            go(data)


class BudgetThroughBlocksTests(unittest.TestCase):
    def test_no_try_catches_a_budget_even_deep_inside(self):
        data = {"nodes": [
            {"id": "start", "type": "entry", "next": "guard"},
            {"id": "guard", "type": "try", "body": "each",
             "except": [{"error_equals": ["*"], "next": "swallowed"}], "next": "end"},
            {"id": "each", "type": "map", "items": "vars.xs", "body": "both",
             "mode": "parallel"},
            {"id": "both", "type": "parallel", "branches": ["s1", "s2"]},
            {"id": "s1", "type": "stage", "stage": "ErrStep"},
            {"id": "s2", "type": "stage", "stage": "ErrStep"},
            {"id": "swallowed", "type": "terminal", "result": {"status": "swallowed"}},
            {"id": "end", "type": "terminal", "result": {"status": "finished"}},
        ]}
        policy = Policy(limits=Limits(counters={"steps": 12}))
        result = go(data, policy=policy, xs=list(range(10)))
        self.assertEqual(result.result["status"], "budget_exceeded")
        self.assertEqual(result.result["meter"], "steps")

    def test_a_subpipeline_spends_from_the_parent_budget(self):
        child = {"nodes": [
            {"id": "c_start", "type": "entry", "next": "c1"},
            {"id": "c1", "type": "stage", "stage": "ErrStep", "next": "c2"},
            {"id": "c2", "type": "stage", "stage": "ErrStep", "next": "c_end"},
            {"id": "c_end", "type": "terminal"},
        ]}
        data = {"nodes": [
            {"id": "start", "type": "entry", "next": "sub"},
            {"id": "sub", "type": "subpipeline", "subpipeline_id": "child", "next": "end"},
            {"id": "end", "type": "terminal", "result": {"status": "finished"}},
        ], "subpipelines": {"child": child}}
        result = go(data)
        # parent: start, sub, end; child: c_start, c1, c2, c_end
        self.assertEqual(result.meters["steps"], 7)
        self.assertEqual(result.meters["peak_depth"], 1)

        tight = Policy(limits=Limits(counters={"steps": 5}))
        self.assertEqual(go(data, policy=tight).result["status"], "budget_exceeded")

    def test_a_policy_reaches_into_subpipelines_before_anything_runs(self):
        data = {"nodes": [
            {"id": "start", "type": "entry", "next": "sub"},
            {"id": "sub", "type": "subpipeline", "subpipeline_id": "child", "next": "end"},
            {"id": "end", "type": "terminal"},
        ], "subpipelines": {"child": {"nodes": [
            {"id": "c_start", "type": "entry", "next": "c1"},
            {"id": "c1", "type": "stage", "stage": "ErrBroken",
             "arguments": {"const": {"value": 1}}},
        ]}}}
        narrow = Policy(stages={"ErrStep"})
        with self.assertRaisesRegex(PipelineValidationError, r"\[child\] c1.*ErrBroken"):
            go(data, policy=narrow)


if __name__ == "__main__":
    unittest.main()
