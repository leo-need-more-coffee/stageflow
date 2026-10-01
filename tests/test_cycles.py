import unittest

from stageflow import Pipeline
from stageflow.exceptions import PipelineValidationError


def _graph(*nodes):
    return {"entry": nodes[0]["id"], "nodes": list(nodes)}


class CycleTests(unittest.TestCase):
    """A road that comes back to where it has been.

    The language has a bounded loop of its own — the `map` node. A cycle in
    the order edges is not bounded by anything: it spins for as long as the
    process lives, at tens of thousands of nodes a second, and it is almost
    always a `next` pointed at the wrong id.
    """

    def test_a_node_pointing_at_itself(self):
        data = _graph({"id": "a", "type": "stage", "stage": "SetValueStage",
                       "arguments": {"const": {"value": 1}},
                       "outputs": {"value": "n"}, "next": "a"})
        self.assertIn("cycle in the graph: a -> a",
                      Pipeline.from_dict(data).collect_errors())

    def test_a_cycle_through_a_condition(self):
        data = _graph(
            {"id": "a", "type": "stage", "stage": "IncrementStage",
             "arguments": {"vars": {"value": "n"}}, "outputs": {"value": "n"},
             "next": "check"},
            {"id": "check", "type": "condition", "condition": "vars.n < 3",
             "then": "a", "else": "done"},
            {"id": "done", "type": "terminal", "result": {}},
        )
        self.assertIn("cycle in the graph: a -> check -> a",
                      Pipeline.from_dict(data).collect_errors())

    def test_the_cycle_is_reported_once_however_many_ways_in(self):
        data = _graph(
            {"id": "a", "type": "condition", "condition": "true",
             "then": "b", "else": "c"},
            {"id": "b", "type": "stage", "stage": "SetValueStage",
             "arguments": {"const": {"value": 1}}, "outputs": {"value": "n"},
             "next": "loop"},
            {"id": "c", "type": "stage", "stage": "SetValueStage",
             "arguments": {"const": {"value": 2}}, "outputs": {"value": "n"},
             "next": "loop"},
            {"id": "loop", "type": "stage", "stage": "IncrementStage",
             "arguments": {"vars": {"value": "n"}}, "outputs": {"value": "n"},
             "next": "loop"},
        )
        cycles = [e for e in Pipeline.from_dict(data).collect_errors() if "cycle" in e]
        self.assertEqual(cycles, ["cycle in the graph: loop -> loop"])

    def test_validate_refuses_a_cyclic_graph(self):
        data = _graph({"id": "a", "type": "stage", "stage": "SetValueStage",
                       "arguments": {"const": {"value": 1}},
                       "outputs": {"value": "n"}, "next": "a"})
        with self.assertRaises(PipelineValidationError):
            Pipeline.from_dict(data).validate()


class NotCyclesTests(unittest.TestCase):
    """The shapes that look like a cycle to a careless walk and are not."""

    def test_a_diamond_is_not_a_cycle(self):
        data = _graph(
            {"id": "a", "type": "condition", "condition": "true",
             "then": "l", "else": "r"},
            {"id": "l", "type": "stage", "stage": "SetValueStage",
             "arguments": {"const": {"value": 1}}, "outputs": {"value": "n"},
             "next": "join"},
            {"id": "r", "type": "stage", "stage": "SetValueStage",
             "arguments": {"const": {"value": 2}}, "outputs": {"value": "n"},
             "next": "join"},
            {"id": "join", "type": "terminal", "result": {}},
        )
        self.assertEqual(Pipeline.from_dict(data).collect_errors(), [])

    def test_a_map_body_is_a_loop_and_still_not_a_cycle(self):
        data = _graph(
            {"id": "loop", "type": "map", "items": "vars.xs", "body": "work",
             "item_var": "x", "next": "done"},
            {"id": "work", "type": "stage", "stage": "SetValueStage",
             "arguments": {"const": {"value": 1}}, "outputs": {"value": "n"}},
            {"id": "done", "type": "terminal", "result": {}},
        )
        self.assertEqual(Pipeline.from_dict(data).collect_errors(), [])

    def test_a_try_block_with_a_handler_rejoining_is_not_a_cycle(self):
        data = _graph(
            {"id": "guard", "type": "try", "body": "work", "next": "done",
             "except": [{"error_equals": ["*"], "next": "rescue"}]},
            {"id": "work", "type": "stage", "stage": "SetValueStage",
             "arguments": {"const": {"value": 1}}, "outputs": {"value": "n"}},
            {"id": "rescue", "type": "stage", "stage": "SetValueStage",
             "arguments": {"const": {"value": 2}}, "outputs": {"value": "n"}},
            {"id": "done", "type": "terminal", "result": {}},
        )
        self.assertEqual(Pipeline.from_dict(data).collect_errors(), [])


if __name__ == "__main__":
    unittest.main()


class PolicyTests(unittest.TestCase):
    """A road that comes back, where the host said it may.

    The rule is off by default and stays off when there is no policy at all:
    the thing that makes a cycle safe is something outside the graph that will
    stop it, and absence of a policy is absence of that too.
    """

    LOOP = _graph(
        {"id": "start", "type": "entry", "variables": {"n": 0}, "next": "bump"},
        {"id": "bump", "type": "stage", "stage": "IncrementStage",
         "arguments": {"vars": {"current": "n"}}, "outputs": {"value": "n"},
         "next": "enough"},
        {"id": "enough", "type": "condition", "condition": "vars.n >= 3",
         "then": "done", "else": "bump"},
        {"id": "done", "type": "terminal"},
    )

    def test_a_loop_is_refused_by_default(self):
        from stageflow import Policy

        errors = Pipeline.from_dict(self.LOOP).collect_errors(Policy())
        self.assertTrue(any("cycle in the graph" in e for e in errors), errors)

    def test_and_with_no_policy_at_all(self):
        errors = Pipeline.from_dict(self.LOOP).collect_errors()
        self.assertTrue(any("cycle in the graph" in e for e in errors), errors)

    def test_a_policy_that_allows_them_accepts_this_one(self):
        from stageflow import Limits, Policy

        allowed = Policy(allow_cycles=True, limits=Limits(counters={"steps": 100}))
        self.assertEqual(Pipeline.from_dict(self.LOOP).collect_errors(allowed), [])

    def test_nothing_else_about_the_graph_stops_being_checked(self):
        """Allowing a loop is not switching validation off."""
        from stageflow import Policy

        broken = _graph(
            {"id": "a", "type": "entry", "next": "b"},
            {"id": "b", "type": "condition", "condition": "true",
             "then": "a", "else": "nowhere"},
        )
        errors = Pipeline.from_dict(broken).collect_errors(Policy(allow_cycles=True))
        self.assertTrue(any("nowhere" in e for e in errors), errors)

    def test_a_node_pointing_at_itself_is_a_loop_like_any_other(self):
        """Nothing runs between two visits, so nothing can change the decision —
        but that is the author's problem to see, not a rule of the language. A
        policy that permits loops permits this one."""
        from stageflow import Policy

        data = _graph({"id": "a", "type": "stage", "stage": "SetValueStage",
                       "arguments": {"const": {"value": 1}},
                       "outputs": {"value": "n"}, "next": "a"})
        self.assertEqual(
            Pipeline.from_dict(data).collect_errors(Policy(allow_cycles=True)), [])

    def test_validate_lets_it_through(self):
        from stageflow import Limits, Policy

        Pipeline.from_dict(self.LOOP).validate(
            Policy(allow_cycles=True, limits=Limits(counters={"steps": 50})))


class CapabilitiesTests(unittest.TestCase):
    def test_a_client_is_told_whether_it_may_write_one(self):
        """Finding out after writing a graph is the worst moment to learn a
        rule, so the answer travels with the node types."""
        from stageflow import Policy, capabilities

        self.assertFalse(capabilities()["cycles"])
        self.assertFalse(capabilities(Policy())["cycles"])
        self.assertTrue(capabilities(Policy(allow_cycles=True))["cycles"])


class RunningTests(unittest.IsolatedAsyncioTestCase):
    """A permitted loop has to actually work, and a runaway one has to stop.

    Validation accepting a graph says nothing about what the runtime does with
    it, and the whole argument for the setting is that something outside the
    graph will end a loop the graph does not.
    """

    @staticmethod
    async def _run(graph, policy):
        from stageflow import Context, Session

        pipeline = Pipeline.from_dict(graph)
        pipeline.validate(policy)
        session = Session(id="loop", pipeline=pipeline,
                          context=Context(vars={}), policy=policy)
        return await session.run(), session

    async def test_a_counter_loop_runs_and_leaves(self):
        from stageflow import Limits, Policy

        policy = Policy(allow_cycles=True, limits=Limits(counters={"steps": 50}))
        _, session = await self._run(PolicyTests.LOOP, policy)
        self.assertEqual(session.context.get_var("n"), 3)

    async def test_a_loop_with_no_way_out_is_ended_by_the_budget(self):
        """And ended as a RESULT rather than as a hang: `BudgetExceeded` is
        caught in one place and comes back as the answer."""
        from stageflow import Limits, Policy

        spin = _graph(
            {"id": "start", "type": "entry", "variables": {"n": 0}, "next": "a"},
            {"id": "a", "type": "stage", "stage": "IncrementStage",
             "arguments": {"vars": {"current": "n"}}, "outputs": {"value": "n"},
             "next": "a"},
        )
        policy = Policy(allow_cycles=True, limits=Limits(counters={"steps": 20}))
        result, session = await self._run(spin, policy)
        self.assertEqual(result.result["status"], "budget_exceeded")
        self.assertEqual(result.result["meter"], "steps")
        self.assertGreater(session.context.get_var("n"), 0)
