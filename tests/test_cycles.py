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
