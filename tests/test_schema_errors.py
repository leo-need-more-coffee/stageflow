"""What a shape complaint says, and why it says it that way.

A graph is checked twice: by the JSON Schema for its shape, and by the
cross-graph rules for everything the shape cannot express. The second half has
always answered with the whole list at once, on purpose — "so that the author
fixes them in one pass". The first half used to answer with the first problem
it met and no indication of where it was, which for a graph of any size is a
true sentence nobody can act on.
"""
import unittest

from stageflow import Pipeline
from stageflow.exceptions import PipelineDefinitionError


def refusal(graph: dict) -> str:
    try:
        Pipeline.from_dict(graph)
    except PipelineDefinitionError as exc:
        return str(exc)
    raise AssertionError("the graph was accepted")


class WhereTests(unittest.TestCase):
    def test_the_node_is_named_by_its_id(self):
        """An index is something the author has to count; an id is what they
        typed and what the editor shows on the card."""
        said = refusal({"nodes": [
            {"id": "start", "type": "entry", "next": "guard"},
            {"id": "guard", "type": "try", "body": "risky",
             "except": [{"error_equals": ["*"], "then": "recover"}], "next": "done"},
            {"id": "risky", "type": "stage", "stage": "FailStage"},
            {"id": "done", "type": "terminal"},
        ]})
        self.assertIn("guard.except[0]", said)
        self.assertIn("'next' is a required property", said)

    def test_a_path_inside_a_node_is_spelled_out(self):
        said = refusal({"nodes": [
            {"id": "a", "type": "entry"},
            {"id": "b", "type": "stage", "stage": "FailStage", "arguments": {"oops": 1}},
        ]})
        self.assertIn("b.arguments", said)

    def test_a_node_without_an_id_falls_back_to_its_position(self):
        """Nothing to name it by, so say where to count to."""
        said = refusal({"nodes": [{"type": "entry"}]})
        self.assertIn("nodes[0]", said)

    def test_a_problem_outside_the_nodes_keeps_its_own_path(self):
        said = refusal({"nodes": [{"id": "a", "type": "entry"}], "types": "not an object"})
        self.assertIn("types", said)


class AllAtOnceTests(unittest.TestCase):
    def test_every_shape_problem_is_listed(self):
        """A shape is usually wrong the same way in several places, and fixing
        them one round trip at a time is a conversation rather than a
        correction."""
        said = refusal({"nodes": [
            {"id": "a", "type": "entry", "nonsense": 1},
            {"id": "b", "type": "terminal", "nonsense": 2},
            {"id": "c", "type": "terminal", "nonsense": 3},
        ]})
        for node in ("a", "b", "c"):
            self.assertIn(f"{node}: ", said)

    def test_a_wall_of_them_is_cut_off_and_says_so(self):
        graph = {"nodes": [{"id": f"n{i}", "type": "terminal", "nonsense": i}
                           for i in range(25)]}
        said = refusal(graph)
        self.assertIn("more", said)
        self.assertLess(said.count("; "), 15, "a list this long stops being a list")

    def test_a_subpipeline_says_which_one(self):
        said = refusal({
            "nodes": [{"id": "a", "type": "entry"}],
            "subpipelines": {"inner": {"nodes": [{"id": "x", "type": "try"}]}},
        })
        self.assertIn("inner", said)
        self.assertIn("x", said)


class StillAcceptsGoodGraphsTests(unittest.TestCase):
    def test_a_graph_that_is_shaped_right_passes(self):
        pipeline = Pipeline.from_dict({"nodes": [
            {"id": "start", "type": "entry", "variables": {"n": 1}, "next": "guard"},
            {"id": "guard", "type": "try", "body": "risky",
             "except": [{"error_equals": ["*"], "next": "done", "result_var": "err"}],
             "next": "done"},
            {"id": "risky", "type": "stage", "stage": "FailStage"},
            {"id": "done", "type": "terminal"},
        ]})
        self.assertEqual(pipeline.entry, "start")


if __name__ == "__main__":
    unittest.main()
