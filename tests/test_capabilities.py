import unittest
from importlib.metadata import version

import stageflow
from stageflow import capabilities
from stageflow.core.nodes import get_node_types


class CapabilitiesTests(unittest.TestCase):
    def test_version_is_the_installed_one(self):
        self.assertEqual(stageflow.__version__, version("stageflow-framework"))

    def test_node_types_are_the_registry(self):
        """The point of the field: it is the registry, not a written-down list.

        A client decides whether it may offer a node by this answer, so a type
        registered after this test was written has to appear here on its own.
        """
        self.assertEqual(capabilities()["node_types"], sorted(get_node_types()))

    def test_node_types_are_sorted_and_unique(self):
        types = capabilities()["node_types"]
        self.assertEqual(types, sorted(set(types)))

    def test_a_registered_type_shows_up(self):
        for expected in ("entry", "stage", "map", "try", "terminal"):
            self.assertIn(expected, capabilities()["node_types"])

    def test_stage_count_matches_the_registry(self):
        from stageflow.core.stage import get_stages

        self.assertEqual(capabilities()["stages"], len(get_stages()))

    def test_the_answer_is_json_shaped(self):
        """It is served over HTTP, so it has to survive a dump."""
        import json

        self.assertEqual(json.loads(json.dumps(capabilities())), capabilities())


if __name__ == "__main__":
    unittest.main()
