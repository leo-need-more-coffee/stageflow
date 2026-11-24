"""The pipeline format version and the upgrade path between versions.

There is one real version today, so the upgrade machinery is exercised
with a made-up version 2 that renames `metadata` to `about`.
"""
import copy
import unittest
from unittest import mock

from stageflow import Pipeline, capabilities
from stageflow.core import migrations
from stageflow.core.migrations import FORMAT_VERSION, format_version, migrate
from stageflow.exceptions import PipelineDefinitionError

DOC = {"metadata": {"title": "t"}, "entry": "end",
       "nodes": [{"id": "end", "type": "terminal", "result": {"ok": True}}]}


def _to_v2(doc: dict) -> dict:
    doc["about"] = doc.pop("metadata", {})
    return doc


def _to_v3(doc: dict) -> dict:
    doc["about"]["upgraded"] = True
    return doc


class FormatVersionTests(unittest.TestCase):
    def test_no_version_is_version_one(self):
        self.assertEqual(format_version(DOC), 1)
        self.assertEqual(FORMAT_VERSION, 1)

    def test_a_current_document_is_returned_as_is(self):
        self.assertIs(migrate(DOC), DOC)
        self.assertIs(Pipeline.from_dict(DOC).raw_json, DOC)
        Pipeline.from_dict({**DOC, "version": 1}).validate()

    def test_a_bad_version_is_refused_by_name(self):
        for bad in ["1", 0, -1, 1.0, True, None]:
            with self.subTest(version=bad):
                with self.assertRaisesRegex(PipelineDefinitionError, "positive integer"):
                    Pipeline.from_dict({**DOC, "version": bad})

    def test_a_newer_document_says_it_is_newer(self):
        with self.assertRaisesRegex(PipelineDefinitionError, "newer StageFlow"):
            Pipeline.from_dict({**DOC, "version": FORMAT_VERSION + 1})

    def test_the_core_reports_its_format(self):
        self.assertEqual(capabilities()["format"], FORMAT_VERSION)


class UpgradeTests(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch.dict(migrations.MIGRATIONS, {1: _to_v2, 2: _to_v3})
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_upgrades_run_in_order_on_a_copy(self):
        original = copy.deepcopy(DOC)
        upgraded = migrate(DOC, target=3)
        self.assertEqual(upgraded["version"], 3)
        self.assertEqual(upgraded["about"], {"title": "t", "upgraded": True})
        self.assertNotIn("metadata", upgraded)
        self.assertEqual(DOC, original)

    def test_an_upgrade_starts_where_the_document_is(self):
        v2 = {"version": 2, "about": {}, "nodes": DOC["nodes"]}
        self.assertEqual(migrate(v2, target=3)["about"], {"upgraded": True})

    def test_from_dict_upgrades_before_it_checks(self):
        # `about` is not in today's schema, so the upgrade must also undo
        # itself for this test: a v2 core would have a v2 schema.
        def back_to_v1_shape(doc):
            doc["metadata"] = doc.pop("about")
            return doc

        with mock.patch.dict(migrations.MIGRATIONS, {1: lambda d: d, 2: back_to_v1_shape}), \
                mock.patch.object(migrations, "FORMAT_VERSION", 3):
            old = {"version": 2, "about": {"title": "old"}, "nodes": DOC["nodes"]}
            pipeline = Pipeline.from_dict(old)
        self.assertEqual(pipeline.metadata, {"title": "old"})
        self.assertEqual(pipeline.raw_json["version"], 3)
        self.assertEqual(old["version"], 2)


if __name__ == "__main__":
    unittest.main()
