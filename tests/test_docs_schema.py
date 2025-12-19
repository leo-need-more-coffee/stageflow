import json
import unittest

import yaml

from stageflow.docs.schema import generate_stages_yaml, generate_stages_json
from stageflow.core.stage import BaseStage, register_stage
from stageflow.core.event import EventSpec, InputSpec


@register_stage("DocStage")
class DocStage(BaseStage):
    """
    description: "Demo stage"
    arguments:
      foo:
        type: string
    config:
      bar:
        type: int
      base_url:
        type: str
        optional: true
        description: "Кастомный endpoint для LLM"
        default: "asdasd"
    outputs:
      baz:
        type: string
    """

    allowed_events = [EventSpec(type="progress", description="Progress event")]
    allowed_inputs = [InputSpec(type="user_input", description="User input")]
    category = "demo"

    async def run(self):
        return None


class DocsSchemaTests(unittest.TestCase):
    @staticmethod
    def _find_field(fields, name):
        return next((f for f in fields if f.get("name") == name), None)

    def test_generate_json_includes_specs(self):
        doc_json = generate_stages_json({"DocStage": DocStage})
        specs = json.loads(doc_json)
        self.assertIn("DocStage", specs)
        doc_spec = specs["DocStage"]
        self.assertEqual(doc_spec["description"], "Demo stage")

        foo_arg = self._find_field(doc_spec["arguments"], "foo")
        self.assertIsNotNone(foo_arg)
        self.assertEqual(foo_arg["type"], "string")

        bar_cfg = self._find_field(doc_spec["config"], "bar")
        self.assertIsNotNone(bar_cfg)
        self.assertEqual(bar_cfg["type"], "int")

        base_url_cfg = self._find_field(doc_spec["config"], "base_url")
        self.assertIsNotNone(base_url_cfg)
        self.assertTrue(base_url_cfg["optional"])
        self.assertEqual(base_url_cfg["default"], "asdasd")
        self.assertIn("LLM", base_url_cfg["description"])

        baz_out = self._find_field(doc_spec["outputs"], "baz")
        self.assertIsNotNone(baz_out)
        self.assertEqual(baz_out["type"], "string")

        self.assertEqual(doc_spec["allowed_events"][0]["type"], "progress")
        self.assertEqual(doc_spec["allowed_inputs"][0]["type"], "user_input")
        self.assertEqual(doc_spec["category"], "demo")

    def test_generate_yaml_contains_description(self):
        doc_yaml = generate_stages_yaml({"DocStage": DocStage})
        specs = yaml.safe_load(doc_yaml)
        self.assertIn("DocStage", specs)
        self.assertEqual(specs["DocStage"]["description"], "Demo stage")
        self.assertIsInstance(specs["DocStage"]["arguments"], list)


if __name__ == "__main__":
    unittest.main()
