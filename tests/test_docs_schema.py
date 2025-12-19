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
      foo: string
    config:
      bar: int
    outputs:
      baz: string
    """

    allowed_events = [EventSpec(type="progress", description="Progress event")]
    allowed_inputs = [InputSpec(type="user_input", description="User input")]
    category = "demo"

    async def run(self):
        return None


class DocsSchemaTests(unittest.TestCase):
    def test_generate_json_includes_specs(self):
        doc_json = generate_stages_json({"DocStage": DocStage})
        specs = json.loads(doc_json)
        self.assertIn("DocStage", specs)
        self.assertEqual(specs["DocStage"]["description"], "Demo stage")
        self.assertIn("bar", specs["DocStage"]["config"])
        self.assertIn("foo", specs["DocStage"]["arguments"])
        self.assertEqual(specs["DocStage"]["allowed_events"][0]["type"], "progress")
        self.assertEqual(specs["DocStage"]["allowed_inputs"][0]["type"], "user_input")
        self.assertEqual(specs["DocStage"]["category"], "demo")

    def test_generate_yaml_contains_description(self):
        doc_yaml = generate_stages_yaml({"DocStage": DocStage})
        specs = yaml.safe_load(doc_yaml)
        self.assertIn("DocStage", specs)
        self.assertEqual(specs["DocStage"]["description"], "Demo stage")


if __name__ == "__main__":
    unittest.main()
