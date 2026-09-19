import json
import unittest

import yaml

from stageflow import BaseStage, EventSpec, InputSpec, register_stage
from stageflow.docs.schema import generate_stages_json, generate_stages_yaml


@register_stage("DocStage")
class DocStage(BaseStage):
    """
    description: "Demo stage"
    icon: "◎"
    color: "#ff8800"
    arguments:
      foo:
        type: string
      bar:
        type: int
      base_url:
        type: str
        optional: true
        description: "Custom LLM endpoint"
        default: "asdasd"
    outputs:
      baz:
        type: string
    """

    allowed_events = [
        EventSpec(
            type="progress",
            description="Progress event",
            payload_schema={"step": int, "tags": list[str]},
        )
    ]
    allowed_inputs = [
        InputSpec(
            type="user_input",
            description="User input",
            payload_schema={"answer": str, "meta": {"id": int}, "tags": [str]},
        )
    ]
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

        bar_arg = self._find_field(doc_spec["arguments"], "bar")
        self.assertIsNotNone(bar_arg)
        self.assertEqual(bar_arg["type"], "int")

        base_url_arg = self._find_field(doc_spec["arguments"], "base_url")
        self.assertIsNotNone(base_url_arg)
        self.assertTrue(base_url_arg["optional"])
        self.assertEqual(base_url_arg["default"], "asdasd")
        self.assertIn("LLM", base_url_arg["description"])

        baz_out = self._find_field(doc_spec["outputs"], "baz")
        self.assertIsNotNone(baz_out)
        self.assertEqual(baz_out["type"], "string")

        self.assertEqual(doc_spec["allowed_events"][0]["type"], "progress")
        self.assertEqual(doc_spec["allowed_inputs"][0]["type"], "user_input")
        self.assertEqual(doc_spec["allowed_events"][0]["payload_schema"], {"step": "int", "tags": ["str"]})
        self.assertEqual(
            doc_spec["allowed_inputs"][0]["payload_schema"],
            {"answer": "str", "meta": {"id": "int"}, "tags": ["str"]},
        )
        self.assertEqual(doc_spec["category"], "demo")
        self.assertEqual(doc_spec["icon"], "◎")
        self.assertFalse(doc_spec["icon_mono"])
        self.assertEqual(doc_spec["color"], "#ff8800")

    def test_visual_hints_are_optional(self):
        class PlainStage(BaseStage):
            """
            description: "No visual hints"
            """
            stage_name = "PlainStage"

            async def run(self):
                return None

        spec = PlainStage.get_specs()
        self.assertEqual(spec["icon"], "")
        self.assertFalse(spec["icon_mono"])
        self.assertIsNone(spec["color"])

    def test_icon_can_be_svg_reference(self):
        class SvgIconStage(BaseStage):
            """
            description: "SVG icon"
            icon: "/icons/globe.svg"
            icon_mono: true
            """
            stage_name = "SvgIconStage"

            async def run(self):
                return None

        spec = SvgIconStage.get_specs()
        self.assertEqual(spec["icon"], "/icons/globe.svg")
        self.assertTrue(spec["icon_mono"])

    def test_builtin_stages_declare_icons(self):
        from stageflow import get_stages

        missing = [name for name, cls in get_stages().items()
                   if (cls.category or "").startswith("builtin.") and not cls.get_specs()["icon"]]
        self.assertEqual(missing, [])

    def test_generate_yaml_contains_description(self):
        doc_yaml = generate_stages_yaml({"DocStage": DocStage})
        specs = yaml.safe_load(doc_yaml)
        self.assertIn("DocStage", specs)
        self.assertEqual(specs["DocStage"]["description"], "Demo stage")
        self.assertIsInstance(specs["DocStage"]["arguments"], list)

class NodeSchemaDispatchTests(unittest.TestCase):
    @staticmethod
    def _errors(node):
        from stageflow import get_stages
        from stageflow.docs.schema import generate_pipeline_schema
        import jsonschema
        schema = generate_pipeline_schema(get_stages())
        data = {"entry": node["id"], "nodes": [
            node, {"id": "end", "type": "terminal", "result": {"status": "ok"}},
        ]}
        validator = jsonschema.Draft202012Validator(schema)
        return [e.message for e in validator.iter_errors(data)]

    def test_valid_node_passes(self):
        self.assertEqual(self._errors({
            "id": "p", "type": "stage", "stage": "SetValueStage",
            "arguments": {"const": {"value": 1}}, "next": "end"}), [])

    def test_unknown_field_rejected(self):
        errors = self._errors({
            "id": "p", "type": "stage", "stage": "SetValueStage",
            "arguments": {"const": {"value": 1}}, "nxt": "end"})
        self.assertTrue(any("nxt" in e for e in errors), errors)

    def test_missing_required_field_rejected(self):
        errors = self._errors({"id": "c", "type": "condition", "then": "end"})
        self.assertTrue(any("condition" in e for e in errors), errors)

    def test_every_registered_node_type_has_a_branch(self):
        from stageflow import get_node_types
        from stageflow.docs.schema import load_pipeline_schema
        schema = load_pipeline_schema()
        dispatched = {
            branch["if"]["properties"]["type"]["const"]
            for branch in schema["$defs"]["node"]["allOf"]
        }
        self.assertEqual(dispatched, set(get_node_types()))

    def test_unregistered_stage_name_rejected(self):
        errors = self._errors({"id": "p", "type": "stage", "stage": "NoSuchStage",
                               "next": "end"})
        self.assertTrue(any("NoSuchStage" in e for e in errors), errors)


if __name__ == "__main__":
    unittest.main()
