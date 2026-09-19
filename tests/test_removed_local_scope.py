import asyncio
import unittest

from stageflow import BaseStage, Context, Pipeline, Session, register_stage
from stageflow.exceptions import (
    ExpressionError,
    PipelineDefinitionError,
    TypeDeclarationError,
)


@register_stage("PassValueStage")
class PassValueStage(BaseStage):
    """
    description: "Echo the value argument back"
    arguments:
      value:
        type: any
    outputs:
      value:
        type: any
    """

    async def run(self):
        self.set_outputs({"value": self.get_arguments().get("value")})


def _graph(node: dict, **top) -> dict:
    return {"nodes": [node, {"id": "end", "type": "terminal", "result": {"status": "ok"}}], **top}


def _stage_node(**fields) -> dict:
    return {"id": "p", "type": "stage", "stage": "PassValueStage", "next": "end", **fields}


class RemovedLocalScopeTests(unittest.TestCase):
    def _definition_error(self, data: dict) -> str:
        with self.assertRaises((PipelineDefinitionError, TypeDeclarationError)) as caught:
            Pipeline.from_dict(data)
        return str(caught.exception)

    def test_local_bucket_in_arguments_rejected(self):
        message = self._definition_error(_graph(
            _stage_node(arguments={"local": {"value": "seed"}}), entry="p"))
        self.assertIn("local", message)

    def test_local_level_in_outputs_rejected(self):
        pipeline = Pipeline.from_dict(_graph(
            _stage_node(arguments={"const": {"value": 1}},
                        outputs={"local": {"value": "total"}}), entry="p"))
        errors = " ".join(pipeline.collect_errors())
        self.assertIn("the 'local' scope level in outputs was removed in 0.7.0", errors)

    def test_local_level_in_variables_rejected(self):
        message = self._definition_error(_graph(
            {"id": "p", "type": "entry", "next": "end"},
            variables={"local": {"total": "int"}}))
        self.assertIn("local", message)

    def test_local_prefix_in_expose_rejected(self):
        message = self._definition_error(_graph(
            {"id": "p", "type": "entry", "expose": {"local.a": "local.b"}, "next": "end"}))
        self.assertIn("local.a", message)

    def test_result_local_on_except_rejected(self):
        message = self._definition_error({
            "nodes": [
                {"id": "start", "type": "entry", "next": "guard"},
                {"id": "guard", "type": "try", "body": "end", "next": "end",
                 "except": [{"error_equals": ["*"], "next": "end", "result_local": "err"}]},
                {"id": "end", "type": "terminal", "result": {"status": "ok"}},
            ]})
        self.assertIn("result_local", message)

    def test_local_namespace_gone_from_expressions(self):
        pipeline = Pipeline.from_dict({
            "nodes": [
                {"id": "start", "type": "entry", "variables": {"n": 5}, "next": "check"},
                {"id": "check", "type": "condition", "condition": "local.n > 1",
                 "then": "end"},
                {"id": "end", "type": "terminal", "result": {"status": "ok"}},
            ]})
        with self.assertRaises(ExpressionError):
            asyncio.run(Session(id="t", pipeline=pipeline).run())

    def test_vars_namespace_works(self):
        pipeline = Pipeline.from_dict({
            "nodes": [
                {"id": "start", "type": "entry", "variables": {"n": 5}, "next": "check"},
                {"id": "check", "type": "condition", "condition": "vars.n > 1",
                 "then": "end"},
                {"id": "end", "type": "terminal", "result": {"status": "ok"}},
            ]})
        result = asyncio.run(Session(id="t", pipeline=pipeline).run())
        self.assertEqual(result.result, {"status": "ok"})

    def test_context_has_no_local_api(self):
        ctx = Context(vars={"a": 1})
        for attr in ("local", "get_local", "has_local", "with_local",
                     "without_local", "local_keys"):
            self.assertFalse(hasattr(ctx, attr), f"Context still carries '{attr}'")
        self.assertEqual(ctx.to_dict(), {"vars": {"a": 1}})

    def test_old_snapshot_context_rejected(self):
        with self.assertRaises(PipelineDefinitionError):
            Context.from_dict({"local": {"a": 1}})


if __name__ == "__main__":
    unittest.main()
