import unittest

from stageflow.core.stage import BaseStage, register_stage
from stageflow.core.pipeline import Pipeline
from stageflow.core.context import Context
from stageflow.core.session import Session


@register_stage("OuterStage")
class OuterStage(BaseStage):
    async def run(self):
        self.set_outputs({"x": 1})


@register_stage("InnerStage")
class InnerStage(BaseStage):
    async def run(self):
        args = self.get_arguments()
        self.set_outputs({"sum": args.get("a", 0) + args.get("b", 0)})


class SubpipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_subpipeline_artifacts_flow(self):
        subpipelines = {
            "inner_flow": {
                "entry": "inner",
                "nodes": [
                    {
                        "id": "inner",
                        "type": "stage",
                        "stage": "InnerStage",
                        "arguments": {"a": "a", "b": "b"},
                        "outputs": {"sum": "sum"},
                        "next": "end",
                    },
                    {
                        "id": "end",
                        "type": "terminal",
                        "result": {"status": "inner_ok"},
                        "artifacts": ["sum"],
                    },
                ],
            }
        }

        pipeline_data = {
            "entry": "start",
            "nodes": [
                {
                    "id": "start",
                    "type": "stage",
                    "stage": "OuterStage",
                    "outputs": {"x": "a"},
                    "next": "child",
                },
                {
                    "id": "child",
                    "type": "subpipeline",
                    "subpipeline_id": "inner_flow",
                    "inputs": {"a": "a", "b": "b"},
                    "artifact_outputs": {"sum_out": "sum"},
                    "result_output": "inner_result",
                    "next": "finish",
                },
                {
                    "id": "finish",
                    "type": "terminal",
                    "result": {"status": "ok"},
                    "artifacts": ["sum_out", "inner_result"],
                },
            ],
            "subpipelines": subpipelines,
        }
        pipeline = Pipeline.from_dict(pipeline_data)
        ctx = Context(payload={"b": 2})
        session = Session(id="outer", pipeline=pipeline, context=ctx)
        result = await session.run()
        self.assertEqual(result.artifacts["sum_out"], 3)
        self.assertEqual(result.artifacts["inner_result"], {"status": "inner_ok"})

    async def test_nested_subpipeline_isolated_context(self):
        subpipelines = {
            "inner_flow": {
                "entry": "inner",
                "nodes": [
                    {"id": "inner", "type": "stage", "stage": "InnerStage",
                     "arguments": {"a": "a", "b": "b"},
                     "outputs": {"sum": "sum"},
                     "next": "end"},
                    {"id": "end", "type": "terminal", "result": {"status": "ok"}, "artifacts": ["sum"]},
                ],
            },
            "mid_flow": {
                "entry": "call_inner",
                "nodes": [
                    {
                        "id": "call_inner",
                        "type": "subpipeline",
                        "subpipeline_id": "inner_flow",
                        "inputs": {"a": "a", "b": "b"},
                        "artifact_outputs": {"sum_mid": "sum"},
                        "next": "mid_end",
                    },
                    {"id": "mid_end", "type": "terminal", "result": {"status": "mid_ok"}, "artifacts": ["sum_mid"]},
                ],
            },
        }
        pipeline_data = {
            "entry": "start",
            "nodes": [
                {"id": "start", "type": "stage", "stage": "OuterStage", "outputs": {"x": "a"}, "next": "call_mid"},
                {
                    "id": "call_mid",
                    "type": "subpipeline",
                    "subpipeline_id": "mid_flow",
                    "inputs": {"a": "a", "b": "b"},
                    "artifact_outputs": {"final_sum": "sum_mid"},
                    "next": "finish",
                },
                {"id": "finish", "type": "terminal", "result": {"status": "ok"}, "artifacts": ["final_sum"]},
            ],
            "subpipelines": subpipelines,
        }
        ctx = Context(payload={"b": 4})
        result = await Session(id="outer", pipeline=Pipeline.from_dict(pipeline_data), context=ctx).run()
        self.assertEqual(result.artifacts["final_sum"], 5)
        # Ensure intermediate keys not leaked unless mapped
        self.assertNotIn("sum", result.context.payload)
        self.assertNotIn("sum_mid", result.context.payload)

    async def test_missing_artifact_raises(self):
        subpipelines = {
            "inner_flow": {
                "entry": "inner",
                "nodes": [
                    {"id": "inner", "type": "stage", "stage": "InnerStage",
                     "arguments": {"a": "a", "b": "b"},
                     "outputs": {"sum": "sum"},
                     "next": "end"},
                    {"id": "end", "type": "terminal", "result": {"status": "ok"}, "artifacts": ["sum"]},
                ],
            }
        }
        pipeline_data = {
            "entry": "start",
            "nodes": [
                {"id": "start", "type": "stage", "stage": "OuterStage", "outputs": {"x": "a"}, "next": "child"},
                {"id": "child", "type": "subpipeline", "subpipeline_id": "inner_flow",
                 "inputs": {"a": "a", "b": "b"}, "artifact_outputs": {"missing": "nope"}, "next": "finish"},
                {"id": "finish", "type": "terminal", "result": {"status": "ok"}, "artifacts": []},
            ],
            "subpipelines": subpipelines,
        }
        session = Session(id="outer", pipeline=Pipeline.from_dict(pipeline_data), context=Context(payload={"b": 1}))
        with self.assertRaises(ValueError):
            await session.run()

    async def test_subpipeline_in_parallel(self):
        subpipelines = {
            "inner_flow": {
                "entry": "inner",
                "nodes": [
                    {"id": "inner", "type": "stage", "stage": "InnerStage",
                     "arguments": {"a": "a", "b": "b"},
                     "outputs": {"sum": "sum"},
                     "next": "end"},
                    {"id": "end", "type": "terminal", "result": {"status": "ok"}, "artifacts": ["sum"]},
                ],
            }
        }
        pipeline_data = {
            "entry": "par",
            "nodes": [
                {"id": "par", "type": "parallel", "children": ["child"], "policy": "all", "next": "finish"},
                {"id": "child", "type": "subpipeline", "subpipeline_id": "inner_flow",
                 "inputs": {"a": "a", "b": "b"}, "artifact_outputs": {"sum_out": "sum"}, "next": "finish"},
                {"id": "finish", "type": "terminal", "result": {"status": "ok"}, "artifacts": ["sum_out"]},
            ],
            "subpipelines": subpipelines,
        }
        ctx = Context(payload={"a": 2, "b": 3})
        result = await Session(id="outer", pipeline=Pipeline.from_dict(pipeline_data), context=ctx).run()
        self.assertEqual(result.artifacts["sum_out"], 5)


if __name__ == "__main__":
    unittest.main()
