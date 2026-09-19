import unittest

from stageflow import BaseStage, Context, Pipeline, Session, register_stage
from stageflow.exceptions import ArtifactNotFoundError


@register_stage("OuterStage")
class OuterStage(BaseStage):
    async def run(self):
        self.set_outputs({"x": 1})


@register_stage("InnerStage")
class InnerStage(BaseStage):
    async def run(self):
        args = self.get_arguments()
        self.set_outputs({"sum": (args.get("a") or 0) + (args.get("b") or 0)})


INNER_FLOW = {
    "entry": "inner",
    "nodes": [
        {"id": "inner", "type": "stage", "stage": "InnerStage",
         "arguments": {"vars": ["a", "b"]},
         "outputs": {"sum": "sum"},
         "next": "end"},
        {"id": "end", "type": "terminal", "result": {"status": "inner_ok"}, "artifacts": ["sum"]},
    ],
}


class SubpipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_subpipeline_artifacts_flow(self):
        pipeline_data = {
            "entry": "start",
            "nodes": [
                {"id": "start", "type": "stage", "stage": "OuterStage",
                 "outputs": {"x": "a"}, "next": "child"},
                {"id": "child", "type": "subpipeline", "subpipeline_id": "inner_flow",
                 "inputs": {"a": "a", "b": "b"},
                 "artifact_outputs": {"sum_out": "sum"},
                 "result_output": "inner_result",
                 "next": "finish"},
                {"id": "finish", "type": "terminal", "result": {"status": "ok"},
                 "artifacts": ["sum_out", "inner_result"]},
            ],
            "subpipelines": {"inner_flow": INNER_FLOW},
        }
        pipeline = Pipeline.from_dict(pipeline_data)
        session = Session(id="outer", pipeline=pipeline, context=Context(vars={"b": 2}))
        result = await session.run()
        self.assertEqual(result.artifacts["sum_out"], 3)
        self.assertEqual(result.artifacts["inner_result"], {"status": "inner_ok"})

    async def test_child_does_not_see_parent_frame(self):
        pipeline_data = {
            "entry": "child",
            "nodes": [
                {"id": "child", "type": "subpipeline", "subpipeline_id": "peek",
                 "inputs": {"a": "a"},
                 "artifact_outputs": {"seen": "seen"},
                 "next": "finish"},
                {"id": "finish", "type": "terminal", "artifacts": ["seen", "secret"]},
            ],
            "subpipelines": {
                "peek": {
                    "entry": "look",
                    "nodes": [
                        {"id": "look", "type": "stage", "stage": "InnerStage",
                         "arguments": {"vars": ["a", "secret"]},
                         "outputs": {"sum": "seen"},
                         "next": "end"},
                        {"id": "end", "type": "terminal", "artifacts": ["seen"]},
                    ],
                },
            },
        }
        ctx = Context(vars={"a": 7, "secret": 1000})
        result = await Session(id="outer", pipeline=Pipeline.from_dict(pipeline_data), context=ctx).run()
        self.assertEqual(result.artifacts["seen"], 7)
        self.assertEqual(result.artifacts["secret"], 1000)

    async def test_nested_subpipeline_isolated_context(self):
        pipeline_data = {
            "entry": "start",
            "nodes": [
                {"id": "start", "type": "stage", "stage": "OuterStage",
                 "outputs": {"x": "a"}, "next": "call_mid"},
                {"id": "call_mid", "type": "subpipeline", "subpipeline_id": "mid_flow",
                 "inputs": {"a": "a", "b": "b"},
                 "artifact_outputs": {"final_sum": "sum_mid"},
                 "next": "finish"},
                {"id": "finish", "type": "terminal", "result": {"status": "ok"},
                 "artifacts": ["final_sum"]},
            ],
            "subpipelines": {
                "inner_flow": INNER_FLOW,
                "mid_flow": {
                    "entry": "call_inner",
                    "nodes": [
                        {"id": "call_inner", "type": "subpipeline", "subpipeline_id": "inner_flow",
                         "inputs": {"a": "a", "b": "b"},
                         "artifact_outputs": {"sum_mid": "sum"},
                         "next": "mid_end"},
                        {"id": "mid_end", "type": "terminal", "result": {"status": "mid_ok"},
                         "artifacts": ["sum_mid"]},
                    ],
                },
            },
        }
        ctx = Context(vars={"b": 4})
        result = await Session(id="outer", pipeline=Pipeline.from_dict(pipeline_data), context=ctx).run()
        self.assertEqual(result.artifacts["final_sum"], 5)
        self.assertNotIn("sum", result.context.var_names())
        self.assertNotIn("sum_mid", result.context.var_names())

    async def test_missing_artifact_raises(self):
        pipeline_data = {
            "entry": "start",
            "nodes": [
                {"id": "start", "type": "stage", "stage": "OuterStage",
                 "outputs": {"x": "a"}, "next": "child"},
                {"id": "child", "type": "subpipeline", "subpipeline_id": "inner_flow",
                 "inputs": {"a": "a", "b": "b"},
                 "artifact_outputs": {"missing": "nope"}, "next": "finish"},
                {"id": "finish", "type": "terminal", "result": {"status": "ok"}, "artifacts": []},
            ],
            "subpipelines": {"inner_flow": INNER_FLOW},
        }
        session = Session(id="outer", pipeline=Pipeline.from_dict(pipeline_data),
                          context=Context(vars={"b": 1}))
        with self.assertRaises(ArtifactNotFoundError):
            await session.run()

    async def test_subpipeline_in_parallel(self):
        pipeline_data = {
            "entry": "par",
            "nodes": [
                {"id": "par", "type": "parallel",
                 "branches": [{"id": "only", "entry": "child"}], "next": "finish"},
                {"id": "child", "type": "subpipeline", "subpipeline_id": "inner_flow",
                 "inputs": {"a": "a", "b": "b"},
                 "artifact_outputs": {"sum_out": "sum"}},
                {"id": "finish", "type": "terminal", "result": {"status": "ok"},
                 "artifacts": ["sum_out"]},
            ],
            "subpipelines": {"inner_flow": INNER_FLOW},
        }
        ctx = Context(vars={"a": 2, "b": 3})
        result = await Session(id="outer", pipeline=Pipeline.from_dict(pipeline_data), context=ctx).run()
        self.assertEqual(result.artifacts["sum_out"], 5)


if __name__ == "__main__":
    unittest.main()
