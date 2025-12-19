import unittest

from stageflow.testing import PipelineTestSpec, run_pipeline_test
from stageflow.core.stage import BaseStage, register_stage


@register_stage("EmitStage")
class EmitStage(BaseStage):
    async def run(self):
        self.emit("stage_done", {"id": self.stage_id})
        self.set_outputs({"value": self.config.get("value", 0)})


@register_stage("WaitAndEmitStage")
class WaitAndEmitStage(BaseStage):
    async def run(self):
        res = await self.wait_input("user_input", timeout=1.0)
        self.emit("stage_done", {"id": self.stage_id})
        if res:
            self.set_outputs({"payload_value": res["payload"].get("v")})


class PipelineTesterTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_pipeline_test_passes(self):
        pipeline = {
            "entry": "start",
            "nodes": [
                {"id": "start", "type": "stage", "stage": "EmitStage", "config": {"value": 1}, "outputs": {"value": "value"}, "next": "wait"},
                {"id": "wait", "type": "stage", "stage": "WaitAndEmitStage", "outputs": {"payload_value": "payload_value"}, "next": "end"},
                {"id": "end", "type": "terminal", "result": {"status": "ok"}, "artifacts": ["value", "payload_value"]},
            ],
        }
        spec = PipelineTestSpec(
            pipeline=pipeline,
            inputs=[{"type": "user_input", "payload": {"v": 5}}],
            expected_result={"status": "ok"},
            expected_artifacts={"value": 1, "payload_value": 5},
            expected_history=["start", "wait", "end"],
        )
        result, events = await run_pipeline_test(spec)
        self.assertEqual(result.artifacts["value"], 1)
        self.assertEqual(result.artifacts["payload_value"], 5)


if __name__ == "__main__":
    unittest.main()
