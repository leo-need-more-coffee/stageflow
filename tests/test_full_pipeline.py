import asyncio
import unittest

from stageflow.core.stage import BaseStage, register_stage
from stageflow.core.node import StageNode, ConditionNode, Condition, TerminalNode
from stageflow.core.pipeline import Pipeline
from stageflow.core.context import Context
from stageflow.core.session import Session
from stageflow.core.event import EventSpec, InputSpec
from stageflow.core.jsonlogic import JsonLogic


@register_stage("InitStage")
class InitStage(BaseStage):
    async def run(self):
        # Seed payload with initial values
        self.set_outputs({
            "flag": True,
            "need_wait": self.config.get("need_wait", False),
            "value": self.config.get("value", 0),
        })


@register_stage("MaybeFailStage")
class MaybeFailStage(BaseStage):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fail_first = True

    async def run(self):
        if self.fail_first:
            self.fail_first = False
            raise RuntimeError("first attempt fails")
        self.set_outputs({"recovered": False})


@register_stage("RecoverStage")
class RecoverStage(BaseStage):
    async def run(self):
        self.set_outputs({"recovered": True})


@register_stage("WaitStage")
class WaitStage(BaseStage):
    allowed_inputs = [InputSpec(type="user_input")]

    async def run(self):
        res = await self.wait_input("user_input", timeout=1.0)
        if res:
            payload = res.get("payload", {})
            self.set_outputs({"waited_value": payload.get("value")})


@register_stage("WorkerStage")
class WorkerStage(BaseStage):
    allowed_events = [EventSpec(type="progress")]

    async def run(self):
        args = self.get_arguments()
        total = args.get("value", 0) + (args.get("waited_value") or 0)
        self.emit("progress", {"current": total})
        self.set_outputs({"result": total})


class FullPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_full_pipeline_flow(self):
        nodes = [
            StageNode(
                id="init",
                type="stage",
                stage="InitStage",
                config={"need_wait": True, "value": 5},
                outputs={"flag": "flag", "need_wait": "need_wait", "value": "value"},
                next="maybe_fail",
            ),
            StageNode(
                id="maybe_fail",
                type="stage",
                stage="MaybeFailStage",
                next="decide",
                fallback="recover",
            ),
            StageNode(
                id="recover",
                type="stage",
                stage="RecoverStage",
                outputs={"recovered": "recovered"},
                next="decide",
            ),
            ConditionNode(
                id="decide",
                type="condition",
                conditions=[Condition(if_condition=JsonLogic({"var": "need_wait"}), then_goto="wait")],
                else_goto="worker",
            ),
            StageNode(
                id="wait",
                type="stage",
                stage="WaitStage",
                outputs={"waited_value": "waited_value"},
                next="worker",
            ),
            StageNode(
                id="worker",
                type="stage",
                stage="WorkerStage",
                arguments={"value": "value", "waited_value": "waited_value"},
                outputs={"result": "result"},
                next="finish",
            ),
            TerminalNode(
                id="finish",
                type="terminal",
                result={"status": "ok"},
                artifact_paths=["result", "recovered", "waited_value"],
            ),
        ]
        pipeline = Pipeline(entry="init", nodes=nodes)
        ctx = Context(payload={})
        session = Session(id="full", pipeline=pipeline, context=ctx)

        async def feed_input():
            while "user_input" not in session._waiting:
                await asyncio.sleep(0.01)
            await session.input("user_input", {"value": 3})

        run_task = asyncio.create_task(session.run())
        await asyncio.gather(run_task, feed_input())
        result = run_task.result()

        self.assertEqual(result.result, {"status": "ok"})
        self.assertEqual(result.artifacts["result"], 8)
        self.assertTrue(result.artifacts["recovered"])
        self.assertEqual(result.artifacts["waited_value"], 3)
        # Ensure progress event emitted
        self.assertTrue(any(e.type == "progress" for e in result.history))

    async def test_full_pipeline_from_json(self):
        pipeline_json = {
            "entry": "init",
            "nodes": [
                {"id": "init", "type": "stage", "stage": "InitStage", "config": {"need_wait": True, "value": 2},
                 "outputs": {"flag": "flag", "need_wait": "need_wait", "value": "value"}, "next": "maybe_fail"},
                {"id": "maybe_fail", "type": "stage", "stage": "MaybeFailStage", "next": "decide", "fallback": "recover"},
                {"id": "recover", "type": "stage", "stage": "RecoverStage", "outputs": {"recovered": "recovered"}, "next": "decide"},
                {"id": "decide", "type": "condition",
                 "conditions": [{"if": {"var": "need_wait"}, "then": "wait"}],
                 "else": "worker"},
                {"id": "wait", "type": "stage", "stage": "WaitStage", "outputs": {"waited_value": "waited_value"}, "next": "worker"},
                {"id": "worker", "type": "stage", "stage": "WorkerStage",
                 "arguments": {"value": "value", "waited_value": "waited_value"},
                 "outputs": {"result": "result"}, "next": "finish"},
                {"id": "finish", "type": "terminal", "result": {"status": "ok"},
                 "artifacts": ["result", "recovered", "waited_value"]},
            ],
        }
        pipeline = Pipeline.from_dict(pipeline_json)
        ctx = Context(payload={})
        session = Session(id="full-json", pipeline=pipeline, context=ctx)

        async def feed_input():
            while "user_input" not in session._waiting:
                await asyncio.sleep(0.01)
            await session.input("user_input", {"value": 4})

        task = asyncio.create_task(session.run())
        await asyncio.gather(task, feed_input())
        result = task.result()
        self.assertEqual(result.artifacts["result"], 6)
        self.assertEqual(result.artifacts["waited_value"], 4)
        self.assertTrue(any(e.type == "progress" for e in result.history))


if __name__ == "__main__":
    unittest.main()
