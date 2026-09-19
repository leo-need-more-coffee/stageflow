import asyncio
import unittest

from stageflow import (
    BaseStage,
    ExceptHandler,
    ConditionNode,
    Context,
    EventSpec,
    InputSpec,
    Pipeline,
    Session,
    StageNode,
    TerminalNode,
    TryNode,
    register_stage,
)


@register_stage("InitStage")
class InitStage(BaseStage):
    async def run(self):
        args = self.get_arguments()
        self.set_outputs({
            "flag": True,
            "need_wait": args.get("need_wait", False),
            "value": args.get("value", 0),
        })


@register_stage("MaybeFailStage")
class MaybeFailStage(BaseStage):
    _failed_once = False

    async def run(self):
        if not type(self)._failed_once:
            type(self)._failed_once = True
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
        payload = (res or {}).get("payload", {})
        self.set_outputs({"waited_value": payload.get("value")})


@register_stage("WorkerStage")
class WorkerStage(BaseStage):
    allowed_events = [EventSpec(type="progress")]

    async def run(self):
        args = self.get_arguments()
        total = (args.get("value") or 0) + (args.get("waited_value") or 0)
        self.emit("progress", {"current": total})
        self.set_outputs({"result": total})


class FullPipelineTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        MaybeFailStage._failed_once = False

    async def test_full_pipeline_flow(self):
        nodes = [
            StageNode(
                id="init",
                stage="InitStage",
                arguments={"const": {"need_wait": True, "value": 5}},
                outputs={"flag": "flag", "need_wait": "need_wait", "value": "value"},
                next="guard",
            ),
            TryNode(
                id="guard",
                body="maybe_fail",
                handlers=[ExceptHandler(error_equals=["RuntimeError"], next="recover")],
                next="decide",
            ),
            StageNode(
                id="maybe_fail",
                stage="MaybeFailStage",
            ),
            StageNode(
                id="recover",
                stage="RecoverStage",
                outputs={"recovered": "recovered"},
                next="decide",
            ),
            ConditionNode(
                id="decide",
                condition="vars.need_wait",
                then="wait",
                else_="worker",
            ),
            StageNode(
                id="wait",
                stage="WaitStage",
                outputs={"waited_value": "waited_value"},
                next="worker",
            ),
            StageNode(
                id="worker",
                stage="WorkerStage",
                arguments={"vars": ["value", "waited_value"]},
                outputs={"result": "result"},
                next="finish",
            ),
            TerminalNode(
                id="finish",
                result={"status": "ok"},
                artifacts=["result", "recovered", "waited_value"],
            ),
        ]
        pipeline = Pipeline(entry="init", nodes=nodes)
        session = Session(id="full", pipeline=pipeline, context=Context())

        async def feed_input():
            while not session.is_waiting_for("user_input"):
                await asyncio.sleep(0.01)
            await session.input("user_input", {"value": 3})

        run_task = asyncio.create_task(session.run())
        await asyncio.gather(run_task, feed_input())
        result = run_task.result()

        self.assertEqual(result.result, {"status": "ok"})
        self.assertEqual(result.artifacts["result"], 8)
        self.assertTrue(result.artifacts["recovered"])
        self.assertEqual(result.artifacts["waited_value"], 3)
        self.assertTrue(any(e.type == "progress" for e in result.history))

    async def test_full_pipeline_from_json(self):
        pipeline_json = {
            "entry": "init",
            "nodes": [
                {"id": "init", "type": "stage", "stage": "InitStage",
                 "arguments": {"const": {"need_wait": True, "value": 2}},
                 "outputs": {"flag": "flag", "need_wait": "need_wait", "value": "value"},
                 "next": "guard"},
                {"id": "guard", "type": "try", "body": "maybe_fail", "next": "decide",
                 "except": [{"error_equals": ["RuntimeError"], "next": "recover"}]},
                {"id": "maybe_fail", "type": "stage", "stage": "MaybeFailStage"},
                {"id": "recover", "type": "stage", "stage": "RecoverStage",
                 "outputs": {"recovered": "recovered"}, "next": "decide"},
                {"id": "decide", "type": "condition", "condition": "vars.need_wait",
                 "then": "wait", "else": "worker"},
                {"id": "wait", "type": "stage", "stage": "WaitStage",
                 "outputs": {"waited_value": "waited_value"}, "next": "worker"},
                {"id": "worker", "type": "stage", "stage": "WorkerStage",
                 "arguments": {"vars": ["value", "waited_value"]},
                 "outputs": {"result": "result"}, "next": "finish"},
                {"id": "finish", "type": "terminal", "result": {"status": "ok"},
                 "artifacts": ["result", "recovered", "waited_value"]},
            ],
        }
        pipeline = Pipeline.from_dict(pipeline_json)
        session = Session(id="full-json", pipeline=pipeline, context=Context())

        async def feed_input():
            while not session.is_waiting_for("user_input"):
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
