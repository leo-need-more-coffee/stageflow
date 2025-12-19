import asyncio
import unittest

from stageflow.core.context import Context
from stageflow.core.jsonlogic import JsonLogic
from stageflow.core.node import StageNode, TerminalNode
from stageflow.core.pipeline import Pipeline
from stageflow.core.session import Session
from stageflow.core.stage import BaseStage, register_stage


@register_stage("EchoStage")
class EchoStage(BaseStage):
    async def run(self):
        args = self.get_arguments()
        self.set_outputs({"echo": args.get("value", None)})


class SessionFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_stage_success_and_outputs(self):
        nodes = [
            StageNode(id="start", type="stage", stage="EchoStage",
                      arguments={"value": "input_value"}, outputs={"echo": "echoed"}, next="end"),
            TerminalNode(id="end", type="terminal", result={"status": "ok"}, artifact_paths=["echoed"])
        ]
        pipeline = Pipeline(entry="start", nodes=nodes)
        ctx = Context(payload={"input_value": 123})
        session = Session(id="s", pipeline=pipeline, context=ctx)
        result = await session.run()
        self.assertEqual(result.artifacts["echoed"], 123)
        self.assertEqual(result.result, {"status": "ok"})

    async def test_fallback_on_failure(self):
        nodes = [
            StageNode(id="failing", type="stage", stage="FailStage", next=None, fallback="recover"),
            StageNode(id="recover", type="stage", stage="EchoStage",
                      arguments={"value": "payload_val"}, outputs={"echo": "echoed"}, next="end"),
            TerminalNode(id="end", type="terminal", result={"status": "recovered"}, artifact_paths=["echoed"])
        ]
        pipeline = Pipeline(entry="failing", nodes=nodes)
        ctx = Context(payload={"payload_val": "ok"})
        session = Session(id="s", pipeline=pipeline, context=ctx)
        result = await session.run()
        self.assertEqual(result.result, {"status": "recovered"})
        self.assertEqual(result.artifacts["echoed"], "ok")


class JsonLogicContextTests(unittest.TestCase):
    def test_jsonlogic_reads_payload(self):
        ctx = Context(payload={"score": 10, "flag": True})
        cond = JsonLogic({"and": [{"==": [{"var": "score"}, 10]}, {"var": "flag"}]})
        self.assertTrue(cond.evaluate(ctx))

    def test_context_get_set_paths(self):
        ctx = Context(payload={"a": {"b": 1}})
        self.assertEqual(ctx.get("a.b"), 1)
        ctx.set("a.c", 2)
        self.assertEqual(ctx.get("a.c"), 2)


class WaitInputBroadcastTests(unittest.IsolatedAsyncioTestCase):
    async def test_wait_input_multiple(self):
        pipeline = Pipeline(entry="end", nodes=[TerminalNode(id="end", type="terminal", result={"status": "done"})])
        session = Session(id="s", pipeline=pipeline, context=Context())

        async def waiter():
            return await session.wait_input("ping")

        task1 = asyncio.create_task(waiter())
        task2 = asyncio.create_task(waiter())
        await asyncio.sleep(0)  # allow waiters to register
        await session.input("ping", {"msg": "hello"})
        res1, res2 = await asyncio.wait_for(asyncio.gather(task1, task2), timeout=1.0)
        self.assertEqual(res1["payload"]["msg"], "hello")
        self.assertEqual(res2["payload"]["msg"], "hello")


if __name__ == "__main__":
    unittest.main()
