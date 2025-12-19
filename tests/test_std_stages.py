import unittest

from stageflow.core.context import Context
from stageflow.core.session import Session
from stageflow.core.pipeline import Pipeline
from stageflow.core.node import StageNode, TerminalNode
from stageflow.builtins import (  # noqa: F401 - imports register stages
    SetValueStage,
    CopyValueStage,
    IncrementStage,
    MergeDictStage,
    AppendListStage,
    ExtendListStage,
)


class StdStagesTests(unittest.IsolatedAsyncioTestCase):
    async def test_var_stages(self):
        nodes = [
            StageNode(id="set", type="stage", stage="SetValueStage",
                      config={"value": 1}, outputs={"value": "a"}, next="inc"),
            StageNode(id="inc", type="stage", stage="IncrementStage",
                      config={"delta": 2}, arguments={"current": "a"}, outputs={"value": "a"}, next="copy"),
            StageNode(id="copy", type="stage", stage="CopyValueStage",
                      arguments={"value": "a"}, outputs={"value": "b"}, next="merge"),
            StageNode(id="merge", type="stage", stage="MergeDictStage",
                      arguments={"src": "m2", "dst": "m1"}, outputs={"merged": "m1"}, next="end"),
            TerminalNode(id="end", type="terminal", result={"status": "ok"},
                         artifact_paths=["a", "b", "m1"]),
        ]
        pipeline = Pipeline(entry="set", nodes=nodes)
        ctx = Context(payload={"m1": {"x": 1}, "m2": {"y": 2}})
        result = await Session(id="t", pipeline=pipeline, context=ctx).run()
        self.assertEqual(result.artifacts["a"], 3)
        self.assertEqual(result.artifacts["b"], 3)
        self.assertEqual(result.artifacts["m1"], {"x": 1, "y": 2})

    async def test_list_stages(self):
        nodes = [
            StageNode(id="append", type="stage", stage="AppendListStage",
                      config={"value": 1}, outputs={"list": "lst"}, next="extend"),
            StageNode(id="extend", type="stage", stage="ExtendListStage",
                      arguments={"list": "lst", "items": "src"}, outputs={"list": "lst"}, next="end"),
            TerminalNode(id="end", type="terminal", result={"status": "ok"}, artifact_paths=["lst"]),
        ]
        pipeline = Pipeline(entry="append", nodes=nodes)
        ctx = Context(payload={"src": [2, 3]})
        result = await Session(id="t2", pipeline=pipeline, context=ctx).run()
        self.assertEqual(result.artifacts["lst"], [1, 2, 3])


if __name__ == "__main__":
    unittest.main()
