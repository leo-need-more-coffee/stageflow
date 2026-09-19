import unittest

from stageflow import Context, Pipeline, Session, StageNode, TerminalNode
from stageflow.exceptions import StageContractError


class StdStagesTests(unittest.IsolatedAsyncioTestCase):
    async def test_var_stages(self):
        nodes = [
            StageNode(id="set", stage="SetValueStage",
                      arguments={"const": {"value": 1}},
                      outputs={"value": "a"}, next="inc"),
            StageNode(id="inc", stage="IncrementStage",
                      arguments={"vars": {"current": "a"}, "const": {"delta": 2}},
                      outputs={"value": "a"}, next="copy"),
            StageNode(id="copy", stage="CopyValueStage",
                      arguments={"vars": {"value": "a"}},
                      outputs={"value": "b"}, next="merge"),
            StageNode(id="merge", stage="MergeDictStage",
                      arguments={"vars": {"src": "m2", "dst": "m1"}},
                      outputs={"merged": "m1"}, next="end"),
            TerminalNode(id="end", result={"status": "ok"}, artifacts=["a", "b", "m1"]),
        ]
        pipeline = Pipeline(entry="set", nodes=nodes)
        ctx = Context(vars={"m1": {"x": 1}, "m2": {"y": 2}})
        result = await Session(id="t", pipeline=pipeline, context=ctx).run()
        self.assertEqual(result.artifacts["a"], 3)
        self.assertEqual(result.artifacts["b"], 3)
        self.assertEqual(result.artifacts["m1"], {"x": 1, "y": 2})

    async def test_list_stages(self):
        nodes = [
            StageNode(id="append", stage="AppendListStage",
                      arguments={"const": {"value": 1}},
                      outputs={"list": "lst"}, next="extend"),
            StageNode(id="extend", stage="ExtendListStage",
                      arguments={"vars": {"list": "lst", "items": "src"}},
                      outputs={"list": "lst"}, next="end"),
            TerminalNode(id="end", result={"status": "ok"}, artifacts=["lst"]),
        ]
        pipeline = Pipeline(entry="append", nodes=nodes)
        ctx = Context(vars={"src": [2, 3]})
        result = await Session(id="t2", pipeline=pipeline, context=ctx).run()
        self.assertEqual(result.artifacts["lst"], [1, 2, 3])

    async def test_list_stages_do_not_mutate_input(self):
        source = [1, 2]
        nodes = [
            StageNode(id="append", stage="AppendListStage",
                      arguments={"vars": {"list": "lst"}, "const": {"value": 3}},
                      outputs={"list": "lst2"}, next="end"),
            TerminalNode(id="end", result={"status": "ok"}, artifacts=["lst", "lst2"]),
        ]
        pipeline = Pipeline(entry="append", nodes=nodes)
        result = await Session(id="t3", pipeline=pipeline, context=Context(vars={"lst": source})).run()
        self.assertEqual(result.artifacts["lst2"], [1, 2, 3])
        self.assertEqual(result.artifacts["lst"], [1, 2])
        self.assertEqual(source, [1, 2])

    async def test_pop_empty_list_fails_loudly(self):
        nodes = [
            StageNode(id="pop", stage="PopListStage",
                      arguments={"vars": {"items": "lst"}},
                      outputs={"popped": "popped"}, next="end"),
            TerminalNode(id="end", result={"status": "ok"}),
        ]
        pipeline = Pipeline(entry="pop", nodes=nodes)
        session = Session(id="t4", pipeline=pipeline, context=Context(vars={"lst": []}))
        with self.assertRaises(StageContractError):
            await session.run()

class ConstArgumentsTests(unittest.IsolatedAsyncioTestCase):
    async def _run(self, nodes, variables=None, artifacts=()):
        pipeline = Pipeline.from_dict({"entry": nodes[0]["id"], "nodes": [
            *nodes,
            {"id": "end", "type": "terminal", "result": {"status": "ok"},
             "artifacts": list(artifacts)},
        ]})
        self.assertEqual(pipeline.collect_errors(), [])
        session = Session(id="c", pipeline=pipeline, context=Context(vars=variables or {}))
        return await session.run(), session

    async def test_filter_condition_stays_literal_cel_source(self):
        result, _ = await self._run([
            {"id": "f", "type": "stage", "stage": "FilterListStage",
             "arguments": {"vars": {"items": "nums"}, "const": {"condition": "item > 2"}},
             "outputs": {"list": "out"}, "next": "end"},
        ], {"nums": [1, 2, 3, 4]}, ["out"])
        self.assertEqual(result.artifacts["out"], [3, 4])

    async def test_dict_stages_take_keys_as_argument(self):
        result, _ = await self._run([
            {"id": "pick", "type": "stage", "stage": "PickKeysStage",
             "arguments": {"vars": {"src": "d"}, "const": {"keys": ["a"]}},
             "outputs": {"result": "picked"}, "next": "drop"},
            {"id": "drop", "type": "stage", "stage": "DropKeysStage",
             "arguments": {"vars": {"src": "d"}, "const": {"keys": ["a"]}},
             "outputs": {"result": "dropped"}, "next": "end"},
        ], {"d": {"a": 1, "b": 2}}, ["picked", "dropped"])
        self.assertEqual(result.artifacts["picked"], {"a": 1})
        self.assertEqual(result.artifacts["dropped"], {"b": 2})

    async def test_string_stages_output_declared_field(self):
        result, _ = await self._run([
            {"id": "concat", "type": "stage", "stage": "ConcatStage",
             "arguments": {"const": {"parts": ["a", "b"], "separator": "-"}},
             "outputs": {"value": "s"}, "next": "tpl"},
            {"id": "tpl", "type": "stage", "stage": "TemplateStage",
             "arguments": {"vars": {"who": "s"}, "const": {"template": "hi {who}"}},
             "outputs": {"value": "t"}, "next": "end"},
        ], artifacts=["s", "t"])
        self.assertEqual(result.artifacts["s"], "a-b")
        self.assertEqual(result.artifacts["t"], "hi a-b")

    async def test_template_placeholder_is_a_bare_name(self):
        for template in ("{who.__class__}", "{who[0]}", "{0}", "{}"):
            with self.subTest(template=template):
                with self.assertRaises(StageContractError):
                    await self._run([
                        {"id": "tpl", "type": "stage", "stage": "TemplateStage",
                         "arguments": {"vars": {"who": "s"},
                                       "const": {"template": template}},
                         "outputs": {"value": "t"}, "next": "end"},
                    ], {"s": "мир"}, ["t"])

    async def test_template_reports_unknown_placeholder(self):
        with self.assertRaises(StageContractError) as ctx:
            await self._run([
                {"id": "tpl", "type": "stage", "stage": "TemplateStage",
                 "arguments": {"const": {"template": "hi {nobody}"}},
                 "outputs": {"value": "t"}, "next": "end"},
            ], artifacts=["t"])
        self.assertIn("nobody", str(ctx.exception))

    async def test_template_keeps_format_spec_and_escaping(self):
        result, _ = await self._run([
            {"id": "tpl", "type": "stage", "stage": "TemplateStage",
             "arguments": {"vars": {"n": "n"},
                           "const": {"template": "{n:03d} {{не плейсхолдер}}"}},
             "outputs": {"value": "t"}, "next": "end"},
        ], {"n": 7}, ["t"])
        self.assertEqual(result.artifacts["t"], "007 {не плейсхолдер}")

    async def test_assert_sees_other_arguments_as_vars(self):
        result, _ = await self._run([
            {"id": "a", "type": "stage", "stage": "AssertStage",
             "arguments": {"vars": {"n": "n"}, "const": {"condition": "vars.n > 1"}},
             "next": "end"},
        ], {"n": 5})
        self.assertEqual(result.result, {"status": "ok"})

    async def test_assert_failure_uses_message_argument(self):
        with self.assertRaises(AssertionError) as ctx:
            await self._run([
                {"id": "a", "type": "stage", "stage": "AssertStage",
                 "arguments": {"vars": {"n": "n"},
                               "const": {"condition": "vars.n > 10", "message": "мало"}},
                 "next": "end"},
            ], {"n": 5})
        self.assertIn("мало", str(ctx.exception))

    async def test_log_merges_message_with_other_arguments(self):
        _, session = await self._run([
            {"id": "l", "type": "stage", "stage": "LogStage",
             "arguments": {"vars": {"n": "n"}, "const": {"message": "привет"}},
             "next": "end"},
        ], {"n": 7})
        logs = [e.payload for e in session.event_history if e.type == "log"]
        self.assertEqual(logs, [{"message": "привет", "n": 7}])

    async def test_fail_message_comes_from_argument(self):
        with self.assertRaises(RuntimeError) as ctx:
            await self._run([
                {"id": "f", "type": "stage", "stage": "FailStage",
                 "arguments": {"const": {"message": "бум"}}, "next": "end"},
            ])
        self.assertIn("бум", str(ctx.exception))

    async def test_variable_reference_wins_over_const(self):
        result, _ = await self._run([
            {"id": "inc", "type": "stage", "stage": "IncrementStage",
             "arguments": {"vars": {"current": "n", "delta": "step"},
                           "const": {"delta": 100}},
             "outputs": {"value": "out"}, "next": "end"},
        ], {"n": 1, "step": 2}, ["out"])
        self.assertEqual(result.artifacts["out"], 3)

    async def test_missing_required_argument_fails_fast(self):
        for stage in ("SetValueStage", "AppendListStage"):
            with self.subTest(stage=stage), self.assertRaises(StageContractError):
                await self._run([{"id": "x", "type": "stage", "stage": stage, "next": "end"}])


if __name__ == "__main__":
    unittest.main()
