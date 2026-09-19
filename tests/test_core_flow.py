import asyncio
import unittest

from stageflow import (
    BaseStage,
    Context,
    ExceptHandler,
    Pipeline,
    Retrier,
    Session,
    StageNode,
    TerminalNode,
    TryNode,
    register_stage,
)
from stageflow.exceptions import ArtifactNotFoundError, StageOutputError


@register_stage("EchoStage")
class EchoStage(BaseStage):
    async def run(self):
        args = self.get_arguments()
        self.set_outputs({"echo": args.get("value", None)})


class SessionFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_stage_success_and_outputs(self):
        nodes = [
            StageNode(
                id="start",
                stage="EchoStage",
                arguments={"vars": {"value": "input_value"}},
                outputs={"echo": "echoed"},
                next="end",
            ),
            TerminalNode(id="end", result={"status": "ok"}, artifacts=["echoed"]),
        ]
        pipeline = Pipeline(entry="start", nodes=nodes)
        session = Session(id="s", pipeline=pipeline, context=Context(vars={"input_value": 123}))
        result = await session.run()
        self.assertEqual(result.artifacts["echoed"], 123)
        self.assertEqual(result.result, {"status": "ok"})

    async def test_try_catches_failure_in_body(self):
        nodes = [
            TryNode(
                id="guard",
                body="failing",
                handlers=[ExceptHandler(error_equals=["*"], next="recover", result_var="error")],
            ),
            StageNode(
                id="failing",
                stage="FailStage",
                arguments={"const": {"message": "boom"}},
            ),
            StageNode(
                id="recover",
                stage="EchoStage",
                arguments={"vars": {"value": "payload_val"}},
                outputs={"echo": "echoed"},
                next="end",
            ),
            TerminalNode(id="end", result={"status": "recovered"}, artifacts=["echoed", "error"]),
        ]
        pipeline = Pipeline(entry="guard", nodes=nodes)
        session = Session(id="s", pipeline=pipeline, context=Context(vars={"payload_val": "ok"}))
        result = await session.run()
        self.assertEqual(result.result, {"status": "recovered"})
        self.assertEqual(result.artifacts["echoed"], "ok")
        self.assertEqual(result.artifacts["error"]["type"], "RuntimeError")
        self.assertEqual(result.artifacts["error"]["message"], "boom")

    async def test_retry_then_success(self):
        attempts = {"n": 0}

        @register_stage("FlakyStage")
        class FlakyStage(BaseStage):
            async def run(self):
                attempts["n"] += 1
                if attempts["n"] < 3:
                    raise TimeoutError("not yet")
                self.set_outputs({"ok": True})

        nodes = [
            StageNode(
                id="flaky",
                stage="FlakyStage",
                outputs={"ok": "flag"},
                retry=[Retrier(error_equals=["TimeoutError"], max_attempts=5, interval_seconds=0.001)],
                next="end",
            ),
            TerminalNode(id="end", artifacts=["flag"], result={"status": "ok"}),
        ]
        session = Session(id="s", pipeline=Pipeline(entry="flaky", nodes=nodes))
        result = await session.run()
        self.assertIs(result.artifacts["flag"], True)
        self.assertEqual(attempts["n"], 3)


_SCRIPT: dict = {"errors": [], "runs": 0}


@register_stage("ScriptedBoomStage")
class ScriptedBoomStage(BaseStage):
    async def run(self):
        index = _SCRIPT["runs"]
        _SCRIPT["runs"] += 1
        errors = _SCRIPT["errors"]
        raise errors[index] if index < len(errors) else RuntimeError("сценарий кончился")


class RetryBudgetTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _pipeline(retry: list[Retrier]) -> Pipeline:
        return Pipeline(entry="boom", nodes=[
            StageNode(id="boom", stage="ScriptedBoomStage", retry=retry, next="end"),
            TerminalNode(id="end", result={"status": "ok"}),
        ])

    @staticmethod
    def _scripted(errors: list[Exception]) -> dict:
        _SCRIPT["errors"] = errors
        _SCRIPT["runs"] = 0
        return _SCRIPT

    async def test_max_attempts_counts_the_first_run(self):
        state = self._scripted([TimeoutError("boom")] * 10)
        retry = [Retrier(error_equals=["TimeoutError"], max_attempts=3, interval_seconds=0)]
        with self.assertRaises(TimeoutError):
            await Session(id="s", pipeline=self._pipeline(retry)).run()
        self.assertEqual(state["runs"], 3)

    async def test_single_attempt_means_no_retry(self):
        state = self._scripted([TimeoutError("boom")] * 10)
        retry = [Retrier(error_equals=["TimeoutError"], max_attempts=1, interval_seconds=0)]
        with self.assertRaises(TimeoutError):
            await Session(id="s", pipeline=self._pipeline(retry)).run()
        self.assertEqual(state["runs"], 1)

    async def test_each_retrier_spends_its_own_budget(self):
        state = self._scripted([
            TimeoutError("раз"), TimeoutError("два"),
            ValueError("три"), ValueError("четыре"),
        ])
        retry = [
            Retrier(error_equals=["TimeoutError"], max_attempts=5, interval_seconds=0),
            Retrier(error_equals=["ValueError"], max_attempts=2, interval_seconds=0),
        ]
        with self.assertRaises(ValueError):
            await Session(id="s", pipeline=self._pipeline(retry)).run()
        self.assertEqual(state["runs"], 4)


class ErrorMessageTests(unittest.TestCase):
    def test_message_is_not_wrapped_in_quotes(self):
        for cls in (StageOutputError, ArtifactNotFoundError):
            with self.subTest(cls=cls.__name__):
                message = "стадия не вернула поле 'x' (есть: ['y'])"
                self.assertEqual(str(cls(message)), message)

    def test_builtin_type_is_still_catchable(self):
        with self.assertRaises(KeyError):
            raise StageOutputError("нет поля")


class ContextTests(unittest.TestCase):
    def test_frame_is_immutable(self):
        ctx = Context(vars={"a": 1})
        other = ctx.with_var("b", 2)
        self.assertIsNone(ctx.get_var("b"))
        self.assertEqual(other.get_var("b"), 2)
        self.assertEqual(other.get_var("a"), 1)

    def test_without_var(self):
        ctx = Context(vars={"secret": "x", "keep": 1})
        dropped = ctx.without_var("secret")
        self.assertEqual(ctx.get_var("secret"), "x")
        self.assertIsNone(dropped.get_var("secret"))
        self.assertEqual(dropped.get_var("keep"), 1)

    def test_same_frame_can_be_shared_by_concurrent_branches(self):
        base = Context(vars={"shared": 0})
        left = base.with_var("only_left", "L")
        right = base.with_var("only_right", "R")
        self.assertIsNone(left.get_var("only_right"))
        self.assertIsNone(right.get_var("only_left"))
        self.assertEqual(base.var_names(), frozenset({"shared"}))

    def test_roundtrip_dict(self):
        ctx = Context(vars={"a": 1})
        restored = Context.from_dict(ctx.to_dict())
        self.assertEqual(restored.get_var("a"), 1)
        self.assertEqual(ctx.to_dict(), {"vars": {"a": 1}})


class WaitInputBroadcastTests(unittest.IsolatedAsyncioTestCase):
    async def test_wait_input_multiple(self):
        pipeline = Pipeline(entry="end", nodes=[TerminalNode(id="end", result={"status": "done"})])
        session = Session(id="s", pipeline=pipeline, context=Context())

        async def waiter():
            return await session.wait_input("ping")

        task1 = asyncio.create_task(waiter())
        task2 = asyncio.create_task(waiter())
        await asyncio.sleep(0)
        await session.input("ping", {"msg": "hello"})
        res1, res2 = await asyncio.wait_for(asyncio.gather(task1, task2), timeout=1.0)
        self.assertEqual(res1["payload"]["msg"], "hello")
        self.assertEqual(res2["payload"]["msg"], "hello")

class OutputFieldValidationTests(unittest.TestCase):
    @staticmethod
    def _errors(outputs, stage="SetValueStage"):
        return Pipeline.from_dict({
            "entry": "s",
            "nodes": [
                {"id": "s", "type": "stage", "stage": stage,
                 "arguments": {"const": {"value": 1}}, "outputs": outputs, "next": "end"},
                {"id": "end", "type": "terminal", "result": {"status": "ok"}},
            ],
        }).collect_errors()

    def test_unknown_output_field_rejected(self):
        errors = self._errors({"value": "n", "meta": "m"})
        self.assertTrue(any("не возвращает поле 'meta'" in e for e in errors), errors)

    def test_declared_field_passes(self):
        self.assertEqual(self._errors({"value": "n"}), [])

    def test_subset_of_declared_fields_passes(self):
        self.assertEqual(self._errors({"list": "l"}, stage="PopListStage"), [])

    def test_computed_output_is_not_a_result_field(self):
        self.assertEqual(self._errors({"value": "n", "rep.$": "vars.n + 1"}), [])

    def test_stage_without_declared_outputs_is_not_checked(self):
        self.assertEqual(self._errors({"whatever": "x"}, stage="EchoStage"), [])

    def test_runtime_error_matches_the_static_one(self):
        pipeline = Pipeline(entry="s", nodes=[
            StageNode(id="s", stage="EchoStage", arguments={"const": {"value": 1}},
                      outputs={"nope": "x"}, next="end"),
            TerminalNode(id="end", result={"status": "ok"}),
        ])
        from stageflow.exceptions import StageOutputError
        with self.assertRaises(StageOutputError) as caught:
            asyncio.run(Session(id="t", pipeline=pipeline, context=Context()).run())
        self.assertIn("nope", str(caught.exception))

class OutputsAreSimultaneousTests(unittest.TestCase):
    @staticmethod
    def _run(outputs):
        pipeline = Pipeline.from_dict({
            "entry": "s",
            "nodes": [
                {"id": "s", "type": "stage", "stage": "SetValueStage",
                 "arguments": {"const": {"value": 5}},
                 "outputs": outputs, "next": "end"},
                {"id": "end", "type": "terminal", "result": {"status": "ok"},
                 "artifacts": ["n", "echo"]},
            ],
        })
        session = Session(id="t", pipeline=pipeline, context=Context(vars={"n": 111}))
        return asyncio.run(session.run()).artifacts

    def test_expression_sees_frame_before_the_node(self):
        self.assertEqual(self._run({"value": "n", "echo.$": "vars.n"}),
                         {"n": 5, "echo": 111})

    def test_key_order_does_not_change_the_result(self):
        self.assertEqual(self._run({"value": "n", "echo.$": "vars.n"}),
                         self._run({"echo.$": "vars.n", "value": "n"}))

    def test_expression_still_sees_the_stage_result(self):
        self.assertEqual(self._run({"value": "n", "echo.$": "output.value"}),
                         {"n": 5, "echo": 5})


if __name__ == "__main__":
    unittest.main()
