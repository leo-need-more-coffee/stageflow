import unittest

from stageflow import (
    BaseStage,
    Context,
    ExceptHandler,
    Pipeline,
    Session,
    StageNode,
    TerminalNode,
    TryNode,
    register_stage,
)
from stageflow.exceptions import PipelineValidationError


@register_stage("BoomStage")
class BoomStage(BaseStage):
    async def run(self):
        args = self.get_arguments()
        kind = args.get("kind", "runtime")
        message = args.get("message", "boom")
        if kind == "value":
            raise ValueError(message)
        if kind == "timeout":
            raise TimeoutError(message)
        raise RuntimeError(message)


@register_stage("MarkStage")
class MarkStage(BaseStage):
    async def run(self):
        self.set_outputs({"mark": self.get_arguments().get("mark", "?")})


def _run(pipeline_data, variables=None):
    import asyncio

    pipeline = Pipeline.from_dict(pipeline_data)
    session = Session(id="try", pipeline=pipeline, context=Context(vars=variables or {}))
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(session.run())


class TryBlockTests(unittest.IsolatedAsyncioTestCase):
    async def _run(self, data, variables=None):
        session = Session(
            id="try", pipeline=Pipeline.from_dict(data), context=Context(vars=variables or {})
        )
        return await session.run(), session

    async def test_catches_error_from_deep_inside_body(self):
        data = {
            "entry": "guard",
            "nodes": [
                {"id": "guard", "type": "try", "body": "step1", "next": "done",
                 "except": [{"error_equals": ["*"], "next": "handler", "result_var": "err"}]},
                {"id": "step1", "type": "stage", "stage": "MarkStage",
                 "arguments": {"const": {"mark": "one"}}, "outputs": {"mark": "m1"},
                 "next": "step2"},
                {"id": "step2", "type": "stage", "stage": "MarkStage",
                 "arguments": {"const": {"mark": "two"}}, "outputs": {"mark": "m2"},
                 "next": "step3"},
                {"id": "step3", "type": "stage", "stage": "BoomStage",
                 "arguments": {"const": {"message": "deep failure"}}},
                {"id": "handler", "type": "terminal", "result": {"status": "handled"},
                 "artifacts": ["m1", "m2", "err"]},
                {"id": "done", "type": "terminal", "result": {"status": "ok"}},
            ],
        }
        result, _ = await self._run(data)
        self.assertEqual(result.result, {"status": "handled"})
        self.assertEqual(result.artifacts["m1"], "one")
        self.assertEqual(result.artifacts["m2"], "two")
        self.assertEqual(result.artifacts["err"]["message"], "deep failure")
        self.assertEqual(result.artifacts["err"]["type"], "RuntimeError")

    async def test_handler_selected_by_error_type(self):
        data = {
            "entry": "guard",
            "nodes": [
                {"id": "guard", "type": "try", "body": "boom", "next": "done",
                 "except": [
                     {"error_equals": ["ValueError"], "next": "on_value"},
                     {"error_equals": ["TimeoutError"], "next": "on_timeout"},
                     {"error_equals": ["*"], "next": "on_any"},
                 ]},
                {"id": "boom", "type": "stage", "stage": "BoomStage",
                 "arguments": {"const": {"kind": "timeout"}}},
                {"id": "on_value", "type": "terminal", "result": {"picked": "value"}},
                {"id": "on_timeout", "type": "terminal", "result": {"picked": "timeout"}},
                {"id": "on_any", "type": "terminal", "result": {"picked": "any"}},
                {"id": "done", "type": "terminal", "result": {"picked": "none"}},
            ],
        }
        result, _ = await self._run(data)
        self.assertEqual(result.result, {"picked": "timeout"})

    async def test_body_without_error_goes_to_next(self):
        data = {
            "entry": "guard",
            "nodes": [
                {"id": "guard", "type": "try", "body": "ok", "next": "after",
                 "except": [{"error_equals": ["*"], "next": "handler"}]},
                {"id": "ok", "type": "stage", "stage": "MarkStage", "arguments": {"const": {"mark": "fine"}},
                 "outputs": {"mark": "m"}},
                {"id": "after", "type": "terminal", "result": {"status": "after"},
                 "artifacts": ["m"]},
                {"id": "handler", "type": "terminal", "result": {"status": "handled"}},
            ],
        }
        result, session = await self._run(data)
        self.assertEqual(result.result, {"status": "after"})
        self.assertEqual(result.artifacts["m"], "fine")
        self.assertIn("try_completed", [e.type for e in session.event_history])

    async def test_handler_road_without_next_continues_after_the_block(self):
        data = {
            "entry": "guard",
            "nodes": [
                {"id": "guard", "type": "try", "body": "boom", "next": "after",
                 "except": [{"error_equals": ["*"], "next": "handler", "result_var": "err"}]},
                {"id": "boom", "type": "stage", "stage": "BoomStage",
                 "arguments": {"const": {"message": "fell over"}}},
                {"id": "handler", "type": "stage", "stage": "MarkStage",
                 "arguments": {"const": {"mark": "recovered"}}, "outputs": {"mark": "m"}},
                {"id": "after", "type": "terminal", "result": {"status": "after"},
                 "artifacts": ["m", "err"]},
            ],
        }
        result, session = await self._run(data)
        self.assertEqual(result.result, {"status": "after"})
        self.assertEqual(result.artifacts["m"], "recovered")
        self.assertEqual(result.artifacts["err"]["message"], "fell over")
        self.assertIn("try_completed", [e.type for e in session.event_history])

    async def test_terminal_inside_the_block_ends_the_run(self):
        data = {
            "entry": "guard",
            "nodes": [
                {"id": "guard", "type": "try", "body": "work", "next": "after",
                 "except": [{"error_equals": ["*"], "next": "handler"}]},
                {"id": "work", "type": "stage", "stage": "MarkStage",
                 "arguments": {"const": {"mark": "inside"}}, "outputs": {"mark": "m"},
                 "next": "stop_here"},
                {"id": "stop_here", "type": "terminal", "result": {"status": "stopped inside"},
                 "artifacts": ["m"]},
                {"id": "handler", "type": "terminal", "result": {"status": "handled"}},
                {"id": "after", "type": "terminal", "result": {"status": "after"}},
            ],
        }
        result, _ = await self._run(data)
        self.assertEqual(result.result, {"status": "stopped inside"})
        self.assertEqual(result.artifacts["m"], "inside")

    async def test_terminal_on_a_handler_road_ends_the_run(self):
        data = {
            "entry": "guard",
            "nodes": [
                {"id": "guard", "type": "try", "body": "boom", "next": "after",
                 "except": [{"error_equals": ["*"], "next": "handler"}]},
                {"id": "boom", "type": "stage", "stage": "BoomStage", "arguments": {}},
                {"id": "handler", "type": "stage", "stage": "MarkStage",
                 "arguments": {"const": {"mark": "recovered"}}, "outputs": {"mark": "m"},
                 "next": "handled"},
                {"id": "handled", "type": "terminal", "result": {"status": "handled"},
                 "artifacts": ["m"]},
                {"id": "after", "type": "terminal", "result": {"status": "after"}},
            ],
        }
        result, _ = await self._run(data)
        self.assertEqual(result.result, {"status": "handled"})
        self.assertEqual(result.artifacts["m"], "recovered")

    async def test_nested_try_inner_handles_first(self):
        data = {
            "entry": "outer",
            "nodes": [
                {"id": "outer", "type": "try", "body": "inner", "next": "done",
                 "except": [{"error_equals": ["*"], "next": "outer_handler"}]},
                {"id": "inner", "type": "try", "body": "boom", "next": "after_inner",
                 "except": [{"error_equals": ["ValueError"], "next": "inner_handler"}]},
                {"id": "boom", "type": "stage", "stage": "BoomStage",
                 "arguments": {"const": {"kind": "value"}}},
                {"id": "inner_handler", "type": "stage", "stage": "MarkStage",
                 "arguments": {"const": {"mark": "inner"}}, "outputs": {"mark": "who"},
                 "next": "done"},
                {"id": "after_inner", "type": "terminal", "result": {"status": "no_error"}},
                {"id": "outer_handler", "type": "terminal", "result": {"status": "outer"}},
                {"id": "done", "type": "terminal", "result": {"status": "done"},
                 "artifacts": ["who"]},
            ],
        }
        result, _ = await self._run(data)
        self.assertEqual(result.result, {"status": "done"})
        self.assertEqual(result.artifacts["who"], "inner")

    async def test_unmatched_error_bubbles_to_outer_try(self):
        data = {
            "entry": "outer",
            "nodes": [
                {"id": "outer", "type": "try", "body": "inner", "next": "done",
                 "except": [{"error_equals": ["RuntimeError"], "next": "outer_handler"}]},
                {"id": "inner", "type": "try", "body": "boom", "next": "done",
                 "except": [{"error_equals": ["ValueError"], "next": "inner_handler"}]},
                {"id": "boom", "type": "stage", "stage": "BoomStage",
                 "arguments": {"const": {"kind": "runtime"}}},
                {"id": "inner_handler", "type": "terminal", "result": {"status": "inner"}},
                {"id": "outer_handler", "type": "terminal", "result": {"status": "outer"}},
                {"id": "done", "type": "terminal", "result": {"status": "done"}},
            ],
        }
        result, _ = await self._run(data)
        self.assertEqual(result.result, {"status": "outer"})

    async def test_unhandled_error_propagates_out_of_session(self):
        data = {
            "entry": "guard",
            "nodes": [
                {"id": "guard", "type": "try", "body": "boom", "next": "done",
                 "except": [{"error_equals": ["ValueError"], "next": "handler"}]},
                {"id": "boom", "type": "stage", "stage": "BoomStage",
                 "arguments": {"const": {"kind": "runtime"}}},
                {"id": "handler", "type": "terminal", "result": {"status": "handled"}},
                {"id": "done", "type": "terminal", "result": {"status": "ok"}},
            ],
        }
        with self.assertRaises(RuntimeError):
            await self._run(data)

    async def test_retry_runs_before_block_catches(self):
        attempts = {"n": 0}

        @register_stage("FlakyTwiceStage")
        class FlakyTwiceStage(BaseStage):
            async def run(self):
                attempts["n"] += 1
                if attempts["n"] < 3:
                    raise TimeoutError("not yet")
                self.set_outputs({"mark": "recovered"})

        data = {
            "entry": "guard",
            "nodes": [
                {"id": "guard", "type": "try", "body": "flaky", "next": "done",
                 "except": [{"error_equals": ["*"], "next": "handler"}]},
                {"id": "flaky", "type": "stage", "stage": "FlakyTwiceStage",
                 "outputs": {"mark": "m"},
                 "retry": [{"error_equals": ["TimeoutError"], "max_attempts": 5,
                            "interval_seconds": 0.001}]},
                {"id": "handler", "type": "terminal", "result": {"status": "handled"}},
                {"id": "done", "type": "terminal", "result": {"status": "ok"},
                 "artifacts": ["m"]},
            ],
        }
        result, _ = await self._run(data)
        self.assertEqual(result.result, {"status": "ok"})
        self.assertEqual(result.artifacts["m"], "recovered")
        self.assertEqual(attempts["n"], 3)


class TryScopeTests(unittest.TestCase):
    def _pipeline(self, nodes, entry="guard"):
        return Pipeline.from_dict({"entry": entry, "nodes": nodes})

    def test_scope_excludes_everything_after_next(self):
        pipeline = self._pipeline([
            {"id": "guard", "type": "try", "body": "a", "next": "after",
             "except": [{"error_equals": ["*"], "next": "handler"}]},
            {"id": "a", "type": "stage", "stage": "MarkStage", "next": "b"},
            {"id": "b", "type": "stage", "stage": "MarkStage", "next": "after"},
            {"id": "after", "type": "stage", "stage": "MarkStage", "next": "tail"},
            {"id": "tail", "type": "terminal"},
            {"id": "handler", "type": "terminal"},
        ])
        scope = pipeline.get_node("guard").scope(pipeline)
        self.assertEqual(scope, frozenset({"a", "b"}))

    def test_handler_inside_body_is_rejected(self):
        pipeline = self._pipeline([
            {"id": "guard", "type": "try", "body": "a", "next": "after",
             "except": [{"error_equals": ["*"], "next": "b"}]},
            {"id": "a", "type": "stage", "stage": "MarkStage", "next": "b"},
            {"id": "b", "type": "stage", "stage": "MarkStage"},
            {"id": "after", "type": "terminal"},
        ])
        with self.assertRaises(PipelineValidationError) as caught:
            pipeline.validate()
        self.assertIn("inside the block body", str(caught.exception))

    def test_missing_body_and_empty_except_are_reported(self):
        with self.assertRaises(ValueError):
            Pipeline.from_dict({"entry": "guard", "nodes": [
                {"id": "guard", "type": "try", "next": "after",
                 "except": [{"error_equals": ["*"], "next": "after"}]},
                {"id": "after", "type": "terminal"},
            ]})

        pipeline = Pipeline(entry="guard", nodes=[
            TryNode(id="guard", body="a", handlers=[], next="after"),
            StageNode(id="a", stage="MarkStage"),
            TerminalNode(id="after"),
        ])
        errors = pipeline.collect_errors()
        self.assertTrue(any("at least one handler" in e for e in errors))

    def test_handler_object_matches_by_full_name(self):
        handler = ExceptHandler(error_equals=["builtins.TimeoutError"], next="h")
        self.assertTrue(handler.matches(TimeoutError("x")))
        self.assertFalse(handler.matches(ValueError("x")))


if __name__ == "__main__":
    unittest.main()
