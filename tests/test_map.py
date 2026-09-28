import asyncio
import unittest

from stageflow import BaseStage, Context, MapNode, Pipeline, Session, register_stage
from stageflow.exceptions import (
    PipelineDefinitionError,
    PipelineValidationError,
    StageOutputError,
    TypeCheckError,
)


@register_stage("ShoutStage")
class ShoutStage(BaseStage):
    """Upper-case the word it is given.

    arguments:
      word (str): the word
    outputs:
      loud (str): the word, louder
    """

    async def run(self):
        self.set_outputs({"loud": str(self.get_arguments()["word"]).upper()})


@register_stage("MapPaceStage")
class MapPaceStage(BaseStage):
    """Sleep, then say when it woke up.

    arguments:
      word (str): the word
      pause (float): seconds to sleep
    outputs:
      loud (str): the word
      order (int): position among the finished iterations
    """

    counter = 0

    async def run(self):
        args = self.get_arguments()
        await asyncio.sleep(float(args.get("pause", 0)))
        MapPaceStage.counter += 1
        self.set_outputs({"loud": str(args["word"]).upper(), "order": MapPaceStage.counter})


@register_stage("BoomOnStage")
class BoomOnStage(BaseStage):
    """Fail on one particular word, pass everything else through.

    arguments:
      word (str): the word
      bad (str): the word to fail on
    outputs:
      loud (str): the word
    """

    seen = []

    async def run(self):
        args = self.get_arguments()
        BoomOnStage.seen.append(args["word"])
        if args["word"] == args.get("bad"):
            raise ValueError(f"cannot handle {args['word']}")
        self.set_outputs({"loud": str(args["word"]).upper()})


def _loop_pipeline(**node_fields):
    """A map over vars.words whose body shouts each word."""
    node = {
        "id": "loop",
        "type": "map",
        "items": "vars.words",
        "body": "shout",
        "item_var": "word",
        "collect": {"loud": "shouted"},
        "next": "done",
    }
    node.update(node_fields)
    collected = node.get("collect") or {}
    artifacts = sorted(collected if isinstance(collected, list) else collected.values())
    return {
        "entry": "start",
        "nodes": [
            {"id": "start", "type": "entry", "next": "loop"},
            node,
            {"id": "shout", "type": "stage", "stage": "ShoutStage",
             "arguments": {"vars": {"word": "word"}}, "outputs": {"loud": "loud"}},
            {"id": "done", "type": "terminal", "result": {"status": "ok"},
             "artifacts": artifacts},
        ],
    }


class MapNodeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        MapPaceStage.counter = 0
        BoomOnStage.seen = []

    async def _run(self, data, variables=None):
        session = Session(
            id="map", pipeline=Pipeline.from_dict(data), context=Context(vars=variables or {})
        )
        return await session.run(), session

    async def test_collects_one_entry_per_item_in_order(self):
        result, session = await self._run(_loop_pipeline(), {"words": ["a", "b", "c"]})
        self.assertEqual(result.artifacts["shouted"], ["A", "B", "C"])
        self.assertIn("map_completed", [e.type for e in session.event_history])

    async def test_empty_list_collects_nothing_and_goes_on(self):
        result, _ = await self._run(_loop_pipeline(), {"words": []})
        self.assertEqual(result.result, {"status": "ok"})
        self.assertEqual(result.artifacts["shouted"], [])

    async def test_index_var_counts_from_zero(self):
        data = _loop_pipeline(index_var="i", collect={"loud": "shouted", "i": "seen_at"})
        result, _ = await self._run(data, {"words": ["a", "b", "c"]})
        self.assertEqual(result.artifacts["seen_at"], [0, 1, 2])

    async def test_iterations_do_not_see_each_others_writes(self):
        """Each iteration gets its own copy of the frame.

        `keep` is written inside the body on every pass. If the frames leaked
        into one another, the second pass would find the first one's value.
        """
        data = {
            "entry": "start",
            "nodes": [
                {"id": "start", "type": "entry", "variables": {"words": ["a", "b"]},
                 "next": "loop"},
                {"id": "loop", "type": "map", "items": "vars.words", "body": "remember",
                 "item_var": "word", "collect": {"saw": "saw_each"}, "next": "done"},
                {"id": "remember", "type": "stage", "stage": "SetValueStage",
                 "arguments": {"const": {"value": "written"}}, "outputs": {"value": "keep"},
                 "next": "look"},
                {"id": "look", "type": "stage", "stage": "ConcatStage",
                 "arguments": {"vars": {"parts": "words"}, "const": {"separator": "+"}},
                 "outputs": {"value": "saw"}},
                {"id": "done", "type": "terminal", "result": {"status": "ok"},
                 "artifacts": ["saw_each"]},
            ],
        }
        result, session = await self._run(data)
        self.assertEqual(result.artifacts["saw_each"], ["a+b", "a+b"])
        # 'keep' was written inside every iteration and stayed there
        self.assertNotIn("keep", session.context.var_names())

    async def test_only_collected_names_leave_the_loop(self):
        result, session = await self._run(_loop_pipeline(), {"words": ["a"]})
        self.assertEqual(result.artifacts["shouted"], ["A"])
        self.assertNotIn("loud", session.context.var_names())
        self.assertNotIn("word", session.context.var_names())

    async def test_parallel_mode_keeps_item_order_not_finish_order(self):
        data = {
            "entry": "start",
            "nodes": [
                {"id": "start", "type": "entry", "next": "loop"},
                {"id": "loop", "type": "map", "items": "vars.words", "body": "slow",
                 "item_var": "word", "index_var": "i", "mode": "parallel",
                 "collect": {"loud": "shouted", "order": "finished"}, "next": "done"},
                {"id": "slow", "type": "stage", "stage": "MapPaceStage",
                 "arguments": {"vars": {"word": "word"},
                               "const": {"pause.$": "double(3 - vars.i) * 0.02"}},
                 "outputs": {"loud": "loud", "order": "order"}},
                {"id": "done", "type": "terminal", "result": {"status": "ok"},
                 "artifacts": ["shouted", "finished"]},
            ],
        }
        # the first item sleeps the longest, so the iterations finish backwards
        result, _ = await self._run(data, {"words": ["a", "b", "c"]})
        self.assertEqual(result.artifacts["finished"], [3, 2, 1])
        self.assertEqual(result.artifacts["shouted"], ["A", "B", "C"])

    async def test_parallel_mode_runs_items_concurrently(self):
        data = {
            "entry": "start",
            "nodes": [
                {"id": "start", "type": "entry", "next": "loop"},
                {"id": "loop", "type": "map", "items": "vars.words", "body": "slow",
                 "item_var": "word", "mode": "parallel",
                 "collect": {"loud": "shouted"}, "next": "done"},
                {"id": "slow", "type": "stage", "stage": "MapPaceStage",
                 "arguments": {"vars": {"word": "word"}, "const": {"pause": 0.05}},
                 "outputs": {"loud": "loud"}},
                {"id": "done", "type": "terminal", "result": {"status": "ok"},
                 "artifacts": ["shouted"]},
            ],
        }
        started = asyncio.get_running_loop().time()
        result, _ = await self._run(data, {"words": ["a", "b", "c", "d"]})
        elapsed = asyncio.get_running_loop().time() - started
        self.assertEqual(result.artifacts["shouted"], ["A", "B", "C", "D"])
        self.assertLess(elapsed, 0.15, "four 50ms iterations should overlap")

    async def test_first_failure_stops_the_rest(self):
        data = _loop_pipeline(collect={"loud": "shouted"})
        data["nodes"][2] = {
            "id": "shout", "type": "stage", "stage": "BoomOnStage",
            "arguments": {"vars": {"word": "word"}, "const": {"bad": "b"}},
            "outputs": {"loud": "loud"},
        }
        with self.assertRaises(ValueError):
            await self._run(data, {"words": ["a", "b", "c"]})
        self.assertEqual(BoomOnStage.seen, ["a", "b"])

    async def test_cancel_on_error_false_runs_every_item_then_raises(self):
        data = _loop_pipeline(collect={"loud": "shouted"}, cancel_on_error=False)
        data["nodes"][2] = {
            "id": "shout", "type": "stage", "stage": "BoomOnStage",
            "arguments": {"vars": {"word": "word"}, "const": {"bad": "b"}},
            "outputs": {"loud": "loud"},
        }
        with self.assertRaises(ValueError):
            await self._run(data, {"words": ["a", "b", "c"]})
        self.assertEqual(BoomOnStage.seen, ["a", "b", "c"])

    async def test_failure_is_raised_unchanged_so_try_can_catch_it(self):
        """The loop does not wrap the error: a `try` matching on the real
        error type keeps working around a map."""
        data = {
            "entry": "start",
            "nodes": [
                {"id": "start", "type": "entry", "next": "guard"},
                {"id": "guard", "type": "try", "body": "loop", "next": "done",
                 "except": [{"error_equals": ["ValueError"], "next": "rescue",
                             "result_var": "err"}]},
                {"id": "loop", "type": "map", "items": "vars.words", "body": "shout",
                 "item_var": "word", "collect": {"loud": "shouted"}},
                {"id": "shout", "type": "stage", "stage": "BoomOnStage",
                 "arguments": {"vars": {"word": "word"}, "const": {"bad": "b"}},
                 "outputs": {"loud": "loud"}},
                {"id": "rescue", "type": "terminal", "result": {"status": "rescued"},
                 "artifacts": ["err"]},
                {"id": "done", "type": "terminal", "result": {"status": "ok"}},
            ],
        }
        result, session = await self._run(data, {"words": ["a", "b"]})
        self.assertEqual(result.result, {"status": "rescued"})
        self.assertEqual(result.artifacts["err"]["type"], "ValueError")
        failed = [e for e in session.event_history if e.type == "map_item_failed"]
        self.assertEqual([e.payload["index"] for e in failed], [1])

    async def test_retry_on_the_map_node_repeats_the_whole_loop(self):
        data = _loop_pipeline(
            collect={"loud": "shouted"},
            retry=[{"error_equals": ["ValueError"], "max_attempts": 2,
                    "interval_seconds": 0}],
        )
        data["nodes"][2] = {
            "id": "shout", "type": "stage", "stage": "BoomOnStage",
            "arguments": {"vars": {"word": "word"}, "const": {"bad": "b"}},
            "outputs": {"loud": "loud"},
        }
        with self.assertRaises(ValueError):
            await self._run(data, {"words": ["a", "b"]})
        self.assertEqual(BoomOnStage.seen, ["a", "b", "a", "b"])

    async def test_item_that_writes_nothing_is_an_error(self):
        data = _loop_pipeline(collect={"missing": "gathered"})
        with self.assertRaises(StageOutputError) as caught:
            await self._run(data, {"words": ["a"]})
        self.assertIn("did not write 'missing'", str(caught.exception))

    async def test_items_must_evaluate_to_a_list(self):
        with self.assertRaises(TypeCheckError) as caught:
            await self._run(_loop_pipeline(), {"words": {"not": "a list"}})
        self.assertIn("must be a list", str(caught.exception))

    async def test_terminal_inside_the_body_ends_the_whole_run(self):
        data = {
            "entry": "start",
            "nodes": [
                {"id": "start", "type": "entry", "next": "loop"},
                {"id": "loop", "type": "map", "items": "vars.words", "body": "check",
                 "item_var": "word", "next": "done"},
                {"id": "check", "type": "condition", "condition": "vars.word == 'stop'",
                 "then": "halt", "else": "shout"},
                {"id": "halt", "type": "terminal", "result": {"status": "halted"}},
                {"id": "shout", "type": "stage", "stage": "ShoutStage",
                 "arguments": {"vars": {"word": "word"}}, "outputs": {"loud": "loud"}},
                {"id": "done", "type": "terminal", "result": {"status": "ok"}},
            ],
        }
        result, _ = await self._run(data, {"words": ["a", "stop", "c"]})
        self.assertEqual(result.result, {"status": "halted"})

    async def test_typed_collect_is_checked_on_the_way_out(self):
        data = _loop_pipeline()
        data["types"] = {}
        data["variables"] = {"shouted": "list<int>"}
        with self.assertRaises(TypeCheckError):
            await self._run(data, {"words": ["a"]})

    async def test_nested_maps_walk_a_list_of_lists(self):
        data = {
            "entry": "start",
            "nodes": [
                {"id": "start", "type": "entry", "next": "outer"},
                {"id": "outer", "type": "map", "items": "vars.groups", "body": "inner",
                 "item_var": "group", "collect": {"shouted": "per_group"}, "next": "done"},
                {"id": "inner", "type": "map", "items": "vars.group", "body": "shout",
                 "item_var": "word", "collect": {"loud": "shouted"}},
                {"id": "shout", "type": "stage", "stage": "ShoutStage",
                 "arguments": {"vars": {"word": "word"}}, "outputs": {"loud": "loud"}},
                {"id": "done", "type": "terminal", "result": {"status": "ok"},
                 "artifacts": ["per_group"]},
            ],
        }
        result, _ = await self._run(data, {"groups": [["a", "b"], ["c"]]})
        self.assertEqual(result.artifacts["per_group"], [["A", "B"], ["C"]])


class MapValidationTests(unittest.TestCase):
    def _errors(self, data):
        return Pipeline.from_dict(data).collect_errors()

    def test_items_and_body_are_required(self):
        for missing in ("items", "body"):
            data = _loop_pipeline()
            del data["nodes"][1][missing]
            with self.assertRaises(PipelineDefinitionError):
                Pipeline.from_dict(data)

    def test_body_must_exist(self):
        data = _loop_pipeline(body="nowhere")
        self.assertIn(
            "loop: body 'nowhere' not found in the graph", self._errors(data)
        )

    def test_a_road_out_of_the_body_is_rejected(self):
        """The body must be closed: leaving it would abandon the remaining
        items, and in parallel mode there would be no frame to continue with."""
        data = _loop_pipeline()
        data["nodes"][2]["next"] = "done"
        errors = self._errors(data)
        self.assertTrue(
            any("outside the loop body" in e for e in errors), errors
        )

    def test_jumping_back_into_the_map_node_is_rejected(self):
        data = _loop_pipeline()
        data["nodes"][2]["next"] = "loop"
        errors = self._errors(data)
        self.assertTrue(any("outside the loop body" in e for e in errors), errors)

    def test_unknown_mode_is_rejected_by_the_schema(self):
        data = _loop_pipeline(mode="whenever")
        with self.assertRaises(PipelineDefinitionError) as caught:
            Pipeline.from_dict(data)
        self.assertIn("not one of ['sequential', 'parallel']", str(caught.exception))

    def test_unknown_mode_is_rejected_for_a_node_built_in_python(self):
        """The schema guards JSON; validate() guards the Python API."""
        pipeline = Pipeline.from_dict(_loop_pipeline())
        node = MapNode(id="loop", items="vars.words", body="shout", mode="whenever")
        self.assertIn(
            "loop: mode 'whenever' is not one of sequential, parallel",
            node.validate(pipeline),
        )

    def test_item_var_must_be_a_name(self):
        data = _loop_pipeline(item_var="not a name")
        self.assertIn(
            "loop: item_var 'not a name' is not a valid variable name", self._errors(data)
        )

    def test_item_var_and_index_var_must_differ(self):
        data = _loop_pipeline(item_var="x", index_var="x")
        self.assertIn("loop: item_var and index_var are the same name", self._errors(data))

    def test_collect_names_must_be_names(self):
        data = _loop_pipeline(collect={"loud": "not a name"})
        self.assertIn(
            "loop: collect destination 'not a name' must be a variable name",
            self._errors(data),
        )

    def test_collect_accepts_a_list_as_shorthand(self):
        data = _loop_pipeline(collect=["loud"])
        self.assertEqual(self._errors(data), [])
        node = Pipeline.from_dict(data).get_node("loop")
        self.assertEqual(node.collect, {"loud": "loud"})

    def test_validate_raises_on_a_broken_loop(self):
        data = _loop_pipeline(body="nowhere")
        with self.assertRaises(PipelineValidationError):
            Pipeline.from_dict(data).validate()

    def test_scope_is_the_region_between_body_and_next(self):
        data = _loop_pipeline()
        data["nodes"][2]["next"] = "twice"
        data["nodes"].append(
            {"id": "twice", "type": "stage", "stage": "ShoutStage",
             "arguments": {"vars": {"word": "loud"}}, "outputs": {"loud": "loud"}}
        )
        pipeline = Pipeline.from_dict(data)
        self.assertEqual(pipeline.get_node("loop").scope(pipeline), frozenset({"shout", "twice"}))
        self.assertEqual(self._errors(data), [])


if __name__ == "__main__":
    unittest.main()
