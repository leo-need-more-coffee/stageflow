import asyncio
import unittest

from stageflow import (
    BaseStage,
    Budget,
    BudgetExceeded,
    Context,
    Limits,
    Pipeline,
    Session,
    register_stage,
)


@register_stage("SpenderStage")
class SpenderStage(BaseStage):
    """
    description: "Spends what it is told to"
    reserve:
      tokens: "args.budget_hint"
      llm_calls: 1
    outputs:
      value: {type: string}
    """

    ran = 0

    async def run(self):
        SpenderStage.ran += 1
        args = self.get_arguments()
        self.set_outputs({"value": "spent"})
        self.charge(tokens=args.get("really", args.get("budget_hint", 0)), llm_calls=1)


@register_stage("FreeStage")
class FreeStage(BaseStage):
    """A stage that declares nothing at all and is therefore free."""

    async def run(self):
        self.set_outputs({"value": "free"})


@register_stage("SlowlyStage")
class SlowlyStage(BaseStage):
    """
    description: "Sleeps"
    arguments:
      seconds: {type: number}
    """

    timeout = 30

    async def run(self):
        await asyncio.sleep(float(self.get_arguments().get("seconds", 0)))


def _chain(*nodes):
    return {"entry": "start", "nodes": [
        {"id": "start", "type": "entry", "next": nodes[0]["id"]},
        *nodes,
        {"id": "done", "type": "terminal", "result": {"status": "ok"}},
    ]}


def _spend(node_id, hint, really=None, nxt="done"):
    const = {"budget_hint": hint} | ({"really": really} if really is not None else {})
    return {"id": node_id, "type": "stage", "stage": "SpenderStage",
            "arguments": {"const": const}, "outputs": {"value": "v"}, "next": nxt}


class MeterTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        SpenderStage.ran = 0

    async def _run(self, data, limits=None, variables=None):
        session = Session(id="b", pipeline=Pipeline.from_dict(data),
                          context=Context(vars=variables or {}), limits=limits)
        return await session.run(), session

    async def test_without_limits_nothing_is_counted_against_anything(self):
        result, _ = await self._run(_chain(_spend("a", 10_000)))
        self.assertEqual(result.result, {"status": "ok"})
        self.assertEqual(result.meters["tokens"], 10_000)

    async def test_the_bill_comes_back_even_when_nothing_is_limited(self):
        """A host with no ceiling still wants to know what it spent."""
        result, _ = await self._run(_chain(_spend("a", 42)))
        self.assertEqual(result.meters["llm_calls"], 1)
        self.assertEqual(result.meters["steps"], 3)  # entry, stage, terminal
        self.assertIn("seconds", result.meters)

    async def test_a_counter_over_its_limit_stops_the_run(self):
        result, _ = await self._run(
            _chain(_spend("a", 60, nxt="b"), _spend("b", 60)),
            Limits(counters={"tokens": 100}),
        )
        self.assertEqual(result.result["status"], "budget_exceeded")
        self.assertEqual(result.result["meter"], "tokens")
        self.assertEqual(result.result["limit"], 100)

    async def test_a_stage_that_cannot_be_paid_for_never_starts(self):
        """The reservation gates entry: an expensive call is better not begun
        than cut off after the money is gone."""
        await self._run(_chain(_spend("a", 500)), Limits(counters={"tokens": 100}))
        self.assertEqual(SpenderStage.ran, 0)

    async def test_charging_replaces_the_reservation_rather_than_adding(self):
        result, _ = await self._run(_chain(_spend("a", 100, really=30)))
        self.assertEqual(result.meters["tokens"], 30)

    async def test_a_stage_that_reserves_nothing_is_free(self):
        data = _chain({"id": "a", "type": "stage", "stage": "FreeStage",
                       "outputs": {"value": "v"}, "next": "done"})
        result, _ = await self._run(data, Limits(counters={"tokens": 0}))
        self.assertEqual(result.result, {"status": "ok"})

    async def test_steps_are_charged_by_the_runtime(self):
        result, _ = await self._run(_chain(_spend("a", 1)), Limits(counters={"steps": 2}))
        self.assertEqual(result.result["meter"], "steps")

    async def test_the_run_ends_with_a_result_rather_than_an_exception(self):
        """Ending is not the same as throwing the work away: the caller gets
        a result, the meters, and the events — not a traceback."""
        data = {"entry": "start", "nodes": [
            {"id": "start", "type": "entry", "variables": {"kept": "yes"}, "next": "a"},
            _spend("a", 10, nxt="b"),
            _spend("b", 10, nxt="done"),
            {"id": "done", "type": "terminal", "result": {}, "artifacts": ["kept"]},
        ]}
        session = Session(id="b", pipeline=Pipeline.from_dict(data), context=Context(),
                          limits=Limits(counters={"tokens": 15}))
        result = await session.run()
        self.assertEqual(result.result["status"], "budget_exceeded")
        # the second stage never ran: 10 spent, and the 10 it would have
        # reserved did not fit under the ceiling of 15
        self.assertEqual(result.meters["tokens"], 10)
        self.assertIn("budget_exceeded", [e.type for e in result.history])


class UncatchableTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_try_block_cannot_swallow_the_budget(self):
        """The load-bearing property. A tenant who could catch this would
        make every limit decoration."""
        data = {"entry": "start", "nodes": [
            {"id": "start", "type": "entry", "next": "guard"},
            {"id": "guard", "type": "try", "body": "a", "next": "done",
             "except": [{"error_equals": ["*"], "next": "rescue"}]},
            _spend("a", 500, nxt=None) | {"next": None},
            {"id": "rescue", "type": "terminal", "result": {"status": "rescued"}},
            {"id": "done", "type": "terminal", "result": {"status": "ok"}},
        ]}
        session = Session(id="t", pipeline=Pipeline.from_dict(data), context=Context(),
                          limits=Limits(counters={"tokens": 10}))
        result = await session.run()
        self.assertEqual(result.result["status"], "budget_exceeded")

    async def test_retry_cannot_grind_through_the_budget(self):
        """run_with_retry catches Exception, and this is not one."""
        data = {"entry": "start", "nodes": [
            {"id": "start", "type": "entry", "next": "a"},
            _spend("a", 500) | {"retry": [{"error_equals": ["*"], "max_attempts": 5,
                                           "interval_seconds": 0}]},
            {"id": "done", "type": "terminal", "result": {"status": "ok"}},
        ]}
        session = Session(id="r", pipeline=Pipeline.from_dict(data), context=Context(),
                          limits=Limits(counters={"tokens": 10}))
        result = await session.run()
        self.assertEqual(result.result["status"], "budget_exceeded")

    async def test_a_parallel_branch_running_out_is_not_a_branch_failure(self):
        """BranchError is catchable; the budget must not be dressed as one."""
        data = {"entry": "start", "nodes": [
            {"id": "start", "type": "entry", "next": "fan"},
            {"id": "fan", "type": "parallel",
             "branches": [{"id": "l", "entry": "a"}, {"id": "r", "entry": "b"}],
             "next": "done"},
            {"id": "a", "type": "stage", "stage": "SpenderStage",
             "arguments": {"const": {"budget_hint": 500}}, "outputs": {"value": "x"}},
            {"id": "b", "type": "stage", "stage": "SpenderStage",
             "arguments": {"const": {"budget_hint": 500}}, "outputs": {"value": "y"}},
            {"id": "done", "type": "terminal", "result": {"status": "ok"}},
        ]}
        session = Session(id="p", pipeline=Pipeline.from_dict(data), context=Context(),
                          limits=Limits(counters={"tokens": 10}))
        result = await session.run()
        self.assertEqual(result.result["status"], "budget_exceeded")

    def test_the_exception_is_deliberately_not_an_exception(self):
        self.assertFalse(issubclass(BudgetExceeded, Exception))
        self.assertTrue(issubclass(BudgetExceeded, BaseException))


class GaugeTests(unittest.IsolatedAsyncioTestCase):
    async def test_concurrency_throttles_a_map_rather_than_failing_it(self):
        """A long list is wide, not wrong: the iterations take turns.

        Failing here would refuse a legitimate pipeline for the size of its
        data, which is the opposite of what the limit is for — the limit is
        on what is in the air at once.
        """
        data = {"entry": "start", "nodes": [
            {"id": "start", "type": "entry", "next": "loop"},
            {"id": "loop", "type": "map", "items": "vars.xs", "body": "work",
             "item_var": "x", "mode": "parallel", "next": "done"},
            {"id": "work", "type": "stage", "stage": "SlowlyStage",
             "arguments": {"const": {"seconds": 0.05}}},
            {"id": "done", "type": "terminal", "result": {"status": "ok"}},
        ]}
        session = Session(id="g", pipeline=Pipeline.from_dict(data),
                          context=Context(vars={"xs": list(range(8))}),
                          limits=Limits(gauges={"concurrency": 4}))
        started = asyncio.get_running_loop().time()
        result = await session.run()
        elapsed = asyncio.get_running_loop().time() - started

        self.assertEqual(result.result, {"status": "ok"})
        self.assertLessEqual(result.meters["peak_concurrency"], 4)
        # eight items of 50ms, four at a time: two rounds, not one and not eight
        self.assertGreater(elapsed, 0.09)
        self.assertLess(elapsed, 0.3)

    async def test_the_peak_is_reported_even_when_nothing_waits(self):
        data = {"entry": "start", "nodes": [
            {"id": "start", "type": "entry", "next": "fan"},
            {"id": "fan", "type": "parallel",
             "branches": [{"id": "l", "entry": "a"}, {"id": "r", "entry": "b"}],
             "next": "done"},
            {"id": "a", "type": "stage", "stage": "SlowlyStage",
             "arguments": {"const": {"seconds": 0.02}}},
            {"id": "b", "type": "stage", "stage": "SlowlyStage",
             "arguments": {"const": {"seconds": 0.02}}},
            {"id": "done", "type": "terminal", "result": {"status": "ok"}},
        ]}
        session = Session(id="pk", pipeline=Pipeline.from_dict(data),
                          context=Context(), limits=Limits(gauges={"concurrency": 8}))
        result = await session.run()
        self.assertEqual(result.meters["peak_concurrency"], 2)

    async def test_iterations_are_a_counter_so_nested_loops_add_up(self):
        data = {"entry": "start", "nodes": [
            {"id": "start", "type": "entry", "next": "outer"},
            {"id": "outer", "type": "map", "items": "vars.rows", "body": "inner",
             "item_var": "row", "next": "done"},
            {"id": "inner", "type": "map", "items": "vars.row", "body": "work",
             "item_var": "cell"},
            {"id": "work", "type": "stage", "stage": "FreeStage",
             "outputs": {"value": "v"}},
            {"id": "done", "type": "terminal", "result": {"status": "ok"}},
        ]}
        rows = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
        session = Session(id="n", pipeline=Pipeline.from_dict(data),
                          context=Context(vars={"rows": rows}),
                          limits=Limits(counters={"iterations": 8}))
        result = await session.run()
        self.assertEqual(result.result["meter"], "iterations")

    async def test_a_frame_that_grows_too_big_is_caught(self):
        data = {"entry": "start", "nodes": [
            {"id": "start", "type": "entry", "next": "a"},
            {"id": "a", "type": "stage", "stage": "SetValueStage",
             "arguments": {"const": {"value": "x" * 5000}},
             "outputs": {"value": "big"}, "next": "done"},
            {"id": "done", "type": "terminal", "result": {"status": "ok"}},
        ]}
        session = Session(id="f", pipeline=Pipeline.from_dict(data), context=Context(),
                          limits=Limits(gauges={"frame_bytes": 1000}))
        result = await session.run()
        self.assertEqual(result.result["meter"], "frame_bytes")

    async def test_the_frame_is_not_measured_when_it_is_not_limited(self):
        session = Session(id="f", pipeline=Pipeline.from_dict(
            _chain({"id": "a", "type": "stage", "stage": "SetValueStage",
                    "arguments": {"const": {"value": "x" * 5000}},
                    "outputs": {"value": "big"}, "next": "done"})),
            context=Context())
        result = await session.run()
        self.assertEqual(result.result, {"status": "ok"})
        self.assertNotIn("peak_frame_bytes", result.meters)


class DeadlineTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_deadline_ends_the_run(self):
        data = _chain({"id": "a", "type": "stage", "stage": "SlowlyStage",
                       "arguments": {"const": {"seconds": 5}}, "next": "done"})
        session = Session(id="d", pipeline=Pipeline.from_dict(data), context=Context(),
                          limits=Limits(counters={"seconds": 0.2}))
        result = await session.run()
        self.assertEqual(result.result["meter"], "seconds")

    async def test_a_stage_timeout_is_clamped_by_what_is_left(self):
        """A stage allowed 30s and started with 0.2 left must not run 30."""
        data = _chain({"id": "a", "type": "stage", "stage": "SlowlyStage",
                       "arguments": {"const": {"seconds": 5}}, "next": "done"})
        session = Session(id="c", pipeline=Pipeline.from_dict(data), context=Context(),
                          limits=Limits(counters={"seconds": 0.2}))
        started = asyncio.get_running_loop().time()
        await session.run()
        self.assertLess(asyncio.get_running_loop().time() - started, 2)

    async def test_waiting_for_an_answer_does_not_burn_the_deadline(self):
        """Human time is not machine time; a deadline that counted it would
        be a limit on how fast somebody reads."""
        budget = Budget(Limits(counters={"seconds": 0.3}))
        budget.start()
        await asyncio.sleep(0.2)
        budget.add_idle(0.2)
        budget.check_deadline()  # must not raise: all of it was waiting
        self.assertGreater(budget.remaining_seconds(), 0.25)


class BudgetSharingTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_subpipeline_draws_on_the_parent_budget(self):
        """A fresh budget per child would multiply the allowance by the depth
        of nesting, which is the opposite of a limit."""
        data = {
            "entry": "start",
            "nodes": [
                {"id": "start", "type": "entry", "next": "one"},
                {"id": "one", "type": "subpipeline", "subpipeline_id": "inner",
                 "next": "two"},
                {"id": "two", "type": "subpipeline", "subpipeline_id": "inner",
                 "next": "done"},
                {"id": "done", "type": "terminal", "result": {"status": "ok"}},
            ],
            "subpipelines": {"inner": {"entry": "a", "nodes": [
                {"id": "a", "type": "stage", "stage": "SpenderStage",
                 "arguments": {"const": {"budget_hint": 8}},
                 "outputs": {"value": "v"}, "next": "e"},
                {"id": "e", "type": "terminal", "result": {}},
            ]}},
        }
        session = Session(id="s", pipeline=Pipeline.from_dict(data), context=Context(),
                          limits=Limits(counters={"tokens": 12}))
        result = await session.run()
        self.assertEqual(result.result["status"], "budget_exceeded")
        self.assertEqual(result.result["meter"], "tokens")

    async def test_depth_is_a_gauge_over_nested_subpipelines(self):
        data = {
            "entry": "start",
            "nodes": [
                {"id": "start", "type": "entry", "next": "one"},
                {"id": "one", "type": "subpipeline", "subpipeline_id": "a", "next": "done"},
                {"id": "done", "type": "terminal", "result": {"status": "ok"}},
            ],
            "subpipelines": {
                "a": {"entry": "x", "nodes": [
                    {"id": "x", "type": "subpipeline", "subpipeline_id": "b", "next": "y"},
                    {"id": "y", "type": "terminal", "result": {}}]},
                "b": {"entry": "z", "nodes": [
                    {"id": "z", "type": "terminal", "result": {}}]},
            },
        }
        session = Session(id="deep", pipeline=Pipeline.from_dict(data), context=Context(),
                          limits=Limits(gauges={"depth": 1}))
        result = await session.run()
        self.assertEqual(result.result["meter"], "depth")


class ClampTests(unittest.IsolatedAsyncioTestCase):
    async def test_the_pipelines_retry_is_a_request_not_a_setting(self):
        attempts = {"n": 0}

        @register_stage("CountingBoomStage")
        class CountingBoomStage(BaseStage):
            """Fails, every time."""

            async def run(self):
                attempts["n"] += 1
                raise ValueError("no")

        data = {"entry": "start", "nodes": [
            {"id": "start", "type": "entry", "next": "a"},
            {"id": "a", "type": "stage", "stage": "CountingBoomStage",
             "retry": [{"error_equals": ["*"], "max_attempts": 50,
                        "interval_seconds": 0}], "next": "done"},
            {"id": "done", "type": "terminal", "result": {}},
        ]}
        session = Session(id="cl", pipeline=Pipeline.from_dict(data), context=Context(),
                          limits=Limits(max_retries=3))
        with self.assertRaises(ValueError):
            await session.run()
        self.assertEqual(attempts["n"], 3)


class EventHistoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_the_log_is_a_window_not_an_archive(self):
        from stageflow.core.session import EVENT_HISTORY

        session = Session(id="w", pipeline=Pipeline.from_dict(
            _chain({"id": "a", "type": "stage", "stage": "FreeStage",
                    "outputs": {"value": "v"}, "next": "done"})),
            context=Context())
        await session.run()
        for _ in range(EVENT_HISTORY * 2):
            session.emit(session.event_history[0])
        self.assertEqual(len(session.event_history), EVENT_HISTORY)


if __name__ == "__main__":
    unittest.main()


class ClockTests(unittest.IsolatedAsyncioTestCase):
    async def test_the_clock_stops_when_the_run_does(self):
        """A result read a minute later must not claim the run took a minute."""
        session = Session(id="clk", pipeline=Pipeline.from_dict(
            _chain({"id": "a", "type": "stage", "stage": "FreeStage",
                    "outputs": {"value": "v"}, "next": "done"})),
            context=Context())
        result = await session.run()
        first = result.meters["seconds"]
        await asyncio.sleep(0.2)
        self.assertEqual(session.budget.report()["seconds"], first)
