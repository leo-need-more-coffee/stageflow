import asyncio
import unittest

from stageflow import Budget, BudgetExceeded, Limits
from stageflow.core.budget import UNLIMITED


class LimitsShapeTests(unittest.TestCase):
    def test_a_bare_limits_is_unlimited(self):
        self.assertTrue(Limits().unlimited)
        self.assertTrue(UNLIMITED.unlimited)

    def test_any_single_field_makes_it_limited(self):
        self.assertFalse(Limits(counters={"steps": 1}).unlimited)
        self.assertFalse(Limits(gauges={"depth": 1}).unlimited)
        self.assertFalse(Limits(max_retries=1).unlimited)
        self.assertFalse(Limits(max_delay_seconds=1).unlimited)

    def test_a_zero_limit_is_a_limit_not_an_absence(self):
        """`{"tokens": 0}` means "none of that", not "no opinion"."""
        self.assertFalse(Limits(counters={"tokens": 0}).unlimited)
        budget = Budget(Limits(counters={"tokens": 0}))
        with self.assertRaises(BudgetExceeded):
            budget.charge("tokens", 1)


class CounterTests(unittest.TestCase):
    def setUp(self):
        self.budget = Budget(Limits(counters={"tokens": 100}))

    def test_charging_accumulates(self):
        self.budget.charge("tokens", 30)
        self.budget.charge("tokens", 12)
        self.assertEqual(self.budget.spent("tokens"), 42)

    def test_exactly_at_the_limit_is_allowed(self):
        """A ceiling is what you may spend, not what you may not reach."""
        self.budget.charge("tokens", 100)
        self.assertEqual(self.budget.spent("tokens"), 100)
        self.assertEqual(self.budget.remaining("tokens"), 0)

    def test_one_over_the_limit_raises_with_the_numbers(self):
        with self.assertRaises(BudgetExceeded) as caught:
            self.budget.charge("tokens", 101)
        self.assertEqual(caught.exception.meter, "tokens")
        self.assertEqual(caught.exception.limit, 100)
        self.assertEqual(caught.exception.spent, 101)

    def test_the_amount_is_kept_even_when_it_goes_over(self):
        """The bill has to say what was actually spent, not what fitted."""
        with self.assertRaises(BudgetExceeded):
            self.budget.charge("tokens", 150)
        self.assertEqual(self.budget.spent("tokens"), 150)

    def test_a_negative_charge_is_a_correction_and_never_raises(self):
        """Settling for less than was reserved goes through this path."""
        self.budget.charge("tokens", 90)
        self.budget.charge("tokens", -40)
        self.assertEqual(self.budget.spent("tokens"), 50)

    def test_charging_zero_does_nothing_at_all(self):
        self.budget.charge("tokens", 0)
        self.assertNotIn("tokens", self.budget.counters)

    def test_an_unlimited_meter_accumulates_without_a_ceiling(self):
        self.budget.charge("crm_writes", 10_000)
        self.assertEqual(self.budget.spent("crm_writes"), 10_000)
        self.assertIsNone(self.budget.remaining("crm_writes"))

    def test_charge_all_applies_each(self):
        self.budget.charge_all({"tokens": 5, "llm_calls": 1})
        self.assertEqual(self.budget.spent("tokens"), 5)
        self.assertEqual(self.budget.spent("llm_calls"), 1)


class AffordableTests(unittest.TestCase):
    """The gate that keeps an unaffordable stage from starting at all."""

    def setUp(self):
        self.budget = Budget(Limits(counters={"tokens": 100, "llm_calls": 2}))

    def test_what_fits_is_affordable(self):
        self.assertIsNone(self.budget.affordable({"tokens": 100}))

    def test_what_does_not_fit_names_the_meter_and_the_numbers(self):
        self.budget.charge("tokens", 60)
        self.assertEqual(self.budget.affordable({"tokens": 50}), ("tokens", 100, 110))

    def test_an_unlimited_meter_is_always_affordable(self):
        self.assertIsNone(self.budget.affordable({"rows": 10 ** 9}))

    def test_the_first_offending_meter_is_the_one_reported(self):
        self.budget.charge("llm_calls", 2)
        offender = self.budget.affordable({"llm_calls": 1, "tokens": 10_000})
        self.assertEqual(offender[0], "llm_calls")

    def test_affording_something_does_not_spend_it(self):
        self.budget.affordable({"tokens": 50})
        self.assertEqual(self.budget.spent("tokens"), 0)


class GaugeTests(unittest.TestCase):
    def test_a_gauge_is_released_at_the_end_of_the_block(self):
        budget = Budget(Limits(gauges={"depth": 2}))
        with budget.gauge("depth"):
            with budget.gauge("depth"):
                pass
        with budget.gauge("depth"):
            with budget.gauge("depth"):
                pass  # the first pair released, so this one fits again

    def test_a_gauge_is_released_even_when_the_block_raises(self):
        budget = Budget(Limits(gauges={"depth": 1}))
        with self.assertRaises(ValueError):
            with budget.gauge("depth"):
                raise ValueError("boom")
        with budget.gauge("depth"):
            pass

    def test_going_over_raises_on_the_way_in(self):
        budget = Budget(Limits(gauges={"depth": 1}))
        with budget.gauge("depth"):
            with self.assertRaises(BudgetExceeded) as caught:
                with budget.gauge("depth"):
                    pass
        self.assertEqual(caught.exception.meter, "depth")

    def test_the_peak_is_remembered_after_the_gauge_is_released(self):
        budget = Budget(Limits(gauges={"depth": 5}))
        with budget.gauge("depth"):
            with budget.gauge("depth"):
                pass
        self.assertEqual(budget.peaks["depth"], 2)

    def test_an_unlimited_gauge_still_records_its_peak(self):
        budget = Budget()
        with budget.gauge("concurrency"):
            with budget.gauge("concurrency"):
                pass
        self.assertEqual(budget.report()["peak_concurrency"], 2)

    def test_observe_checks_a_value_nothing_holds(self):
        budget = Budget(Limits(gauges={"frame_bytes": 100}))
        budget.observe("frame_bytes", 100)
        with self.assertRaises(BudgetExceeded):
            budget.observe("frame_bytes", 101)

    def test_measures_says_whether_it_is_worth_computing(self):
        self.assertFalse(Budget().measures("frame_bytes"))
        self.assertTrue(Budget(Limits(gauges={"frame_bytes": 1})).measures("frame_bytes"))


class SlotTests(unittest.IsolatedAsyncioTestCase):
    """`slot` waits for a turn; `gauge` refuses. The difference is the point."""

    async def test_a_slot_serialises_instead_of_failing(self):
        budget = Budget(Limits(gauges={"concurrency": 2}))
        live, peak = 0, 0

        async def worker():
            nonlocal live, peak
            async with budget.slot("concurrency"):
                live += 1
                peak = max(peak, live)
                await asyncio.sleep(0.02)
                live -= 1

        await asyncio.gather(*(worker() for _ in range(6)))
        self.assertEqual(peak, 2)
        self.assertEqual(budget.report()["peak_concurrency"], 2)

    async def test_an_unlimited_slot_lets_everything_through(self):
        budget = Budget()
        async with budget.slot("concurrency"):
            async with budget.slot("concurrency"):
                pass
        self.assertEqual(budget.report()["peak_concurrency"], 2)

    async def test_a_slot_is_released_when_the_body_raises(self):
        budget = Budget(Limits(gauges={"concurrency": 1}))
        with self.assertRaises(ValueError):
            async with budget.slot("concurrency"):
                raise ValueError("boom")
        async with budget.slot("concurrency"):
            pass

    async def test_a_limit_of_zero_still_lets_one_through(self):
        """Nonsense in, something sensible out: a throttle of nothing would
        deadlock, so it serialises instead."""
        budget = Budget(Limits(gauges={"concurrency": 0}))
        async with budget.slot("concurrency"):
            pass


class ClockTests(unittest.IsolatedAsyncioTestCase):
    async def test_an_unstarted_clock_reads_zero(self):
        self.assertEqual(Budget().busy_seconds, 0.0)

    async def test_starting_twice_does_not_restart_it(self):
        budget = Budget()
        budget.start()
        await asyncio.sleep(0.05)
        budget.start()
        self.assertGreater(budget.busy_seconds, 0.04)

    async def test_stopping_freezes_it(self):
        budget = Budget()
        budget.start()
        await asyncio.sleep(0.02)
        budget.stop()
        frozen = budget.busy_seconds
        await asyncio.sleep(0.05)
        self.assertEqual(budget.busy_seconds, frozen)

    async def test_stopping_twice_keeps_the_first_reading(self):
        budget = Budget()
        budget.start()
        budget.stop()
        first = budget.busy_seconds
        await asyncio.sleep(0.02)
        budget.stop()
        self.assertEqual(budget.busy_seconds, first)

    async def test_idle_time_is_not_busy_time(self):
        budget = Budget()
        budget.start()
        budget.begin_idle()
        await asyncio.sleep(0.1)
        budget.end_idle()
        self.assertLess(budget.busy_seconds, 0.05)

    async def test_idle_counts_while_it_is_still_going_on(self):
        """Crediting only at the end would be too late for a deadline
        checked during the wait — it would already have fired."""
        budget = Budget(Limits(counters={"seconds": 0.1}))
        budget.start()
        budget.begin_idle()
        await asyncio.sleep(0.25)
        budget.check_deadline()  # still inside the wait, and still fine
        self.assertLess(budget.busy_seconds, 0.05)
        budget.end_idle()
        budget.check_deadline()

    async def test_nested_waits_are_one_idle_period(self):
        """Counting each waiter separately would credit back more time
        than actually passed."""
        budget = Budget()
        budget.start()
        budget.begin_idle()
        budget.begin_idle()
        await asyncio.sleep(0.1)
        budget.end_idle()
        budget.end_idle()
        self.assertLess(budget.idle_seconds, 0.15)

    async def test_the_clock_runs_again_once_the_wait_is_over(self):
        budget = Budget()
        budget.start()
        budget.begin_idle()
        await asyncio.sleep(0.05)
        budget.end_idle()
        await asyncio.sleep(0.05)
        self.assertGreater(budget.busy_seconds, 0.04)

    async def test_no_deadline_means_no_remaining_and_no_check(self):
        budget = Budget()
        budget.start()
        self.assertIsNone(budget.remaining_seconds())
        budget.check_deadline()  # must not raise

    async def test_the_deadline_raises_once_it_is_past(self):
        budget = Budget(Limits(counters={"seconds": 0.05}))
        budget.start()
        await asyncio.sleep(0.08)
        with self.assertRaises(BudgetExceeded) as caught:
            budget.check_deadline()
        self.assertEqual(caught.exception.meter, "seconds")

    async def test_the_clock_appears_in_the_report_as_a_counter(self):
        budget = Budget()
        budget.start()
        budget.stop()
        self.assertIn("seconds", budget.report())


class TimeoutForTests(unittest.TestCase):
    def test_no_deadline_leaves_the_wanted_timeout_alone(self):
        budget = Budget()
        self.assertEqual(budget.timeout_for(30), 30)
        self.assertIsNone(budget.timeout_for(None))

    def test_a_deadline_caps_a_longer_timeout(self):
        budget = Budget(Limits(counters={"seconds": 5}))
        budget.start()
        self.assertLessEqual(budget.timeout_for(30), 5)

    def test_a_shorter_timeout_wins_over_a_longer_deadline(self):
        budget = Budget(Limits(counters={"seconds": 30}))
        budget.start()
        self.assertEqual(budget.timeout_for(2), 2)

    def test_a_deadline_gives_a_timeout_to_a_stage_that_asked_for_none(self):
        """A stage with timeout=None is not a licence to outlive the run."""
        budget = Budget(Limits(counters={"seconds": 5}))
        budget.start()
        self.assertLessEqual(budget.timeout_for(None), 5)


class ReportTests(unittest.TestCase):
    def test_the_report_carries_counters_and_peaks_apart(self):
        budget = Budget()
        budget.charge("tokens", 12)
        with budget.gauge("concurrency"):
            pass
        report = budget.report()
        self.assertEqual(report["tokens"], 12)
        self.assertEqual(report["peak_concurrency"], 1)

    def test_an_untouched_budget_reports_nothing(self):
        self.assertEqual(Budget().report(), {})


if __name__ == "__main__":
    unittest.main()


class IndependenceTests(unittest.TestCase):
    """Defaults that are shared and writable are a trap worth a test.

    `Limits` is frozen, but the dicts inside it are not. A single default
    instance handed to every policy would mean one write changing the
    allowance of everything at once — which is exactly what happened while
    these tests were being written.
    """

    def test_two_default_limits_do_not_share_their_dicts(self):
        first, second = Limits(), Limits()
        first.counters["tokens"] = 1
        self.assertEqual(second.counters, {})

    def test_two_default_policies_do_not_share_their_limits(self):
        from stageflow import Policy

        first, second = Policy(), Policy()
        first.limits.counters["tokens"] = 1
        self.assertEqual(second.limits.counters, {})
        self.assertTrue(second.limits.unlimited)

    def test_a_budget_does_not_write_into_the_limits_it_was_given(self):
        limits = Limits(counters={"tokens": 10})
        budget = Budget(limits)
        budget.charge("tokens", 5)
        self.assertEqual(limits.counters, {"tokens": 10})
