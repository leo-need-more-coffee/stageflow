"""Unit: the parts of `Budget` that fan-out and snapshots lean on."""
import asyncio
import unittest
from unittest import mock

from stageflow import Budget, BudgetExceeded, Limits


class ReleasedTests(unittest.IsolatedAsyncioTestCase):
    async def test_released_without_a_slot_does_nothing(self):
        budget = Budget(Limits(gauges={"concurrency": 1}))
        async with budget.released("concurrency"):
            pass
        async with budget.slot("concurrency"):
            pass  # the unit was never lent out, so it is still there

    async def test_a_released_unit_is_free_for_somebody_else(self):
        budget = Budget(Limits(gauges={"concurrency": 1}))
        got_in = asyncio.Event()

        async def other():
            async with budget.slot("concurrency"):
                got_in.set()

        async with budget.slot("concurrency"):
            async with budget.released("concurrency"):
                task = asyncio.create_task(other())
                await asyncio.wait_for(got_in.wait(), 1)
                await task
        self.assertEqual(budget.report()["peak_concurrency"], 1)

    async def test_a_child_made_while_released_takes_its_own_unit(self):
        budget = Budget(Limits(gauges={"concurrency": 1}))
        inside = []

        async def child():
            async with budget.slot("concurrency"):
                inside.append(budget._held["concurrency"])

        async with budget.slot("concurrency"):
            async with budget.released("concurrency"):
                await asyncio.create_task(child())
        self.assertEqual(inside, [1.0], "the child held the one unit, alone")

    async def test_a_failed_take_back_does_not_give_back_twice(self):
        # the parent's wait to re-take its unit is cut short; the unit it no
        # longer holds must not be released on the way out, or the semaphore
        # would allow two at once from then on
        budget = Budget(Limits(gauges={"concurrency": 1}))
        original = budget._acquire
        calls = {"n": 0}

        async def failing_second_acquire(meter):
            calls["n"] += 1
            if calls["n"] == 2:
                raise asyncio.CancelledError
            await original(meter)

        with mock.patch.object(budget, "_acquire", failing_second_acquire):
            with self.assertRaises(asyncio.CancelledError):
                async with budget.slot("concurrency"):
                    async with budget.released("concurrency"):
                        pass
        self.assertEqual(budget._slots["concurrency"]._value, 1,
                         "exactly one unit, not two")

    async def test_waiting_for_a_unit_stops_at_the_deadline(self):
        budget = Budget(Limits(counters={"seconds": 0.05}, gauges={"concurrency": 1}))
        budget.start()
        hold = asyncio.Event()

        async def holder():
            async with budget.slot("concurrency"):
                await hold.wait()

        task = asyncio.create_task(holder())
        await asyncio.sleep(0)
        with self.assertRaises(BudgetExceeded) as caught:
            async with budget.slot("concurrency"):
                pass
        self.assertEqual(caught.exception.meter, "seconds")
        hold.set()
        await task


class RestoreTests(unittest.TestCase):
    def test_counters_carry_over_and_peaks_do_not(self):
        budget = Budget()
        budget.restore({"steps": 5, "tokens": 120.5, "peak_concurrency": 4})
        self.assertEqual(budget.spent("steps"), 5)
        self.assertEqual(budget.spent("tokens"), 120.5)
        self.assertNotIn("concurrency", budget.peaks)

    def test_seconds_carry_over_into_the_clock(self):
        budget = Budget(Limits(counters={"seconds": 10}))
        budget.restore({"seconds": 9.5})
        budget.start()
        self.assertGreaterEqual(budget.busy_seconds, 9.5)
        self.assertLessEqual(budget.remaining_seconds(), 0.5)

    def test_restored_spending_counts_against_the_limit(self):
        budget = Budget(Limits(counters={"steps": 10}))
        budget.restore({"steps": 10})
        with self.assertRaises(BudgetExceeded):
            budget.charge("steps", 1)

    def test_nothing_to_restore(self):
        budget = Budget()
        budget.restore(None)
        budget.restore({})
        self.assertEqual(budget.report(), {})


class AccountingTests(unittest.TestCase):
    def test_a_correction_never_raises(self):
        budget = Budget(Limits(counters={"tokens": 10}))
        budget.charge("tokens", 10)
        budget.charge("tokens", -4)
        self.assertEqual(budget.spent("tokens"), 6)

    def test_affordable_names_the_first_meter_over(self):
        budget = Budget(Limits(counters={"a": 5, "b": 5}))
        budget.charge("b", 4)
        self.assertIsNone(budget.affordable({"a": 5, "b": 1}))
        self.assertEqual(budget.affordable({"a": 1, "b": 2}), ("b", 5, 6))
        self.assertIsNone(budget.affordable({"unlimited": 1e9}))

    def test_timeout_is_clamped_by_what_is_left(self):
        budget = Budget(Limits(counters={"seconds": 5}))
        budget.start()
        self.assertLessEqual(budget.timeout_for(60), 5)
        self.assertLessEqual(budget.timeout_for(None), 5)
        self.assertEqual(Budget().timeout_for(60), 60)


if __name__ == "__main__":
    unittest.main()
