"""Unit: `retry` — which errors it matches, how long it waits, when it gives up."""
import asyncio
import unittest
from unittest import mock

from stageflow import Context, Limits, Retrier
from stageflow.core.nodes import TerminalNode
from stageflow.core.nodes.recovery import (
    error_full_name,
    error_name,
    matches_error,
    run_with_retry,
)

from _support import StubSession


class Flaky(Exception):
    pass


class Fatal(Exception):
    pass


class MatchingTests(unittest.TestCase):
    def test_names(self):
        exc = Flaky("x")
        self.assertEqual(error_name(exc), "Flaky")
        self.assertEqual(error_full_name(exc), f"{__name__}.Flaky")

    def test_by_short_name_full_name_or_wildcard(self):
        exc = Flaky()
        self.assertTrue(matches_error(["Flaky"], exc))
        self.assertTrue(matches_error([f"{__name__}.Flaky"], exc))
        self.assertTrue(matches_error(["*"], exc))
        self.assertFalse(matches_error(["Fatal"], exc))
        self.assertFalse(matches_error([], exc))

    def test_a_subclass_is_not_its_parent_by_name(self):
        # matching is by name, not isinstance: `Exception` in error_equals
        # does not catch everything — `*` does
        self.assertFalse(matches_error(["Exception"], Flaky()))


class RetrierTests(unittest.TestCase):
    def test_defaults_from_json(self):
        retrier = Retrier.from_dict({})
        self.assertEqual(retrier.error_equals, ["*"])
        self.assertEqual((retrier.max_attempts, retrier.interval_seconds,
                          retrier.backoff_rate, retrier.max_delay_seconds),
                         (3, 1.0, 2.0, None))

    def test_exponential_backoff(self):
        retrier = Retrier(error_equals=["*"], interval_seconds=0.5, backoff_rate=3)
        self.assertEqual([retrier.delay_for(i) for i in range(4)], [0.5, 1.5, 4.5, 13.5])

    def test_backoff_is_capped(self):
        retrier = Retrier(error_equals=["*"], interval_seconds=1, backoff_rate=10,
                          max_delay_seconds=5)
        self.assertEqual([retrier.delay_for(i) for i in range(3)], [1, 5, 5])


class RunWithRetryTests(unittest.TestCase):
    def _run(self, retry, failures, limits=None):
        """A body that raises each of `failures` in turn, then succeeds."""
        node = TerminalNode(id="n", retry=retry)
        session = StubSession(limits=limits)
        calls = {"n": 0}
        queue = list(failures)

        async def body():
            calls["n"] += 1
            if queue:
                raise queue.pop(0)
            return None, Context(vars={"ok": True})

        sleeps: list[float] = []

        async def fake_sleep(delay):
            sleeps.append(delay)

        with mock.patch("stageflow.core.nodes.recovery.asyncio.sleep", fake_sleep):
            outcome = asyncio.run(run_with_retry(node, session, Context(), body))
        return outcome, calls["n"], sleeps, session

    def test_no_failure_runs_once(self):
        (_, ctx), calls, sleeps, _ = self._run([], [])
        self.assertTrue(ctx.get_var("ok"))
        self.assertEqual((calls, sleeps), (1, []))

    def test_a_matching_failure_is_retried_with_backoff(self):
        retry = [Retrier(error_equals=["Flaky"], max_attempts=3, interval_seconds=1)]
        (_, ctx), calls, sleeps, session = self._run(retry, [Flaky(), Flaky()])
        self.assertTrue(ctx.get_var("ok"))
        self.assertEqual((calls, sleeps), (3, [1, 2]))
        self.assertEqual(session.event_types(), ["node_retry", "node_retry"])
        self.assertEqual(session.events[0][2]["attempt"], 1)
        self.assertEqual(session.events[0][2]["of"], 3)

    def test_max_attempts_counts_the_first_try(self):
        retry = [Retrier(error_equals=["*"], max_attempts=2, interval_seconds=0)]
        with self.assertRaises(Flaky):
            self._run(retry, [Flaky(), Flaky(), Flaky()])

    def test_a_failure_nothing_matches_is_raised_at_once(self):
        retry = [Retrier(error_equals=["Flaky"])]
        with self.assertRaises(Fatal):
            self._run(retry, [Fatal()])

    def test_the_first_matching_retrier_is_the_one_used(self):
        retry = [Retrier(error_equals=["Flaky"], max_attempts=2, interval_seconds=7),
                 Retrier(error_equals=["*"], max_attempts=5, interval_seconds=1)]
        _, _, sleeps, _ = self._run(retry, [Flaky()])
        self.assertEqual(sleeps, [7])

    def test_each_retrier_counts_its_own_attempts(self):
        retry = [Retrier(error_equals=["Flaky"], max_attempts=2, interval_seconds=0),
                 Retrier(error_equals=["Fatal"], max_attempts=2, interval_seconds=0)]
        (_, ctx), calls, _, _ = self._run(retry, [Flaky(), Fatal()])
        self.assertTrue(ctx.get_var("ok"))
        self.assertEqual(calls, 3)

    def test_the_host_caps_attempts_and_delays(self):
        retry = [Retrier(error_equals=["*"], max_attempts=10, interval_seconds=60)]
        limits = Limits(max_retries=2, max_delay_seconds=0.5)
        with self.assertRaises(Flaky):
            self._run(retry, [Flaky()] * 5, limits=limits)
        _, calls, sleeps, _ = self._run(retry, [Flaky()], limits=limits)
        self.assertEqual((calls, sleeps), (2, [0.5]))

    def test_cancellation_is_never_retried(self):
        retry = [Retrier(error_equals=["*"], max_attempts=5, interval_seconds=0)]
        with self.assertRaises(asyncio.CancelledError):
            self._run(retry, [asyncio.CancelledError()])


if __name__ == "__main__":
    unittest.main()
