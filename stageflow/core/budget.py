from __future__ import annotations

import asyncio
import contextvars
import time
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Iterator


class _Ticket:
    """A concurrency unit held by a task. `held` is False while it is lent out."""

    __slots__ = ("held",)

    def __init__(self) -> None:
        self.held = True


#: Concurrency units held by the current task, by meter. A context variable
#: because `asyncio.create_task` copies the context: a child task would think
#: it holds its parent's unit, which is why children are created only inside
#: `Budget.released`, where the parent's unit has been removed from the copy.
_HOLDING: contextvars.ContextVar[dict[str, _Ticket]] = contextvars.ContextVar(
    "stageflow_budget_holding", default={}
)


class BudgetExceeded(BaseException):
    """A session exceeded its limits.

    Derives from `BaseException`, not `Exception`, on purpose: `try` nodes and
    `retry` catch `Exception`, so a pipeline cannot catch and ignore its own
    budget. `Session.run` catches it and turns it into a result.
    """

    def __init__(self, meter: str, limit: float, spent: float):
        super().__init__(f"budget exceeded: {meter} {spent:g} of {limit:g}")
        self.meter = meter
        self.limit = limit
        self.spent = spent

    def as_result(self) -> dict:
        return {
            "status": "budget_exceeded",
            "meter": self.meter,
            "limit": self.limit,
            "spent": self.spent,
        }


@dataclass(frozen=True)
class Limits:
    """How much a session may spend. Set by the host, never by the pipeline.

    `Policy` restricts what a pipeline may use; `Limits` restricts how much.
    There are two kinds of meters:

    - **counters** only grow: `seconds`, `steps`, `iterations`, `tokens`, ...
    - **gauges** are current values: `concurrency`, `depth`, `frame_bytes`.

    The runtime charges `steps` and `iterations` and tracks the gauges; stages
    charge their own meters. Meter names are free-form: unlimited meters are
    still counted and reported in `SessionResult.meters`.

    Expensive measurements (like `frame_bytes`) only happen when limited.
    """

    #: Cumulative limits, e.g. {"seconds": 30, "steps": 10_000, "tokens": 400_000}
    counters: dict[str, float] = field(default_factory=dict)
    #: Instantaneous limits, e.g. {"concurrency": 16, "depth": 8, "frame_bytes": 1_000_000}
    gauges: dict[str, float] = field(default_factory=dict)
    #: Upper bound for `max_attempts` in a node's `retry`.
    max_retries: int | None = None
    #: Upper bound for a single delay between retries.
    max_delay_seconds: float | None = None

    @property
    def unlimited(self) -> bool:
        return not (self.counters or self.gauges
                    or self.max_retries is not None
                    or self.max_delay_seconds is not None)

    def limits_counter(self, meter: str) -> bool:
        return meter in self.counters

    def limits_gauge(self, meter: str) -> bool:
        return meter in self.gauges


UNLIMITED = Limits()


class Budget:
    """Running totals for one session and everything it starts.

    Shared by the session, its `parallel` branches, its `map` items and its
    subpipelines, so nesting cannot multiply the allowance.

    `seconds` is reported like a counter but enforced as a deadline, because
    a hung stage never charges anything.

    Not thread-safe; everything using one budget runs in one event loop.
    """

    def __init__(self, limits: Limits | None = None):
        self.limits = limits or UNLIMITED
        self.counters: dict[str, float] = {}
        self.peaks: dict[str, float] = {}
        self._held: dict[str, float] = {}
        self._slots: dict[str, asyncio.Semaphore] = {}
        self._started: float | None = None
        self._stopped: float | None = None
        self._idle: float = 0.0
        self._idle_since: float | None = None
        self._idle_depth = 0
        #: Busy time used before this object was created (restored snapshots).
        self._carried: float = 0.0
        #: Running stage timeouts -> time they had left when the current idle
        #: period began (None while the session is busy).
        self._timeouts: dict[Any, float | None] = {}

    def restore(self, spent: dict[str, float] | None) -> None:
        """Restore counters from a snapshot's `meters`, so that restoring a
        session does not reset its allowance. Gauges are not restored."""
        for meter, amount in (spent or {}).items():
            if meter.startswith("peak_"):
                continue
            if meter == "seconds":
                self._carried = float(amount)
            else:
                self.counters[meter] = float(amount)

    # ---------------------------------------------------------------- clock

    def start(self) -> None:
        if self._started is None:
            self._started = time.monotonic()

    def stop(self) -> None:
        """Stop the clock so `seconds` stops growing after the run ends."""
        if self._started is not None and self._stopped is None:
            self._stopped = time.monotonic()

    @property
    def busy_seconds(self) -> float:
        """Wall time minus idle time (time spent waiting for input)."""
        if self._started is None:
            return 0.0
        until = self._stopped if self._stopped is not None else time.monotonic()
        return self._carried + until - self._started - self.idle_seconds

    def begin_idle(self) -> None:
        """Start an idle period: the session is waiting, not working.

        Nested calls form a single idle period.
        """
        if self._idle_depth == 0:
            self._idle_since = time.monotonic()
            for timeout in self._timeouts:
                self._suspend(timeout)
        self._idle_depth += 1

    def end_idle(self) -> None:
        self._idle_depth = max(0, self._idle_depth - 1)
        if self._idle_depth == 0 and self._idle_since is not None:
            self._idle += time.monotonic() - self._idle_since
            self._idle_since = None
            loop = asyncio.get_running_loop()
            for timeout, left in self._timeouts.items():
                if left is not None:
                    timeout.reschedule(loop.time() + left)
                    self._timeouts[timeout] = None

    @contextmanager
    def pausable(self, timeout) -> Iterator[None]:
        """Pause an `asyncio.timeout` while the session is idle.

        A stage's timeout counts busy time only, so a stage waiting for a
        person does not time out. While idle, registered timeouts are put on
        hold; afterwards each gets back the time it had left.
        """
        self._timeouts[timeout] = None
        if self._idle_depth > 0:
            self._suspend(timeout)
        try:
            yield
        finally:
            self._timeouts.pop(timeout, None)

    def _suspend(self, timeout) -> None:
        when = timeout.when()
        if when is None:
            return
        self._timeouts[timeout] = when - asyncio.get_running_loop().time()
        timeout.reschedule(None)

    @property
    def idle_seconds(self) -> float:
        """Idle time so far, including an idle period still in progress."""
        if self._idle_since is None:
            return self._idle
        return self._idle + (time.monotonic() - self._idle_since)

    @property
    def deadline_seconds(self) -> float | None:
        return self.limits.counters.get("seconds")

    def remaining_seconds(self) -> float | None:
        """Seconds left before the deadline, or None without a deadline."""
        limit = self.deadline_seconds
        return None if limit is None else limit - self.busy_seconds

    def check_deadline(self) -> None:
        left = self.remaining_seconds()
        if left is not None and left <= 0:
            raise BudgetExceeded("seconds", self.deadline_seconds, self.busy_seconds)

    def timeout_for(self, wanted: float | None) -> float | None:
        """A stage timeout capped by the time left before the deadline."""
        left = self.remaining_seconds()
        if left is None:
            return wanted
        return left if wanted is None else min(wanted, left)

    # ------------------------------------------------------------- counters

    def charge(self, meter: str, amount: float) -> None:
        """Add to a counter; raise BudgetExceeded if it goes over the limit.

        Negative amounts (corrections) are never checked.
        """
        if not amount:
            return
        total = self.counters.get(meter, 0.0) + amount
        self.counters[meter] = total
        limit = self.limits.counters.get(meter)
        if limit is not None and amount > 0 and total > limit:
            raise BudgetExceeded(meter, limit, total)

    def charge_all(self, amounts: dict[str, float]) -> None:
        for meter, amount in amounts.items():
            self.charge(meter, amount)

    def spent(self, meter: str) -> float:
        return self.counters.get(meter, 0.0)

    def remaining(self, meter: str) -> float | None:
        limit = self.limits.counters.get(meter)
        return None if limit is None else limit - self.spent(meter)

    def affordable(self, amounts: dict[str, float]) -> tuple[str, float, float] | None:
        """The first meter that `amounts` would push over its limit, or None.

        Used to refuse a stage before it runs.
        """
        for meter, amount in amounts.items():
            limit = self.limits.counters.get(meter)
            if limit is None:
                continue
            total = self.spent(meter) + amount
            if total > limit:
                return meter, limit, total
        return None

    # --------------------------------------------------------------- gauges

    @asynccontextmanager
    async def slot(self, meter: str = "concurrency") -> AsyncIterator[None]:
        """Hold one unit of a gauge, waiting if none is free.

        Unlike `gauge`, this waits instead of failing: too many parallel items
        is not an error, they just take turns. (Depth, on the other hand, fails:
        waiting would not make a graph less deep.)

        Re-entrant within a task: a sequential `map` inside a branch runs in
        the branch's task and does not take a second unit. A task waiting for
        its own children lends its unit out, see `released`.
        """
        if meter in _HOLDING.get():
            yield
            return
        await self._acquire(meter)
        ticket = _Ticket()
        token = _HOLDING.set({**_HOLDING.get(), meter: ticket})
        try:
            yield
        finally:
            _HOLDING.reset(token)
            # Not held if re-acquiring after `released` was interrupted.
            if ticket.held:
                self._release(meter)

    @asynccontextmanager
    async def released(self, meter: str = "concurrency") -> AsyncIterator[None]:
        """Lend this task's unit out while it waits for its children.

        Without this, a `parallel` branch running a `map` holds a unit while
        its items wait for units: a deadlock whenever nesting is wider than
        the limit. A waiting task does no work, so it is not counted; it takes
        its unit back when the children finish.

        Create the child tasks inside this block: they copy the context, and
        only here does it show the parent holding nothing.
        """
        ticket = _HOLDING.get().get(meter)
        if ticket is None or not ticket.held:
            yield
            return
        self._release(meter)
        ticket.held = False
        token = _HOLDING.set({k: v for k, v in _HOLDING.get().items() if k != meter})
        try:
            yield
        finally:
            _HOLDING.reset(token)
            await self._acquire(meter)
            ticket.held = True

    async def _acquire(self, meter: str) -> None:
        limit = self.limits.gauges.get(meter)
        if limit is not None:
            slot = self._slots.get(meter)
            if slot is None:
                # A limit below 1 would deadlock; treat it as 1.
                slot = self._slots[meter] = asyncio.Semaphore(max(1, int(limit)))
            await self._within_deadline(slot.acquire())
        # The semaphore enforces the limit; `_held` only tracks the peak.
        held = self._held.get(meter, 0.0) + 1
        self._held[meter] = held
        self.peaks[meter] = max(self.peaks.get(meter, 0.0), held)

    def _release(self, meter: str) -> None:
        self._held[meter] = self._held.get(meter, 0.0) - 1
        slot = self._slots.get(meter)
        if slot is not None:
            slot.release()

    async def _within_deadline(self, waiting) -> None:
        """Await `waiting`, but no longer than the session deadline allows
        (the deadline is otherwise only checked between nodes)."""
        left = self.remaining_seconds()
        if left is None:
            await waiting
            return
        try:
            await asyncio.wait_for(waiting, timeout=max(left, 0))
        except asyncio.TimeoutError:
            raise BudgetExceeded("seconds", self.deadline_seconds,
                                 self.busy_seconds) from None

    @contextmanager
    def gauge(self, meter: str, amount: float = 1) -> Iterator[None]:
        """Take an instantaneous amount; raise BudgetExceeded if it does not
        fit. Use `slot` for things that should wait instead (concurrency)."""
        limit = self.limits.gauges.get(meter)
        if limit is not None and self._held.get(meter, 0.0) + amount > limit:
            raise BudgetExceeded(meter, limit, self._held.get(meter, 0.0) + amount)
        with self._hold(meter, amount):
            yield

    @contextmanager
    def _hold(self, meter: str, amount: float = 1) -> Iterator[None]:
        """Take an amount and record the peak, without checking the limit."""
        held = self._held.get(meter, 0.0) + amount
        self._held[meter] = held
        self.peaks[meter] = max(self.peaks.get(meter, 0.0), held)
        try:
            yield
        finally:
            self._held[meter] = self._held.get(meter, 0.0) - amount

    def observe(self, meter: str, value: float) -> None:
        """Record an instantaneous value that is not held, such as a frame size."""
        limit = self.limits.gauges.get(meter)
        self.peaks[meter] = max(self.peaks.get(meter, 0.0), value)
        if limit is not None and value > limit:
            raise BudgetExceeded(meter, limit, value)

    def measures(self, meter: str) -> bool:
        """Whether this gauge is limited (and therefore worth measuring)."""
        return meter in self.limits.gauges

    # --------------------------------------------------------------- report

    def report(self) -> dict[str, float]:
        """Every meter used by the session, plus `peak_*` for gauges."""
        out = dict(self.counters)
        if self._started is not None:
            out["seconds"] = round(self.busy_seconds, 4)
        for meter, peak in self.peaks.items():
            out[f"peak_{meter}"] = peak
        return out
