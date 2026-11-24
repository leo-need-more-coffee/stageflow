from __future__ import annotations

import asyncio
import contextvars
import time
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from typing import AsyncIterator, Iterator


class _Ticket:
    """One unit a task holds — or held, until it gave it back for a wait."""

    __slots__ = ("held",)

    def __init__(self) -> None:
        self.held = True


#: The slots the current task holds, by meter. A context variable because a
#: task is what holds one: `asyncio.create_task` copies the context, so a
#: child task starts out believing it holds whatever its parent held — which
#: is why children are only ever created inside `Budget.released`, where the
#: parent has given its slot back and the copy says so.
_HOLDING: contextvars.ContextVar[dict[str, _Ticket]] = contextvars.ContextVar(
    "stageflow_budget_holding", default={}
)


class BudgetExceeded(BaseException):
    """A session ran past what it was allowed.

    Deliberately **not** a `StageFlowError`, and deliberately a
    `BaseException`: `try` blocks in a pipeline catch `Exception`, and so does
    `run_with_retry`. A tenant who could wrap a graph in `try` and swallow
    this would make the whole budget decoration. Uncatchability comes from the
    class hierarchy rather than from a check somebody can forget to write.

    `Session.run` is the one place that catches it, and turns it into a
    result: the work already done is not thrown away with it.
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
    """What a session may spend. Given by the host, never by the pipeline.

    A `Policy` says what a pipeline may be made of; this says how much of
    anything it may use, and the two are separate because a graph built
    entirely of allowed stages can still loop, fan out into thousands of
    tasks, or run until the process dies.

    One mechanism rather than three, with two kinds of meter. **Counters**
    grow and never fall — `seconds`, `steps`, `iterations`, `tokens`.
    **Gauges** are instantaneous — `concurrency`, `depth`, `frame_bytes`.
    The runtime charges `steps` and `iterations` and holds the gauges;
    stages charge whatever they spent.

    Names are not fixed by the core: a host counts what is scarce for it, and
    a meter nobody limits simply accumulates and comes back in the result,
    which is what a bill is made of.

    Nothing is measured unless it is limited. A meter absent from here is not
    computed at all — `frame_bytes` means serialising the frame — so the cost
    of the whole mechanism to a host that sets no limits is one `if`.
    """

    #: cumulative: {"seconds": 30, "steps": 10_000, "tokens": 400_000}
    counters: dict[str, float] = field(default_factory=dict)
    #: instantaneous: {"concurrency": 16, "depth": 8, "frame_bytes": 1_000_000}
    gauges: dict[str, float] = field(default_factory=dict)
    #: the most attempts a node's own `retry` may ask for
    max_retries: int | None = None
    #: the longest a single pause between retries may be
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
    """The running total for one session tree.

    One object is shared by a session, the branches of its `parallel` nodes,
    the iterations of its `map` nodes and the child sessions of its
    subpipelines. A copy per child would multiply the allowance by the depth
    of nesting, which is the opposite of a limit.

    `seconds` is the one hybrid: reported like a counter so that it lands in
    the same table as everything else, enforced as a deadline because a
    counter is only checked when something charges it, and a stage that hangs
    charges nothing.

    Not thread-safe, and does not need to be: everything drawing on one budget
    runs in one event loop.
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
        #: busy time spent before this object existed — a session restored
        #: from a snapshot carries on from what it had already used
        self._carried: float = 0.0

    def restore(self, spent: dict[str, float] | None) -> None:
        """Carry the running total over from a snapshot's `meters`.

        Without it a session restored mid-run starts from zero, and stopping
        and restoring a session is a way to get a fresh allowance whenever the
        old one runs out. Only counters travel: gauges are instantaneous and
        nothing is being held by a session that is not running.
        """
        for meter, amount in (spent or {}).items():
            if meter.startswith("peak_"):
                continue
            if meter == "seconds":
                self._carried = float(amount)
            else:
                self.counters[meter] = float(amount)

    # ------------------------------------------------------------- the clock

    def start(self) -> None:
        if self._started is None:
            self._started = time.monotonic()

    def stop(self) -> None:
        """Freeze the clock. Without this the reported `seconds` keeps
        growing after the run is over, and a result read a minute later
        claims the run took a minute."""
        if self._started is not None and self._stopped is None:
            self._stopped = time.monotonic()

    @property
    def busy_seconds(self) -> float:
        """Wall time minus time spent waiting for somebody to answer.

        Waiting for a human is not work. A pipeline that asks a question and
        sits there is not consuming the host, and a deadline that counted it
        would make human-in-the-loop impossible to write.
        """
        if self._started is None:
            return 0.0
        until = self._stopped if self._stopped is not None else time.monotonic()
        return self._carried + until - self._started - self.idle_seconds

    def begin_idle(self) -> None:
        """Stop the clock: from here the session is waiting, not working.

        Nested waits are one idle period, not several. Counting each waiter
        separately would credit back more time than passed, and a session
        that is waiting at all is usually waiting for the only thing it has
        to do.
        """
        if self._idle_depth == 0:
            self._idle_since = time.monotonic()
        self._idle_depth += 1

    def end_idle(self) -> None:
        self._idle_depth = max(0, self._idle_depth - 1)
        if self._idle_depth == 0 and self._idle_since is not None:
            self._idle += time.monotonic() - self._idle_since
            self._idle_since = None

    @property
    def idle_seconds(self) -> float:
        """Time spent waiting, including a wait that is still going on.

        Crediting it only once the wait ends would be too late for the thing
        it exists for: a deadline checked *during* the wait would already
        have fired.
        """
        if self._idle_since is None:
            return self._idle
        return self._idle + (time.monotonic() - self._idle_since)

    @property
    def deadline_seconds(self) -> float | None:
        return self.limits.counters.get("seconds")

    def remaining_seconds(self) -> float | None:
        """What is left of the deadline, or None when there is none."""
        limit = self.deadline_seconds
        return None if limit is None else limit - self.busy_seconds

    def check_deadline(self) -> None:
        left = self.remaining_seconds()
        if left is not None and left <= 0:
            raise BudgetExceeded("seconds", self.deadline_seconds, self.busy_seconds)

    def timeout_for(self, wanted: float | None) -> float | None:
        """A stage timeout, clamped by what is left of the deadline.

        Without the clamp a stage allowed 60 seconds and started with two left
        runs for sixty, and the budget leaks by the length of its last stage.
        """
        left = self.remaining_seconds()
        if left is None:
            return wanted
        return left if wanted is None else min(wanted, left)

    # ----------------------------------------------------------- counters

    def charge(self, meter: str, amount: float) -> None:
        """Add to a counter. Raises if that takes it past its limit.

        A negative amount is a correction (a reservation settled for less) and
        can never take anything over, so it is not checked.
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
        """The first meter that `amounts` would take over, if any.

        Used to refuse an expensive stage **before** it runs: an operation
        that cannot be paid for is better not begun than cut off halfway.
        """
        for meter, amount in amounts.items():
            limit = self.limits.counters.get(meter)
            if limit is None:
                continue
            total = self.spent(meter) + amount
            if total > limit:
                return meter, limit, total
        return None

    # ------------------------------------------------------------- gauges

    @asynccontextmanager
    async def slot(self, meter: str = "concurrency") -> AsyncIterator[None]:
        """Hold one unit of a gauge, waiting for a turn rather than failing.

        The difference from `gauge` is deliberate and it is about what the
        limit means. Depth is a property of the graph: too deep is too deep,
        and waiting would not help. Concurrency is a property of the moment —
        a hundred items to process is a perfectly good pipeline, it just may
        not have a hundred calls in the air at once. Failing it would refuse
        legitimate work for being wide; holding it back runs the same work
        within the allowance.

        Re-entrant within a task: a sequential `map` inside a `parallel`
        branch runs in the branch's own task and adds no concurrency, so it
        does not take a second unit. A task waiting for children of its own
        gives its unit back for the wait — see `released`.
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
            # not held when taking it back after a wait was cut short — by a
            # cancellation or the deadline — and then there is nothing to give
            if ticket.held:
                self._release(meter)

    @asynccontextmanager
    async def released(self, meter: str = "concurrency") -> AsyncIterator[None]:
        """Give this task's unit back while it waits for its children.

        A `parallel` branch that runs a `map` holds a unit and then waits for
        items that each need one. With the limit at 1 that is a deadlock, and
        with any limit it is one as soon as the nesting is wider than the
        allowance — the waiting parent occupies the very unit its children
        need. A task that is waiting is not working, so for the length of the
        wait it is not counted: the limit bounds the work actually in the
        air, and the parent takes its unit back once the children are done.

        Children must be created *inside* this block: they copy the context,
        and only here does it say the parent holds nothing.
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
                # a throttle of nothing would deadlock, so nonsense serialises
                slot = self._slots[meter] = asyncio.Semaphore(max(1, int(limit)))
            await self._within_deadline(slot.acquire())
        # the semaphore is the enforcement; holding only records the peak, or
        # the two would have to agree about the boundary twice
        held = self._held.get(meter, 0.0) + 1
        self._held[meter] = held
        self.peaks[meter] = max(self.peaks.get(meter, 0.0), held)

    def _release(self, meter: str) -> None:
        self._held[meter] = self._held.get(meter, 0.0) - 1
        slot = self._slots.get(meter)
        if slot is not None:
            slot.release()

    async def _within_deadline(self, waiting) -> None:
        """Wait for a turn, but not past the session's deadline.

        The deadline is otherwise checked between nodes, and a task waiting
        for a unit is between nothing: without this, a run whose units are
        all taken would wait for as long as they are, whatever `seconds` says.
        """
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
        """Take up an instantaneous quantity, refusing if it does not fit.

        For what should wait rather than fail — concurrency — use `slot`.
        """
        limit = self.limits.gauges.get(meter)
        if limit is not None and self._held.get(meter, 0.0) + amount > limit:
            raise BudgetExceeded(meter, limit, self._held.get(meter, 0.0) + amount)
        with self._hold(meter, amount):
            yield

    @contextmanager
    def _hold(self, meter: str, amount: float = 1) -> Iterator[None]:
        """Take the quantity up and remember the peak, checking nothing."""
        held = self._held.get(meter, 0.0) + amount
        self._held[meter] = held
        self.peaks[meter] = max(self.peaks.get(meter, 0.0), held)
        try:
            yield
        finally:
            self._held[meter] = self._held.get(meter, 0.0) - amount

    def observe(self, meter: str, value: float) -> None:
        """Report an instantaneous value that nothing holds — a frame size."""
        limit = self.limits.gauges.get(meter)
        self.peaks[meter] = max(self.peaks.get(meter, 0.0), value)
        if limit is not None and value > limit:
            raise BudgetExceeded(meter, limit, value)

    def measures(self, meter: str) -> bool:
        """Whether anything is limiting this at all — the cheap way out of a
        measurement that costs something to take."""
        return meter in self.limits.gauges

    # ------------------------------------------------------------ the bill

    def report(self) -> dict[str, float]:
        """Every meter this session moved, for the host to bill or log."""
        out = dict(self.counters)
        if self._started is not None:
            out["seconds"] = round(self.busy_seconds, 4)
        for meter, peak in self.peaks.items():
            out[f"peak_{meter}"] = peak
        return out
