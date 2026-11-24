"""Ready-made instruments for watching runs.

- `Recorder` — in-memory counts and timings per node type and per stage.
  No dependencies; good for tests, benchmarks and a quick look.
- `LoggingInstrument` — structured records through the standard `logging`.
- `OpenTelemetryInstrument` (in `stageflow.observability.otel`) — traces and
  metrics. Needs `pip install "stageflow-framework[otel]"`.

Pass one to `Session(instrument=...)`, or several with `Instruments(a, b)`.
"""
from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Iterator

from ..core.budget import BudgetExceeded
from ..core.durable import RunSuspended
from ..core.instrument import Instrument, Instruments

if TYPE_CHECKING:  # pragma: no cover
    from ..core.event import Event
    from ..core.nodes import Node
    from ..core.session import Session

__all__ = ["Instrument", "Instruments", "LoggingInstrument", "Recorder", "Stats",
           "outcome_of", "node_outcome", "node_label"]


def outcome_of(session: "Session", error: BaseException | None = None) -> str:
    """One word for how a run ended: finished, waiting (saved at a `wait`
    node), stopped, budget_exceeded, failed, cancelled or ended (no terminal
    was reached)."""
    if error is not None:
        if isinstance(error, BudgetExceeded):
            return "budget_exceeded"
        return "cancelled" if _is_cancel(error) else "failed"
    result = session.result or {}
    if result.get("status") == "budget_exceeded":
        return "budget_exceeded"
    if result.get("result") == "stopped":
        return "stopped"
    if result.get("status") == "waiting":
        return "waiting"
    return "finished" if session.finished else "ended"


def node_label(node: "Node") -> str:
    """`stage:LoadTicket`, `map`, `parallel` — what a node is, for grouping."""
    stage = getattr(node, "stage", None)
    return f"stage:{stage}" if stage else node.type


def _is_cancel(error: BaseException) -> bool:
    import asyncio
    return isinstance(error, asyncio.CancelledError)


def node_outcome(error: BaseException) -> str:
    """How a node that raised ended: cancelled, waiting (a `wait` node saved
    the run), or the exception's class name."""
    if _is_cancel(error):
        return "cancelled"
    if isinstance(error, RunSuspended):
        return "waiting"
    return type(error).__name__


@dataclass
class Stats:
    """Counts and timings for one group of nodes or sessions."""

    count: int = 0
    errors: int = 0
    total_seconds: float = 0.0
    max_seconds: float = 0.0
    outcomes: dict[str, int] = field(default_factory=dict)

    def add(self, seconds: float, outcome: str) -> None:
        self.count += 1
        self.total_seconds += seconds
        self.max_seconds = max(self.max_seconds, seconds)
        self.outcomes[outcome] = self.outcomes.get(outcome, 0) + 1
        if outcome not in ("ok", "finished", "ended", "stopped", "waiting"):
            self.errors += 1

    @property
    def mean_seconds(self) -> float:
        return self.total_seconds / self.count if self.count else 0.0

    def to_dict(self) -> dict[str, Any]:
        return {"count": self.count, "errors": self.errors,
                "total_seconds": self.total_seconds, "mean_seconds": self.mean_seconds,
                "max_seconds": self.max_seconds, "outcomes": dict(self.outcomes)}


class Recorder(Instrument):
    """Keeps counts and timings in memory.

    `nodes` is keyed by `node_label` (`map`, `stage:LoadTicket`, …), `sessions`
    by whether the run was top-level or a subpipeline's, and `usage` sums the
    meters of top-level runs. Note that a block's time includes the time of
    everything inside it.
    """

    def __init__(self) -> None:
        self.nodes: dict[str, Stats] = {}
        self.sessions: dict[str, Stats] = {}
        self.usage: dict[str, float] = {}
        self.events: dict[str, int] = {}

    @contextmanager
    def session(self, session: "Session") -> Iterator[None]:
        started = time.perf_counter()
        error: BaseException | None = None
        try:
            yield
        except BaseException as exc:
            error = exc
            raise
        finally:
            nested = not session._owns_budget
            key = "subpipeline" if nested else "session"
            self.sessions.setdefault(key, Stats()).add(
                time.perf_counter() - started, outcome_of(session, error))
            if not nested:
                for meter, amount in session.budget.report().items():
                    if not meter.startswith("peak_") and meter != "seconds":
                        self.usage[meter] = self.usage.get(meter, 0.0) + amount

    @contextmanager
    def node(self, session: "Session", node: "Node") -> Iterator[None]:
        started = time.perf_counter()
        outcome = "ok"
        try:
            yield
        except BaseException as exc:
            outcome = node_outcome(exc)
            raise
        finally:
            self.nodes.setdefault(node_label(node), Stats()).add(
                time.perf_counter() - started, outcome)

    def event(self, session: "Session", event: "Event") -> None:
        self.events[event.type] = self.events.get(event.type, 0) + 1

    def summary(self) -> dict[str, Any]:
        return {"nodes": {k: v.to_dict() for k, v in sorted(self.nodes.items())},
                "sessions": {k: v.to_dict() for k, v in sorted(self.sessions.items())},
                "usage": dict(sorted(self.usage.items())),
                "events": dict(sorted(self.events.items()))}

    def table(self) -> str:
        """The nodes as a plain-text table, slowest total first."""
        rows = sorted(self.nodes.items(), key=lambda kv: -kv[1].total_seconds)
        width = max([len(k) for k, _ in rows] + [4])
        lines = [f"{'node':<{width}}  {'count':>7}  {'errors':>6}  "
                 f"{'mean ms':>9}  {'max ms':>9}  {'total ms':>10}"]
        for label, s in rows:
            lines.append(f"{label:<{width}}  {s.count:>7}  {s.errors:>6}  "
                         f"{s.mean_seconds * 1e3:>9.3f}  {s.max_seconds * 1e3:>9.3f}  "
                         f"{s.total_seconds * 1e3:>10.3f}")
        return "\n".join(lines)


class LoggingInstrument(Instrument):
    """Structured records through `logging`.

    Session start and end at INFO, a failed session at ERROR, nodes and events
    at DEBUG. Every record carries `record.stageflow`, a dict with the session
    id and whatever else applies (node, type, seconds, outcome, meters), for a
    JSON formatter to pick up.
    """

    def __init__(self, logger: logging.Logger | str = "stageflow"):
        self.logger = logging.getLogger(logger) if isinstance(logger, str) else logger

    def _log(self, level: int, message: str, **fields: Any) -> None:
        if self.logger.isEnabledFor(level):
            self.logger.log(level, message, extra={"stageflow": fields})

    @contextmanager
    def session(self, session: "Session") -> Iterator[None]:
        started = time.perf_counter()
        self._log(logging.INFO, f"session {session.id} started", session=session.id)
        error: BaseException | None = None
        try:
            yield
        except BaseException as exc:
            error = exc
            raise
        finally:
            outcome = outcome_of(session, error)
            seconds = time.perf_counter() - started
            level = logging.ERROR if outcome == "failed" else logging.INFO
            self._log(level, f"session {session.id} {outcome} in {seconds:.3f}s",
                      session=session.id, outcome=outcome, seconds=seconds,
                      result=session.result, meters=session.budget.report(),
                      error=repr(error) if error else None)

    @contextmanager
    def node(self, session: "Session", node: "Node") -> Iterator[None]:
        if not self.logger.isEnabledFor(logging.DEBUG):
            yield
            return
        started = time.perf_counter()
        outcome = "ok"
        try:
            yield
        except BaseException as exc:
            outcome = node_outcome(exc)
            raise
        finally:
            seconds = time.perf_counter() - started
            self._log(logging.DEBUG,
                      f"{session.id} {node.type} {node.id}: {outcome} in {seconds * 1e3:.2f}ms",
                      session=session.id, node=node.id, type=node.type,
                      stage=getattr(node, "stage", None), outcome=outcome, seconds=seconds)

    def event(self, session: "Session", event: "Event") -> None:
        self._log(logging.DEBUG, f"{session.id} event {event.type}",
                  session=session.id, event=event.type, node=event.stage_id,
                  payload=event.payload)
