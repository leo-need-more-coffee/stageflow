"""OpenTelemetry traces and metrics for StageFlow runs.

    pip install "stageflow-framework[otel]"

    from stageflow.observability.otel import OpenTelemetryInstrument
    session = Session(..., instrument=OpenTelemetryInstrument())

Uses the global tracer and meter providers unless given others, so whatever
the host configured (OTLP exporter, Jaeger, Prometheus) receives the data.

**Traces.** One span per run (`stageflow.session`) and one per node
(`<type> <id>`, e.g. `stage classify`). Spans nest like the graph: nodes of a
`parallel` branch or a `map` item are children of that node's span, and a
subpipeline's run is a child of its `subpipeline` node. Session events
(`node_retry`, `try_caught`, `stage_charged`, …) become span events on the
node that emitted them. A node that raised has status ERROR and the exception
recorded; a cancelled one (a sibling branch failed) is marked
`stageflow.cancelled`.

**Metrics.**

| Name | Kind | Attributes |
|---|---|---|
| `stageflow.sessions` | counter | `outcome`, `nested` |
| `stageflow.session.duration` | histogram, s | `outcome`, `nested` |
| `stageflow.nodes` | counter | `type`, `stage`, `outcome` |
| `stageflow.node.duration` | histogram, s | `type`, `stage`, `outcome` |
| `stageflow.usage` | counter | `meter` — what top-level runs spent: steps, tokens, … |
"""
from __future__ import annotations

import json
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Iterator

from ..capabilities import __version__
from ..core.budget import BudgetExceeded
from ..core.instrument import Instrument
from . import _is_cancel, outcome_of

try:
    from opentelemetry import metrics, trace
    from opentelemetry.trace import Status, StatusCode
except ImportError as exc:  # pragma: no cover - depends on the environment
    raise ImportError(
        "OpenTelemetry is not installed: pip install \"stageflow-framework[otel]\""
    ) from exc

if TYPE_CHECKING:  # pragma: no cover
    from ..core.event import Event
    from ..core.nodes import Node
    from ..core.session import Session

_MAX_ATTRIBUTE = 512


def _attribute(value: Any) -> Any:
    """A value OpenTelemetry accepts as an attribute; JSON text otherwise."""
    if isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, (list, tuple)) and value and all(
            isinstance(v, type(value[0])) and isinstance(v, (str, bool, int, float))
            for v in value):
        return list(value)
    text = json.dumps(value, default=str, ensure_ascii=False)
    return text if len(text) <= _MAX_ATTRIBUTE else text[:_MAX_ATTRIBUTE] + "…"


class OpenTelemetryInstrument(Instrument):
    """Spans for runs and nodes, plus counters and histograms."""

    def __init__(self, tracer_provider=None, meter_provider=None):
        self.tracer = trace.get_tracer("stageflow", __version__,
                                       tracer_provider=tracer_provider)
        meter = metrics.get_meter("stageflow", __version__, meter_provider=meter_provider)
        self._sessions = meter.create_counter(
            "stageflow.sessions", unit="{session}", description="Runs that ended")
        self._session_seconds = meter.create_histogram(
            "stageflow.session.duration", unit="s", description="Wall time of a run")
        self._nodes = meter.create_counter(
            "stageflow.nodes", unit="{node}", description="Nodes executed")
        self._node_seconds = meter.create_histogram(
            "stageflow.node.duration", unit="s",
            description="Wall time of a node, including everything inside it")
        self._usage = meter.create_counter(
            "stageflow.usage", description="Meters spent by top-level runs")

    @contextmanager
    def session(self, session: "Session") -> Iterator[None]:
        nested = not session._owns_budget
        started = time.perf_counter()
        error: BaseException | None = None
        with self.tracer.start_as_current_span(
            "stageflow.session",
            attributes={"stageflow.session.id": session.id,
                        "stageflow.session.nested": nested},
            record_exception=False, set_status_on_exception=False,
        ) as span:
            try:
                yield
            except BaseException as exc:
                error = exc
                self._mark_failed(span, exc)
                raise
            finally:
                outcome = outcome_of(session, error)
                span.set_attribute("stageflow.outcome", outcome)
                report = session.budget.report()
                for meter, amount in report.items():
                    span.set_attribute(f"stageflow.meter.{meter}", amount)
                if outcome == "budget_exceeded":
                    span.set_status(Status(StatusCode.ERROR, "budget exceeded"))
                attrs = {"outcome": outcome, "nested": nested}
                self._sessions.add(1, attrs)
                self._session_seconds.record(time.perf_counter() - started, attrs)
                if not nested:
                    for meter, amount in report.items():
                        if not meter.startswith("peak_") and meter != "seconds" and amount:
                            self._usage.add(amount, {"meter": meter})

    @contextmanager
    def node(self, session: "Session", node: "Node") -> Iterator[None]:
        stage = getattr(node, "stage", None) or ""
        started = time.perf_counter()
        outcome = "ok"
        attributes = {"stageflow.node.id": node.id, "stageflow.node.type": node.type,
                      "stageflow.session.id": session.id}
        if stage:
            attributes["stageflow.stage"] = stage
        with self.tracer.start_as_current_span(
            f"{node.type} {node.id}", attributes=attributes,
            record_exception=False, set_status_on_exception=False,
        ) as span:
            try:
                yield
            except BaseException as exc:
                outcome = "cancelled" if _is_cancel(exc) else type(exc).__name__
                self._mark_failed(span, exc)
                raise
            finally:
                attrs = {"type": node.type, "stage": stage, "outcome": outcome}
                self._nodes.add(1, attrs)
                self._node_seconds.record(time.perf_counter() - started, attrs)

    def event(self, session: "Session", event: "Event") -> None:
        span = trace.get_current_span()
        if not span.is_recording():
            return
        attributes = {f"stageflow.{k}": _attribute(v)
                      for k, v in (event.payload or {}).items() if v is not None}
        if event.stage_id:
            attributes["stageflow.node.id"] = event.stage_id
        span.add_event(event.type, attributes)

    @staticmethod
    def _mark_failed(span, exc: BaseException) -> None:
        if _is_cancel(exc):
            span.set_attribute("stageflow.cancelled", True)
            return
        span.record_exception(exc)
        description = "budget exceeded" if isinstance(exc, BudgetExceeded) else str(exc)
        span.set_status(Status(StatusCode.ERROR, description[:_MAX_ATTRIBUTE]))
