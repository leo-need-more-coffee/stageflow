"""Hooks for observing a run: tracing, metrics, logs.

The core does not depend on any telemetry library. A session takes an
optional `Instrument`, and calls it at three points:

- `session(session)` wraps a whole run (including a subpipeline's child run);
- `node(session, node)` wraps every node, wherever it runs — the main loop,
  a `try` body, a `parallel` branch, a `map` item;
- `event(session, event)` sees every event the session emits.

Both context managers are entered and exited in the task that runs the work,
so anything kept in a context variable — an OpenTelemetry span, for example —
nests the way the graph does: a branch's nodes inside the `parallel` node, a
subpipeline's run inside its node.

With no instrument the cost is one `if` per node.

Ready-made instruments live in `stageflow.observability`.
"""
from __future__ import annotations

from contextlib import ExitStack, contextmanager
from typing import TYPE_CHECKING, Iterator

if TYPE_CHECKING:  # pragma: no cover
    from .event import Event
    from .nodes import Node
    from .session import Session


class Instrument:
    """Base class: every hook does nothing. Override the ones you need."""

    @contextmanager
    def session(self, session: "Session") -> Iterator[None]:
        yield

    @contextmanager
    def node(self, session: "Session", node: "Node") -> Iterator[None]:
        yield

    def event(self, session: "Session", event: "Event") -> None:
        pass


class Instruments(Instrument):
    """Several instruments at once, called in the order given."""

    def __init__(self, *instruments: Instrument):
        self.instruments = [i for i in instruments if i is not None]

    @contextmanager
    def session(self, session: "Session") -> Iterator[None]:
        with ExitStack() as stack:
            for instrument in self.instruments:
                stack.enter_context(instrument.session(session))
            yield

    @contextmanager
    def node(self, session: "Session", node: "Node") -> Iterator[None]:
        with ExitStack() as stack:
            for instrument in self.instruments:
                stack.enter_context(instrument.node(session, node))
            yield

    def event(self, session: "Session", event: "Event") -> None:
        for instrument in self.instruments:
            instrument.event(session, event)
