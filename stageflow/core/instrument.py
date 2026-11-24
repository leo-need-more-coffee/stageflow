"""Hooks for observing runs: tracing, metrics, logs.

The core depends on no telemetry library. A session takes an optional
`Instrument` and calls it at three points:

- `session(session)` around a whole run (subpipeline runs included);
- `node(session, node)` around every node, wherever it runs;
- `event(session, event)` for every event the session emits.

The context managers run in the task doing the work, so context variables
(such as an OpenTelemetry span) nest the same way the graph does.

Without an instrument the cost is one `if` per node. Ready-made instruments
are in `stageflow.observability`.
"""
from __future__ import annotations

from contextlib import ExitStack, contextmanager
from typing import TYPE_CHECKING, Iterator

if TYPE_CHECKING:  # pragma: no cover
    from .event import Event
    from .nodes import Node
    from .session import Session


class Instrument:
    """Base class: all hooks do nothing. Override the ones you need."""

    @contextmanager
    def session(self, session: "Session") -> Iterator[None]:
        yield

    @contextmanager
    def node(self, session: "Session", node: "Node") -> Iterator[None]:
        yield

    def event(self, session: "Session", event: "Event") -> None:
        pass


class Instruments(Instrument):
    """Several instruments, entered in the given order."""

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
