# Observability

A session takes an optional **instrument** that sees every run, every node and
every event. Nothing is measured without one; with one, you get traces,
metrics or logs without touching a stage.

```python
from stageflow import Instruments, Session
from stageflow.observability import LoggingInstrument, Recorder
from stageflow.observability.otel import OpenTelemetryInstrument

session = Session(
    id="t-1001", pipeline=pipeline, policy=policy,
    instrument=Instruments(OpenTelemetryInstrument(), LoggingInstrument()),
)
```

A subpipeline's child run uses the parent's instrument, so nothing has to be
passed down.

## OpenTelemetry

```bash
pip install "stageflow-framework[otel]"
```

`OpenTelemetryInstrument()` uses the global tracer and meter providers, so it
reports wherever the host already sends telemetry — an OTLP collector, Jaeger,
Tempo, Prometheus. Pass `tracer_provider=` / `meter_provider=` to use others.

**Traces.** One span per run, `stageflow.session`, and one per node, named
`<type> <id>` (`stage classify`, `map each`). Spans nest the way the graph
does: the nodes of a `parallel` branch or a `map` item are children of that
node, a `try` body is inside the `try`, and a subpipeline's run is a child of
its `subpipeline` node — the whole run is one trace.

| On | Attributes and events |
|---|---|
| session span | `stageflow.session.id`, `stageflow.session.nested`, `stageflow.outcome` (finished, stopped, budget_exceeded, failed, …), `stageflow.meter.<name>` for every meter |
| node span | `stageflow.node.id`, `stageflow.node.type`, `stageflow.stage`; status ERROR with the exception recorded when the node raised; `stageflow.cancelled` when a failing sibling cancelled it |
| span events | every session event emitted inside the node: `stage_started`, `node_retry`, `try_caught`, `stage_charged`, `map_item_failed`, … with their payload as `stageflow.*` attributes |

**Metrics.**

| Name | Kind | Attributes |
|---|---|---|
| `stageflow.sessions` | counter | `outcome`, `nested` |
| `stageflow.session.duration` | histogram, seconds | `outcome`, `nested` |
| `stageflow.nodes` | counter | `type`, `stage`, `outcome` (`ok`, `cancelled` or the exception class) |
| `stageflow.node.duration` | histogram, seconds | `type`, `stage`, `outcome` |
| `stageflow.usage` | counter | `meter` — what top-level runs spent: `steps`, `tokens`, your own meters |

`stageflow.usage` is the bill: it adds up the same meters `SessionResult.meters`
reports, across every run.

!!! note "A block's time includes its contents"
    The duration of a `parallel`, `try`, `map` or `subpipeline` node is the
    time of everything inside it. For time spent in stages alone, filter
    `stageflow.node.duration` by `type="stage"`.

## Logs

`LoggingInstrument(logger="stageflow")` writes through the standard `logging`:

| Level | What |
|---|---|
| INFO | a run started; a run ended, with its outcome, duration, result and meters |
| ERROR | a run ended with an exception |
| DEBUG | every node with its outcome and duration; every event |

Every record has a `stageflow` attribute — a dict with `session`, and `node`,
`type`, `stage`, `outcome`, `seconds`, `meters` where they apply — for a JSON
formatter to emit as fields.

## In memory

`Recorder()` keeps counts and timings in the process: per node kind
(`map`, `parallel`, `stage:LoadTicketStage`), per run, the meters spent and
how many of each event.

```python
recorder = Recorder()
await Session(id="s", pipeline=pipeline, instrument=recorder).run()
print(recorder.table())
```

```
node                     count  errors    mean ms     max ms    total ms
stage:LoadTicketStage        1       0      0.412      0.412       0.412
switch                       1       0      0.081      0.081       0.081
...
```

`recorder.summary()` is the same as a dict. Useful in tests and benchmarks,
and for a quick look at where a run spends its time.

## Your own

Subclass `Instrument` and override what you need. Each hook is called in the
task that does the work, so context variables — a span, a request id — nest
the way the graph does.

```python
from contextlib import contextmanager
from stageflow import Instrument

class SlowNodes(Instrument):
    @contextmanager
    def node(self, session, node):
        started = time.perf_counter()
        yield
        if time.perf_counter() - started > 1:
            print(f"{node.id} took over a second")

    def event(self, session, event):
        ...
```

`session()` wraps a whole run, `node()` every node, `event()` sees every event.
`Instruments(a, b, ...)` combines several, entered in order and left in
reverse.
