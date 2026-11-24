"""Integration: instruments watching real runs — Recorder, logging, OpenTelemetry."""
import asyncio
import logging
import unittest
from contextlib import contextmanager

from stageflow import (
    BaseStage,
    Instrument,
    Instruments,
    Limits,
    Pipeline,
    Policy,
    Session,
    register_stage,
)
from stageflow.observability import LoggingInstrument, Recorder


class ObsBroken(Exception):
    pass


@register_stage("ObsWork")
class ObsWork(BaseStage):
    """
    description: "Works for a moment; fails when told; charges a token"
    arguments:
      fail: boolean
      slow: boolean
    outputs:
      done: boolean
    """

    timeout = None

    async def run(self):
        args = self.get_arguments()
        await asyncio.sleep(1 if args.get("slow") else 0)
        if args.get("fail"):
            raise ObsBroken("told to")
        self.charge(tokens=5)
        self.set_outputs({"done": True})


def work(id_, next_=None, out=None, **args):
    node = {"id": id_, "type": "stage", "stage": "ObsWork",
            "arguments": {"const": args}, "outputs": {"done": out or id_}}
    if next_:
        node["next"] = next_
    return node


GRAPH = {
    "nodes": [
        {"id": "start", "type": "entry", "next": "fan"},
        {"id": "fan", "type": "parallel", "branches": ["left", "right"], "next": "guard"},
        work("left"),
        work("right"),
        {"id": "guard", "type": "try", "body": "boom",
         "except": [{"error_equals": ["ObsBroken"], "next": "recover"}], "next": "sub"},
        work("boom", fail=True),
        work("recover"),
        {"id": "sub", "type": "subpipeline", "subpipeline_id": "child", "next": "end"},
        {"id": "end", "type": "terminal", "result": {"status": "ok"}},
    ],
    "subpipelines": {"child": {"nodes": [
        {"id": "c_start", "type": "entry", "next": "c_work"},
        work("c_work", "c_end"),
        {"id": "c_end", "type": "terminal"},
    ]}},
}


def run(instrument, data=GRAPH, policy=None):
    session = Session(id="obs", pipeline=Pipeline.from_dict(data),
                      instrument=instrument, policy=policy)
    return asyncio.run(session.run())


class RecorderTests(unittest.TestCase):
    def test_counts_per_node_kind_and_stage(self):
        recorder = Recorder()
        result = run(recorder)
        self.assertEqual(result.result, {"status": "ok"})
        nodes = recorder.nodes
        self.assertEqual(nodes["stage:ObsWork"].count, 5)  # left right boom recover c_work
        self.assertEqual(nodes["stage:ObsWork"].outcomes, {"ok": 4, "ObsBroken": 1})
        self.assertEqual(nodes["parallel"].count, 1)
        self.assertEqual(nodes["entry"].count, 2, "the child's entry too")
        self.assertEqual(recorder.sessions["session"].outcomes, {"finished": 1})
        self.assertEqual(recorder.sessions["subpipeline"].outcomes, {"finished": 1})
        self.assertEqual(recorder.usage["tokens"], 20)
        self.assertEqual(recorder.events["try_caught"], 1)
        self.assertIn("stage:ObsWork", recorder.table())

    def test_a_block_takes_at_least_as_long_as_what_is_inside(self):
        recorder = Recorder()
        run(recorder)
        self.assertGreaterEqual(recorder.nodes["subpipeline"].total_seconds,
                                recorder.sessions["subpipeline"].total_seconds)

    def test_a_cancelled_branch_and_a_budget_are_told_apart(self):
        data = {"nodes": [
            {"id": "start", "type": "entry", "next": "fan"},
            {"id": "fan", "type": "parallel", "branches": ["bad", "slow"], "next": "end"},
            work("bad", fail=True),
            work("slow", slow=True),
            {"id": "end", "type": "terminal"},
        ]}
        recorder = Recorder()
        with self.assertRaises(Exception):
            run(recorder, data)
        self.assertEqual(recorder.nodes["stage:ObsWork"].outcomes,
                         {"ObsBroken": 1, "cancelled": 1})
        self.assertEqual(recorder.sessions["session"].outcomes, {"failed": 1})

        recorder = Recorder()
        result = run(recorder, GRAPH, Policy(limits=Limits(counters={"tokens": 7})))
        self.assertEqual(result.result["status"], "budget_exceeded")
        self.assertEqual(recorder.sessions["session"].outcomes, {"budget_exceeded": 1})


class CompositeTests(unittest.TestCase):
    def test_instruments_are_entered_in_order_and_left_in_reverse(self):
        calls = []

        class Tracking(Instrument):
            def __init__(self, name):
                self.name = name

            @contextmanager
            def session(self, session):
                calls.append(f"{self.name}+")
                yield
                calls.append(f"{self.name}-")

        data = {"nodes": [{"id": "start", "type": "entry", "next": "end"},
                          {"id": "end", "type": "terminal"}]}
        run(Instruments(Tracking("a"), None, Tracking("b")), data)
        self.assertEqual(calls, ["a+", "b+", "b-", "a-"])


class LoggingTests(unittest.TestCase):
    def test_records_carry_structured_fields(self):
        with self.assertLogs("stageflow.test", level="DEBUG") as logs:
            run(LoggingInstrument("stageflow.test"))
        messages = [r.getMessage() for r in logs.records]
        self.assertTrue(any(m == "session obs started" for m in messages))
        self.assertTrue(any(m.startswith("session obs finished in") for m in messages))
        failed = [r for r in logs.records
                  if getattr(r, "stageflow", {}).get("outcome") == "ObsBroken"]
        self.assertEqual(len(failed), 1)
        self.assertEqual(failed[0].stageflow["node"], "boom")
        end = [r for r in logs.records if r.getMessage().startswith("session obs finished")][0]
        self.assertEqual(end.stageflow["meters"]["tokens"], 20)

    def test_info_level_says_only_start_and_end(self):
        with self.assertLogs("stageflow.quiet", level="INFO") as logs:
            run(LoggingInstrument(logging.getLogger("stageflow.quiet")))
        self.assertTrue(all(r.levelno >= logging.INFO for r in logs.records))
        self.assertEqual(len(logs.records), 4, "two runs (one nested), start and end each")


try:
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
    from opentelemetry.trace import StatusCode

    from stageflow.observability.otel import OpenTelemetryInstrument
    HAVE_OTEL = True
except ImportError:  # pragma: no cover
    HAVE_OTEL = False


@unittest.skipUnless(HAVE_OTEL, "opentelemetry-sdk is not installed")
class OpenTelemetryTests(unittest.TestCase):
    def setUp(self):
        self.exporter = InMemorySpanExporter()
        tracer_provider = TracerProvider()
        tracer_provider.add_span_processor(SimpleSpanProcessor(self.exporter))
        self.reader = InMemoryMetricReader()
        self.instrument = OpenTelemetryInstrument(
            tracer_provider=tracer_provider,
            meter_provider=MeterProvider(metric_readers=[self.reader]))

    def spans(self):
        return {span.name: span for span in self.exporter.get_finished_spans()}

    def test_spans_nest_the_way_the_graph_does(self):
        run(self.instrument)
        spans = self.spans()
        parent = {name: s.parent.span_id if s.parent else None for name, s in spans.items()}
        sid = {name: s.context.span_id for name, s in spans.items()}
        sessions = [s for s in self.exporter.get_finished_spans()
                    if s.name == "stageflow.session"]
        root = next(s for s in sessions if not s.attributes["stageflow.session.nested"])
        child_run = next(s for s in sessions if s.attributes["stageflow.session.nested"])

        self.assertIsNone(root.parent)
        self.assertEqual(parent["parallel fan"], root.context.span_id)
        self.assertEqual(parent["stage left"], sid["parallel fan"])
        self.assertEqual(parent["stage right"], sid["parallel fan"])
        self.assertEqual(parent["stage boom"], sid["try guard"])
        self.assertEqual(parent["stage recover"], sid["try guard"])
        self.assertEqual(child_run.parent.span_id, sid["subpipeline sub"])
        self.assertEqual(parent["stage c_work"], child_run.context.span_id)
        self.assertEqual(len({s.context.trace_id for s in self.exporter.get_finished_spans()}), 1)

    def test_failures_and_events_are_on_the_right_span(self):
        run(self.instrument)
        spans = self.spans()
        boom = spans["stage boom"]
        self.assertEqual(boom.status.status_code, StatusCode.ERROR)
        self.assertEqual(boom.events[-1].name, "exception")
        self.assertIn("stage_failed", [e.name for e in boom.events])
        self.assertIn("try_caught", [e.name for e in spans["try guard"].events])
        charged = [e for e in spans["stage left"].events if e.name == "stage_charged"]
        self.assertEqual(charged[0].attributes["stageflow.tokens"], 5)
        self.assertEqual(spans["stage left"].attributes["stageflow.stage"], "ObsWork")
        root = [s for s in self.exporter.get_finished_spans()
                if s.name == "stageflow.session" and s.parent is None][0]
        self.assertEqual(root.attributes["stageflow.outcome"], "finished")
        self.assertEqual(root.attributes["stageflow.meter.tokens"], 20)

    def test_a_cancelled_sibling_is_marked_not_failed(self):
        data = {"nodes": [
            {"id": "start", "type": "entry", "next": "fan"},
            {"id": "fan", "type": "parallel", "branches": ["bad", "slow"], "next": "end"},
            work("bad", fail=True),
            work("slow", slow=True),
            {"id": "end", "type": "terminal"},
        ]}
        with self.assertRaises(Exception):
            run(self.instrument, data)
        spans = self.spans()
        self.assertTrue(spans["stage slow"].attributes["stageflow.cancelled"])
        self.assertNotEqual(spans["stage slow"].status.status_code, StatusCode.ERROR)
        self.assertEqual(spans["stage bad"].status.status_code, StatusCode.ERROR)

    def test_metrics(self):
        run(self.instrument)
        points = {}
        for resource in self.reader.get_metrics_data().resource_metrics:
            for scope in resource.scope_metrics:
                for metric in scope.metrics:
                    for point in metric.data.data_points:
                        key = (metric.name, tuple(sorted(point.attributes.items())))
                        points[key] = getattr(point, "value", None) or getattr(point, "count", None)
        self.assertEqual(points[("stageflow.nodes", (("outcome", "ok"), ("stage", "ObsWork"),
                                                     ("type", "stage")))], 4)
        self.assertEqual(points[("stageflow.nodes", (("outcome", "ObsBroken"),
                                                     ("stage", "ObsWork"), ("type", "stage")))], 1)
        self.assertEqual(points[("stageflow.usage", (("meter", "tokens"),))], 20)
        self.assertEqual(points[("stageflow.sessions", (("nested", False),
                                                        ("outcome", "finished")))], 1)
        self.assertEqual(points[("stageflow.session.duration", (("nested", True),
                                                                ("outcome", "finished")))], 1)


if __name__ == "__main__":
    unittest.main()
