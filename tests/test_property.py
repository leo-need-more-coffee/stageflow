"""Property tests: invariants checked on generated inputs, not chosen examples.

Hypothesis builds hundreds of random pipelines (see `_graphs.py`) and random
values, and every one of them has to keep the promises below. When one does
not, Hypothesis shrinks it to the smallest graph that still breaks and prints
it — that is the bug report.

Skipped when Hypothesis is not installed (`pip install -e ".[test]"`).
"""
from __future__ import annotations

import asyncio
import json
import unittest

try:
    from hypothesis import HealthCheck, given, settings
    from hypothesis import strategies as st
except ImportError:  # pragma: no cover
    raise unittest.SkipTest("hypothesis is not installed")

from stageflow import Budget, BudgetExceeded, Context, Limits, Pipeline, Policy, Session
from stageflow.builtins.lists import UniqueListStage
from stageflow.exceptions import BranchError, StageFlowError

from _graphs import PropFailure, blocks, failing_blocks, pipeline_of
from _support import run_stage

GRAPHS = settings(max_examples=150, deadline=None,
                  suppress_health_check=[HealthCheck.too_slow])
RUN_TIMEOUT = 10


def run(data: dict, limits: Limits | None = None):
    """Run a generated pipeline; return (result, probe, unretrieved errors)."""
    pipeline = Pipeline.from_dict(data)
    policy = Policy(limits=limits or Limits())
    pipeline.validate(policy)
    unretrieved: list = []

    async def go():
        asyncio.get_running_loop().set_exception_handler(
            lambda loop, context: unretrieved.append(context.get("message")))
        session = Session(id="prop", pipeline=pipeline, policy=policy)
        result = await asyncio.wait_for(session.run(), RUN_TIMEOUT)
        return result, session.__dict__.get("probe")

    result, probe = asyncio.run(go())
    return result, probe, unretrieved


class GeneratedGraphTests(unittest.TestCase):
    @GRAPHS
    @given(blocks())
    def test_every_graph_validates_and_finishes(self, block):
        data, built = pipeline_of(block)
        result, _, unretrieved = run(data)
        self.assertEqual(result.result, {"status": "done"})
        self.assertEqual(unretrieved, [])

    @GRAPHS
    @given(blocks(), st.integers(1, 3))
    def test_the_concurrency_limit_holds_and_nothing_deadlocks(self, block, limit):
        data, _ = pipeline_of(block)
        result, probe, _ = run(data, Limits(gauges={"concurrency": limit}))
        self.assertEqual(result.result, {"status": "done"})
        if probe is not None:
            self.assertLessEqual(probe.peak, limit, "stages running at once")
        self.assertLessEqual(result.meters.get("peak_concurrency", 0), limit)

    @GRAPHS
    @given(blocks(), st.integers(1, 3))
    def test_the_outcome_does_not_depend_on_the_limit(self, block, limit):
        data, _ = pipeline_of(block)
        free, free_probe, _ = run(data)
        tight, tight_probe, _ = run(data, Limits(gauges={"concurrency": limit}))
        self.assertEqual(dict(free.context.vars), dict(tight.context.vars))
        self.assertEqual(free.meters["steps"], tight.meters["steps"])
        self.assertEqual(getattr(free_probe, "ran", 0), getattr(tight_probe, "ran", 0))

    @GRAPHS
    @given(blocks())
    def test_a_step_ceiling_is_exact(self, block):
        data, _ = pipeline_of(block)
        free, _, _ = run(data)
        needed = int(free.meters["steps"])

        enough, _, _ = run(data, Limits(counters={"steps": needed}))
        self.assertEqual(enough.result, {"status": "done"})

        try:
            short, _, _ = run(data, Limits(counters={"steps": needed - 1}))
        except StageFlowError:
            return  # refused at validation: the shortest road does not fit
        self.assertEqual(short.result["status"], "budget_exceeded")
        self.assertEqual(short.result["meter"], "steps")

    @GRAPHS
    @given(failing_blocks(), st.sampled_from([None, 1, 2]))
    def test_a_failure_is_caught_or_raised_never_lost(self, block, limit):
        data, _ = pipeline_of(block)
        limits = Limits(gauges={"concurrency": limit}) if limit else None
        try:
            result, _, unretrieved = run(data, limits)
        except (PropFailure, BranchError):
            return
        self.assertEqual(result.result, {"status": "done"})
        self.assertEqual(unretrieved, [], "an exception nobody looked at")

    @GRAPHS
    @given(failing_blocks())
    def test_whether_a_run_fails_does_not_depend_on_the_limit(self, block):
        data, _ = pipeline_of(block)

        def fails(limits):
            try:
                run(data, limits)
            except (PropFailure, BranchError):
                return True
            return False

        self.assertEqual(fails(None), fails(Limits(gauges={"concurrency": 1})))


# ------------------------------------------------------------------ loading

FIELDS = ["id", "type", "next", "stage", "arguments", "outputs", "condition", "then",
          "else", "cases", "default", "branches", "body", "except", "items",
          "subpipeline_id", "variables", "retry", "expose", "collect", "mode",
          "result", "artifacts", "when", "error_equals", "entry"]
TYPES = ["entry", "stage", "condition", "switch", "parallel", "try", "map",
         "subpipeline", "terminal", "nonsense", None, 3]
scalars = st.one_of(st.none(), st.booleans(), st.integers(-3, 3), st.text(max_size=4),
                    st.sampled_from(["start", "a", "b", "end", "SetValueStage", "vars.x",
                                     "true", "*"]))
json_values = st.recursive(
    scalars,
    lambda inner: st.one_of(st.lists(inner, max_size=3),
                            st.dictionaries(st.sampled_from(FIELDS), inner, max_size=4)),
    max_leaves=8,
)
nodes = st.fixed_dictionaries(
    {"id": st.sampled_from(["start", "a", "b", "end", "", 7]),
     "type": st.sampled_from(TYPES)},
    optional={name: json_values for name in FIELDS if name not in ("id", "type")},
)
documents = st.one_of(
    st.fixed_dictionaries({"nodes": st.lists(nodes, max_size=5)},
                          optional={"entry": scalars, "subpipelines": json_values,
                                    "variables": json_values, "types": json_values}),
    json_values,
)


class LoadingFuzzTests(unittest.TestCase):
    @settings(max_examples=600, deadline=None)
    @given(documents)
    def test_any_document_is_loaded_or_refused_never_crashes(self, document):
        try:
            pipeline = Pipeline.from_dict(document)
            pipeline.validate()
        except StageFlowError:
            pass  # refused, with one of the framework's own errors

    @settings(max_examples=200, deadline=None)
    @given(blocks(max_leaves=6), st.data())
    def test_a_valid_graph_with_one_field_broken_is_refused_cleanly(self, block, data):
        document, _ = pipeline_of(block)
        node = data.draw(st.sampled_from(document["nodes"]))
        field_name = data.draw(st.sampled_from(sorted(set(node) - {"id"}) or ["next"]))
        node[field_name] = data.draw(json_values)
        try:
            Pipeline.from_dict(json.loads(json.dumps(document))).validate()
        except StageFlowError:
            pass


# ------------------------------------------------------------------ models
# The real thing against the simplest thing that could be right.

ops = st.lists(st.tuples(st.sampled_from(["set", "drop"]), st.sampled_from("abcde"),
                         st.integers()), max_size=30)


class ModelTests(unittest.TestCase):
    @settings(max_examples=300, deadline=None)
    @given(ops)
    def test_the_frame_behaves_like_a_dict_that_never_changes_in_place(self, steps):
        ctx, model, history = Context(), {}, []
        for op, name, value in steps:
            history.append((ctx, dict(model)))
            if op == "set":
                ctx, model[name] = ctx.with_var(name, value), value
            else:
                ctx = ctx.without_var(name)
                model.pop(name, None)
            self.assertEqual(dict(ctx.vars), model)
        for old, snapshot in history:
            self.assertEqual(dict(old.vars), snapshot, "an older frame changed")

    @settings(max_examples=300, deadline=None)
    @given(st.integers(0, 50),
           st.lists(st.integers(-10, 20), max_size=20))
    def test_a_counter_fails_exactly_when_a_charge_crosses_the_limit(self, limit, charges):
        budget = Budget(Limits(counters={"m": limit}))
        total = 0
        for amount in charges:
            total += amount
            crossed = amount > 0 and total > limit
            try:
                budget.charge("m", amount)
                self.assertFalse(crossed, f"{total} of {limit} went through")
            except BudgetExceeded as over:
                self.assertTrue(crossed, f"{total} of {limit} was refused")
                self.assertEqual(over.spent, total)
            self.assertEqual(budget.spent("m"), total)

    @settings(max_examples=300, deadline=None)
    @given(st.lists(json_values, max_size=12))
    def test_unique_keeps_first_occurrences_in_order(self, items):
        got = asyncio.run(run_stage(UniqueListStage, items=items))["list"]

        def key(value):
            return json.dumps(value, sort_keys=True)

        keys = [key(v) for v in got]
        self.assertEqual(len(keys), len(set(keys)), "a duplicate survived")
        self.assertEqual(set(keys), {key(v) for v in items}, "something was lost")
        firsts = []
        for value in items:
            if key(value) not in [key(v) for v in firsts]:
                firsts.append(value)
        self.assertEqual([key(v) for v in got], [key(v) for v in firsts])
        again = asyncio.run(run_stage(UniqueListStage, items=got))["list"]
        self.assertEqual(again, got, "not idempotent")

    @settings(max_examples=200, deadline=None)
    @given(st.permutations(["a", "b", "c", "d"]), st.integers(0, 9))
    def test_entry_variables_bind_in_dependency_order_whatever_the_key_order(self, order, seed):
        exprs = {"a": "vars.seed + 1", "b": "vars.a * 2", "c": "vars.b + vars.a",
                 "d": "vars.c - vars.seed"}
        variables = {f"{name}.$": exprs[name] for name in order}
        variables["seed"] = seed
        data = {"nodes": [{"id": "start", "type": "entry", "variables": variables,
                           "next": "end"},
                          {"id": "end", "type": "terminal",
                           "artifacts": ["a", "b", "c", "d"]}]}
        result = asyncio.run(Session(id="e", pipeline=Pipeline.from_dict(data)).run())
        a = seed + 1
        b = a * 2
        c = b + a
        self.assertEqual(result.artifacts, {"a": a, "b": b, "c": c, "d": c - seed})


if __name__ == "__main__":
    unittest.main()
