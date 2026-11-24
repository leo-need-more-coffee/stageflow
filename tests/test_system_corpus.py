"""System: a corpus of involved pipelines, each with what it must do.

Every file in `fixtures/corpus/` is one pipeline and a list of runs. A run
says what to seed the frame with, which policy the host applies, what a person
answers when asked, and what must come out: the result, the artifacts, an
error, a refusal at validation, the meters, the events. Adding a case is
adding a file; nothing here changes.

The pipelines go through the public API only — `Pipeline.from_dict`,
`validate`, `Session` — the way a host runs them.

    python -m unittest tests/test_system_corpus.py      # all of them
    CORPUS=07 python -m unittest tests/test_system_corpus.py   # the ones matching
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import unittest
from collections import Counter
from pathlib import Path

from stageflow import (
    BaseStage,
    Context,
    InputSpec,
    Limits,
    MemoryStore,
    Pipeline,
    Policy,
    Session,
    get_node_types,
    register_stage,
)
from stageflow.exceptions import PipelineValidationError

CORPUS = Path(__file__).resolve().parent / "fixtures" / "corpus"
RUN_TIMEOUT = 15


# ------------------------------------------------------------------ stages
# What the built-in stages cannot do: fail on purpose, spend, ask a person.


class CorpusFlakyError(Exception):
    pass


@register_stage("CorpusFlaky")
class CorpusFlaky(BaseStage):
    """
    description: "Fails the first `fail_times` calls for a key in this run"
    arguments:
      key: string
      fail_times: integer
    outputs:
      attempts: integer
    """

    async def run(self):
        args = self.get_arguments()
        counts = self.session.__dict__.setdefault("_corpus_calls", Counter())
        counts[args["key"]] += 1
        if counts[args["key"]] <= args["fail_times"]:
            raise CorpusFlakyError(f"{args['key']}: attempt {counts[args['key']]}")
        self.set_outputs({"attempts": counts[args["key"]]})


@register_stage("CorpusSpend")
class CorpusSpend(BaseStage):
    """
    description: "Spends the tokens it is given"
    arguments:
      tokens: integer
    outputs:
      tokens: integer
    reserve:
      tokens: "args.tokens"
    """

    async def run(self):
        tokens = self.get_arguments()["tokens"]
        self.charge(tokens=tokens)
        self.set_outputs({"tokens": tokens})


@register_stage("CorpusAsk")
class CorpusAsk(BaseStage):
    """
    description: "Waits for a person's answer"
    outputs:
      text: string
    """

    allowed_inputs = [InputSpec(type="answer")]
    timeout = None

    async def run(self):
        answer = await self.wait_input("answer")
        self.set_outputs({"text": answer["payload"]["text"]})


@register_stage("CorpusBoom")
class CorpusBoom(BaseStage):
    """
    description: "Refuses negative numbers"
    arguments:
      value: integer
    outputs:
      value: integer
    """

    async def run(self):
        value = self.get_arguments()["value"]
        if value < 0:
            raise ValueError(f"negative: {value}")
        self.set_outputs({"value": value})


# ------------------------------------------------------------------ running


def policy_of(spec: dict | None) -> Policy | None:
    if spec is None:
        return None
    limits = spec.get("limits") or {}
    return Policy(
        stages=spec.get("stages"),
        node_types=spec.get("node_types"),
        allow_cycles=spec.get("allow_cycles", False),
        limits=Limits(counters=limits.get("counters", {}),
                      gauges=limits.get("gauges", {}),
                      max_retries=limits.get("max_retries"),
                      max_delay_seconds=limits.get("max_delay_seconds")),
    )


async def execute(pipeline: Pipeline, run: dict, policy: Policy | None):
    if run.get("durable"):
        return await execute_durably(pipeline, run, policy)
    session = Session(id="corpus", pipeline=pipeline,
                      context=Context(vars=run.get("vars", {})), policy=policy)
    task = asyncio.create_task(session.run())
    for given in run.get("inputs", []):
        while not session.is_waiting_for(given["type"]):
            if task.done():
                break
            await asyncio.sleep(0.005)
        if task.done():
            break
        await session.input(given["type"], given["payload"])
    return await asyncio.wait_for(task, RUN_TIMEOUT)


async def execute_durably(pipeline: Pipeline, run: dict, policy: Policy | None):
    """The run with a store: every time it stops at a wait, it is restored
    as a new session (as in another process) and given the next input."""
    store = MemoryStore()
    result = await Session(id="corpus", pipeline=pipeline, policy=policy,
                           context=Context(vars=run.get("vars", {})), store=store).run()
    for given in run.get("inputs", []):
        if result.waiting is None:
            break
        session = await Session.restore(store, "corpus", policy=policy or Policy())
        await session.input(given["type"], given["payload"])
        result = await asyncio.wait_for(session.run(), RUN_TIMEOUT)
    return result


def cases() -> list[tuple[str, dict]]:
    only = os.environ.get("CORPUS")
    return [(path.stem, json.loads(path.read_text("utf-8")))
            for path in sorted(CORPUS.glob("*.json"))
            if not only or only in path.stem]


class CorpusTests(unittest.TestCase):
    maxDiff = None

    def test_every_case(self):
        found = cases()
        self.assertTrue(found, f"no cases in {CORPUS}")
        for name, case in found:
            for run in case["runs"]:
                with self.subTest(case=name, run=run["name"]):
                    self.check_run(case, run)

    def check_run(self, case: dict, run: dict) -> None:
        expect = run["expect"]
        policy = policy_of(run.get("policy"))
        pipeline = Pipeline.from_dict(case["pipeline"])

        if "invalid" in expect:
            with self.assertRaises(PipelineValidationError) as caught:
                pipeline.validate(policy)
            text = str(caught.exception)
            for fragment in expect["invalid"]:
                self.assertIn(fragment, text)
            return
        pipeline.validate(policy)

        if "error" in expect:
            with self.assertRaises(Exception) as caught:
                asyncio.run(execute(pipeline, run, policy))
            self.assertEqual(type(caught.exception).__name__, expect["error"]["type"],
                             str(caught.exception))
            self.assertRegex(str(caught.exception), expect["error"].get("match", ""))
            return

        result = asyncio.run(execute(pipeline, run, policy))
        if "waiting_at" in expect:
            self.assertEqual((result.waiting or {}).get("node"), expect["waiting_at"])
        if "result" in expect:
            if expect["result"] is not None and "status" in expect["result"] \
                    and result.result and result.result.get("status") == "budget_exceeded":
                # a budget result also carries numbers; compare what was named
                self.assertEqual({k: result.result.get(k) for k in expect["result"]},
                                 expect["result"])
            else:
                self.assertEqual(result.result, expect["result"])
        if "artifacts" in expect:
            self.assertEqual(result.artifacts, expect["artifacts"])
        for key, value in expect.get("artifacts_include", {}).items():
            self.assertIn(key, result.artifacts)
            self.assertEqual(result.artifacts[key], value, key)
        for key, pattern in expect.get("artifacts_match", {}).items():
            self.assertRegex(str(result.artifacts.get(key)), pattern)
        for meter, value in expect.get("meters", {}).items():
            self.assertEqual(result.meters.get(meter), value, meter)
        for meter, ceiling in expect.get("meters_at_most", {}).items():
            self.assertLessEqual(result.meters.get(meter, 0), ceiling, meter)

        events = expect.get("events", {})
        types = [event.type for event in result.history]
        for type_ in events.get("include", []):
            self.assertIn(type_, types)
        for type_ in events.get("exclude", []):
            self.assertNotIn(type_, types)
        counted = Counter(types)
        for type_, count in events.get("count", {}).items():
            self.assertEqual(counted[type_], count, type_)


class CorpusShapeTests(unittest.TestCase):
    """The corpus itself: well-formed, and wide enough to be worth having."""

    def test_every_file_has_a_description_and_runs(self):
        for name, case in cases():
            with self.subTest(case=name):
                self.assertTrue(case.get("description"))
                self.assertTrue(case.get("runs"))
                for run in case["runs"]:
                    self.assertTrue(run.get("name"))
                    self.assertTrue(run.get("expect"))

    def test_together_they_use_every_node_type(self):
        if os.environ.get("CORPUS"):
            self.skipTest("a filtered corpus")
        used = set()
        for _, case in cases():
            graphs = [case["pipeline"], *case["pipeline"].get("subpipelines", {}).values()]
            used |= {node["type"] for graph in graphs for node in graph["nodes"]}
        self.assertEqual(used, set(get_node_types()))

    def test_together_they_use_every_built_in_stage(self):
        if os.environ.get("CORPUS"):
            self.skipTest("a filtered corpus")
        from stageflow import get_stages_by_category
        builtin = {cls.stage_name for category, stages in get_stages_by_category().items()
                   if category.startswith("builtin") for cls in stages}
        used = set()
        for _, case in cases():
            graphs = [case["pipeline"], *case["pipeline"].get("subpipelines", {}).values()]
            used |= {node.get("stage") for graph in graphs for node in graph["nodes"]}
        self.assertEqual(builtin - used, set())

    def test_names_are_numbered_in_order(self):
        names = [name for name, _ in cases()]
        self.assertEqual(names, sorted(names))
        self.assertTrue(all(re.match(r"^\d{2}-[a-z0-9-]+$", n) for n in names))


if __name__ == "__main__":
    unittest.main()
