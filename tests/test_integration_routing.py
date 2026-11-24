"""Integration: `condition` and `switch` choosing roads inside a real run.

The unit tests check a branching node on its own. Here it is one node among
others — after a stage that produced what it decides on, inside a `map` body,
inside `parallel` branches — and what is checked is the run as a whole.
"""
import asyncio
import unittest

from stageflow import BaseStage, Pipeline, Session, register_stage


@register_stage("RouteClassify")
class RouteClassify(BaseStage):
    """
    description: "Topic of a text, by keyword"
    arguments:
      text: string
    outputs:
      topic: string
    """

    async def run(self):
        text = self.get_arguments()["text"].lower()
        topic = ("billing" if "invoice" in text else
                 "tech" if "error" in text else "other")
        self.set_outputs({"topic": topic})


@register_stage("RouteLabel")
class RouteLabel(BaseStage):
    """
    description: "Returns the label it was given"
    arguments:
      label: string
    outputs:
      label: string
    """

    async def run(self):
        self.set_outputs({"label": self.get_arguments()["label"]})


def label(node_id, value, next_=None, out="routed"):
    node = {"id": node_id, "type": "stage", "stage": "RouteLabel",
            "arguments": {"const": {"label": value}}, "outputs": {"label": out}}
    if next_:
        node["next"] = next_
    return node


def run(data, **vars):
    pipeline = Pipeline.from_dict(data)
    pipeline.validate()
    return asyncio.run(Session(id="route", pipeline=pipeline,
                               context=pipeline_context(vars)).run())


def pipeline_context(vars):
    from stageflow import Context
    return Context(vars=vars)


SUPPORT = {
    "nodes": [
        {"id": "start", "type": "entry", "next": "classify"},
        {"id": "classify", "type": "stage", "stage": "RouteClassify",
         "arguments": {"vars": {"text": "text"}}, "outputs": {"topic": "topic"},
         "next": "route"},
        {"id": "route", "type": "switch",
         "cases": [{"when": "vars.topic == 'billing'", "next": "to_billing"},
                   {"when": "vars.topic == 'tech'", "next": "to_tech"}],
         "default": "to_human"},
        label("to_billing", "billing team", "done"),
        label("to_tech", "engineers", "done"),
        label("to_human", "a person", "done"),
        {"id": "done", "type": "terminal", "result": {"status": "routed"},
         "artifacts": ["routed", "topic"]},
    ],
}


class SwitchAfterAStageTests(unittest.TestCase):
    def test_each_topic_takes_its_road(self):
        cases = {
            "Where is my invoice?": ("billing", "billing team"),
            "I get an error on login": ("tech", "engineers"),
            "Hello there": ("other", "a person"),
        }
        for text, (topic, routed) in cases.items():
            with self.subTest(text=text):
                result = run(SUPPORT, text=text)
                self.assertEqual(result.artifacts, {"topic": topic, "routed": routed})

    def test_only_the_chosen_road_runs(self):
        result = run(SUPPORT, text="error")
        ran = [e.stage_id for e in result.history if e.type == "stage_completed"]
        self.assertEqual(ran, ["classify", "to_tech"])
        decided = [e for e in result.history if e.type == "switch_evaluated"]
        self.assertEqual(decided[0].payload["next"], "to_tech")


class ConditionInsideAMapTests(unittest.TestCase):
    PIPELINE = {
        "nodes": [
            {"id": "start", "type": "entry", "next": "each"},
            {"id": "each", "type": "map", "items": "vars.amounts", "body": "big",
             "item_var": "amount", "collect": {"routed": "labels"}, "next": "done"},
            {"id": "big", "type": "condition", "condition": "vars.amount >= 100",
             "then": "approve_by_hand", "else": "approve_auto"},
            label("approve_by_hand", "manual"),
            label("approve_auto", "auto"),
            {"id": "done", "type": "terminal", "artifacts": ["labels"]},
        ],
    }

    def test_every_item_takes_its_own_road(self):
        for mode in ("sequential", "parallel"):
            with self.subTest(mode=mode):
                data = {**self.PIPELINE, "nodes": [
                    {**n, "mode": mode} if n["id"] == "each" else n
                    for n in self.PIPELINE["nodes"]]}
                result = run(data, amounts=[5, 500, 99, 100])
                self.assertEqual(result.artifacts["labels"],
                                 ["auto", "manual", "auto", "manual"])


class SwitchInEveryBranchTests(unittest.TestCase):
    def test_branches_decide_on_the_same_frame_independently(self):
        data = {"nodes": [
            {"id": "start", "type": "entry", "next": "both"},
            {"id": "both", "type": "parallel", "branches": ["size", "sign"],
             "next": "done"},
            {"id": "size", "type": "switch",
             "cases": [{"when": "vars.n > 100", "next": "big"}], "default": "small"},
            label("big", "big", out="size_label"),
            label("small", "small", out="size_label"),
            {"id": "sign", "type": "condition", "condition": "vars.n < 0",
             "then": "neg", "else": "pos"},
            label("neg", "negative", out="sign_label"),
            label("pos", "positive", out="sign_label"),
            {"id": "done", "type": "terminal", "artifacts": ["size_label", "sign_label"]},
        ]}
        self.assertEqual(run(data, n=-5).artifacts,
                         {"size_label": "small", "sign_label": "negative"})
        self.assertEqual(run(data, n=1000).artifacts,
                         {"size_label": "big", "sign_label": "positive"})


class ARoadThatJustEndsTests(unittest.TestCase):
    def test_no_else_and_no_terminal_ends_quietly(self):
        data = {"nodes": [
            {"id": "start", "type": "entry", "next": "check"},
            {"id": "check", "type": "condition", "condition": "vars.go",
             "then": "work"},
            label("work", "done", "end"),
            {"id": "end", "type": "terminal", "result": {"status": "worked"}},
        ]}
        self.assertEqual(run(data, go=True).result, {"status": "worked"})
        quiet = run(data, go=False)
        self.assertIsNone(quiet.result)
        self.assertEqual(quiet.artifacts, {})


if __name__ == "__main__":
    unittest.main()
