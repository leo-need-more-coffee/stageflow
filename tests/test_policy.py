import unittest

from stageflow import (
    BaseStage,
    Context,
    Pipeline,
    Policy,
    Session,
    capabilities,
    register_stage,
)
from stageflow.exceptions import PipelineValidationError, PolicyViolationError


@register_stage("CheapStage")
class CheapStage(BaseStage):
    """A stage every plan has.

    outputs:
      value (str): a word
    """

    async def run(self):
        self.set_outputs({"value": "cheap"})


@register_stage("PriceyStage")
class PriceyStage(BaseStage):
    """A stage only the expensive plan has.

    outputs:
      value (str): a word
    """

    ran = 0

    async def run(self):
        PriceyStage.ran += 1
        self.set_outputs({"value": "pricey"})


CHEAP_ONLY = Policy(
    stages={"CheapStage"},
    node_types={"entry", "stage", "condition", "terminal"},
)


def _pipeline(stage="CheapStage"):
    return {
        "entry": "start",
        "nodes": [
            {"id": "start", "type": "entry", "next": "work"},
            {"id": "work", "type": "stage", "stage": stage,
             "outputs": {"value": "said"}, "next": "done"},
            {"id": "done", "type": "terminal", "result": {"status": "ok"},
             "artifacts": ["said"]},
        ],
    }


class PolicyShapeTests(unittest.TestCase):
    def test_none_and_the_empty_set_are_opposites(self):
        """The distinction the whole thing rests on: a policy that forgot to
        list its stages must not grant every stage the process imported."""
        self.assertTrue(Policy().allows_stage("PriceyStage"))
        self.assertFalse(Policy(stages=set()).allows_stage("PriceyStage"))
        self.assertTrue(Policy().allows_node_type("map"))
        self.assertFalse(Policy(node_types=set()).allows_node_type("map"))

    def test_a_bare_policy_is_unrestricted(self):
        self.assertTrue(Policy().unrestricted)
        self.assertFalse(CHEAP_ONLY.unrestricted)

    def test_iterables_are_frozen_on_the_way_in(self):
        mutable = {"CheapStage"}
        policy = Policy(stages=mutable)
        mutable.add("PriceyStage")
        self.assertFalse(policy.allows_stage("PriceyStage"))
        self.assertIsInstance(policy.stages, frozenset)


class PolicyValidationTests(unittest.TestCase):
    def test_a_forbidden_stage_is_a_validation_error(self):
        pipeline = Pipeline.from_dict(_pipeline("PriceyStage"))
        errors = pipeline.collect_errors(CHEAP_ONLY)
        self.assertIn("work: stage 'PriceyStage' is not allowed by the policy", errors)

    def test_a_forbidden_node_type_is_a_validation_error(self):
        data = _pipeline()
        data["nodes"][1]["next"] = "loop"
        data["nodes"].insert(2, {
            "id": "loop", "type": "map", "items": "vars.xs", "body": "inner",
            "item_var": "x", "next": "done"})
        data["nodes"].insert(3, {
            "id": "inner", "type": "stage", "stage": "CheapStage",
            "outputs": {"value": "said"}})
        errors = Pipeline.from_dict(data).collect_errors(CHEAP_ONLY)
        self.assertIn("loop: node type 'map' is not allowed by the policy", errors)

    def test_everything_refused_is_reported_at_once(self):
        """A tenant saving a graph wants the list, not the first offence."""
        data = _pipeline("PriceyStage")
        data["nodes"][1]["next"] = "branch"
        data["nodes"].insert(2, {
            "id": "branch", "type": "parallel",
            "branches": [{"id": "b", "entry": "done"}], "next": "done"})
        errors = Pipeline.from_dict(data).collect_errors(CHEAP_ONLY)
        self.assertIn("work: stage 'PriceyStage' is not allowed by the policy", errors)
        self.assertIn("branch: node type 'parallel' is not allowed by the policy", errors)

    def test_a_subpipeline_is_checked_at_save_time_too(self):
        """Without this the answer to "may I save this" is yes and the answer
        to "may I run it" is no — the same refusal, hours apart."""
        data = {
            "entry": "start",
            "nodes": [
                {"id": "start", "type": "entry", "next": "nested"},
                {"id": "nested", "type": "subpipeline", "subpipeline_id": "inner",
                 "next": "done"},
                {"id": "done", "type": "terminal", "result": {}},
            ],
            "subpipelines": {"inner": {"entry": "w", "nodes": [
                {"id": "w", "type": "stage", "stage": "PriceyStage", "next": "e"},
                {"id": "e", "type": "terminal", "result": {}},
            ]}},
        }
        policy = Policy(stages={"CheapStage"},
                        node_types={"entry", "stage", "subpipeline", "terminal"})
        self.assertIn(
            "[inner] w: stage 'PriceyStage' is not allowed by the policy",
            Pipeline.from_dict(data).collect_errors(policy),
        )

    def test_a_subpipeline_inside_a_subpipeline_is_checked(self):
        data = {
            "entry": "start",
            "nodes": [
                {"id": "start", "type": "entry", "next": "done"},
                {"id": "done", "type": "terminal", "result": {}},
            ],
            "subpipelines": {"outer": {
                "entry": "a", "nodes": [{"id": "a", "type": "terminal", "result": {}}],
                "subpipelines": {"deep": {"entry": "b", "nodes": [
                    {"id": "b", "type": "stage", "stage": "PriceyStage", "next": "c"},
                    {"id": "c", "type": "terminal", "result": {}},
                ]}},
            }},
        }
        self.assertIn(
            "[outer] [deep] b: stage 'PriceyStage' is not allowed by the policy",
            Pipeline.from_dict(data).collect_errors(CHEAP_ONLY),
        )

    def test_without_a_policy_nothing_is_refused(self):
        self.assertEqual(Pipeline.from_dict(_pipeline("PriceyStage")).collect_errors(), [])

    def test_validate_raises_with_the_policy_errors_inside(self):
        with self.assertRaises(PipelineValidationError) as caught:
            Pipeline.from_dict(_pipeline("PriceyStage")).validate(CHEAP_ONLY)
        self.assertIn("not allowed by the policy", str(caught.exception))


class PolicyAtRunTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        PriceyStage.ran = 0

    async def test_an_allowed_pipeline_runs(self):
        session = Session(id="ok", pipeline=Pipeline.from_dict(_pipeline()),
                          context=Context(), policy=CHEAP_ONLY)
        result = await session.run()
        self.assertEqual(result.artifacts["said"], "cheap")

    async def test_a_forbidden_stage_is_refused_before_the_session_starts(self):
        """Constructing the session validates: the graph never begins, so the
        stage is not reached rather than stopped on the way."""
        with self.assertRaises(PipelineValidationError):
            Session(id="no", pipeline=Pipeline.from_dict(_pipeline("PriceyStage")),
                    context=Context(), policy=CHEAP_ONLY)
        self.assertEqual(PriceyStage.ran, 0)

    async def test_the_same_pipeline_runs_under_a_wider_policy(self):
        wide = Policy(stages={"CheapStage", "PriceyStage"},
                      node_types={"entry", "stage", "terminal"})
        session = Session(id="wide", pipeline=Pipeline.from_dict(_pipeline("PriceyStage")),
                          context=Context(), policy=wide)
        result = await session.run()
        self.assertEqual(result.artifacts["said"], "pricey")
        self.assertEqual(PriceyStage.ran, 1)

    async def test_a_pipeline_built_after_validation_is_still_checked(self):
        """The static check is the good error; this is the one that holds when
        a host builds a graph by hand and never validates it."""
        session = Session(id="late", pipeline=Pipeline.from_dict(_pipeline()),
                          context=Context(), policy=CHEAP_ONLY)
        session.pipeline.get_node("work").stage = "PriceyStage"
        with self.assertRaises(PolicyViolationError):
            await session.run()
        self.assertEqual(PriceyStage.ran, 0)

    async def test_a_subpipeline_is_not_a_way_out_of_the_policy(self):
        """The allowance belongs to the tenant, not to the graph. A child
        graph is validated when its session is built, which is why the policy
        has to travel down with it."""
        data = {
            "entry": "start",
            "nodes": [
                {"id": "start", "type": "entry", "next": "nested"},
                {"id": "nested", "type": "subpipeline", "subpipeline_id": "inner",
                 "next": "done"},
                {"id": "done", "type": "terminal", "result": {"status": "ok"}},
            ],
            "subpipelines": {"inner": {
                "entry": "work",
                "nodes": [
                    {"id": "work", "type": "stage", "stage": "PriceyStage",
                     "outputs": {"value": "said"}, "next": "end"},
                    {"id": "end", "type": "terminal", "result": {}, "artifacts": ["said"]},
                ],
            }},
        }
        policy = Policy(stages={"CheapStage"},
                        node_types={"entry", "stage", "subpipeline", "terminal"})
        with self.assertRaises(PipelineValidationError) as caught:
            Session(id="nest", pipeline=Pipeline.from_dict(data),
                    context=Context(), policy=policy)
        self.assertIn("PriceyStage", str(caught.exception))
        self.assertEqual(PriceyStage.ran, 0)

    async def test_no_policy_means_business_as_usual(self):
        session = Session(id="open", pipeline=Pipeline.from_dict(_pipeline("PriceyStage")),
                          context=Context())
        result = await session.run()
        self.assertEqual(result.artifacts["said"], "pricey")


class PolicyCapabilitiesTests(unittest.TestCase):
    def test_capabilities_narrow_to_the_policy(self):
        answer = capabilities(CHEAP_ONLY)
        self.assertEqual(answer["node_types"],
                         ["condition", "entry", "stage", "terminal"])
        self.assertEqual(answer["stages"], 1)

    def test_capabilities_without_a_policy_describe_the_process(self):
        from stageflow.core.nodes import get_node_types

        self.assertEqual(capabilities()["node_types"], sorted(get_node_types()))


if __name__ == "__main__":
    unittest.main()
