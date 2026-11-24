"""`stageflow.testing`: the helpers a stage author tests with."""
import unittest

from stageflow import BaseStage, EventSpec, Limits, register_stage
from stageflow.core.budget import BudgetExceeded
from stageflow.core.stage import get_stages
from stageflow.testing import ContractViolation, isolated_stages, run_stage


@register_stage("HelperGreetStage")
class GreetStage(BaseStage):
    """
    description: "Greets someone, and charges per greeting"
    reserve:
      greetings: 1
    arguments:
      name:
        type: string
      shout:
        type: bool
        optional: true
    outputs:
      text:
        type: string
      length:
        type: int
    """

    allowed_events = [EventSpec("greeted", "Said hello")]

    async def run(self):
        args = self.get_arguments()
        text = f"Hello, {args['name']}"
        if args.get("shout"):
            text = text.upper()
        self.emit("greeted", {"to": args["name"]})
        self.charge(greetings=1)
        self.set_outputs({"text": text, "length": len(text)})


@register_stage("HelperApprovalStage")
class ApprovalStage(BaseStage):
    """
    description: "Waits for a person to approve"
    outputs:
      approved:
        type: bool
    """

    allowed_inputs = [EventSpec("approval", "The decision")]

    async def run(self):
        answer = await self.wait_input("approval", timeout=1)
        self.set_outputs({"approved": bool(answer and answer["payload"].get("ok"))})


@register_stage("HelperLiarStage")
class LiarStage(BaseStage):
    """
    description: "Breaks its own contract in the way it is told to"
    arguments:
      how:
        type: string
    outputs:
      count:
        type: int
      note:
        type: string
        optional: true
    """

    async def run(self):
        how = self.get_arguments()["how"]
        if how == "extra":
            self.set_outputs({"count": 1, "surprise": True})
        elif how == "missing":
            self.set_outputs({"note": "forgot the count"})
        elif how == "type":
            self.set_outputs({"count": "three"})
        else:
            self.set_outputs({"count": 3})


class RunStageTests(unittest.IsolatedAsyncioTestCase):
    async def test_outputs_events_and_charges(self):
        run = await run_stage(GreetStage, {"name": "Ann"})
        self.assertEqual(run.outputs, {"text": "Hello, Ann", "length": 10})
        self.assertEqual(run.emitted("greeted"), [{"to": "Ann"}])
        self.assertEqual(run.charged, {"greetings": 1})
        types = [event.type for event in run.events]
        self.assertLess(types.index("stage_started"), types.index("stage_completed"))

    async def test_a_stage_can_be_named_instead_of_imported(self):
        run = await run_stage("HelperGreetStage", {"name": "Bo", "shout": True})
        self.assertEqual(run.outputs["text"], "HELLO, BO")

    async def test_inputs_are_waiting_when_the_stage_asks(self):
        run = await run_stage(ApprovalStage, inputs=[{"type": "approval", "payload": {"ok": True}}])
        self.assertTrue(run.outputs["approved"])

    async def test_the_budget_is_the_real_one(self):
        with self.assertRaises(BudgetExceeded):
            await run_stage(GreetStage, {"name": "Ann"},
                            limits=Limits(counters={"greetings": 0}))
        run = await run_stage(GreetStage, {"name": "Ann"},
                              limits=Limits(counters={"greetings": 5}))
        self.assertEqual(run.meters["greetings"], 1)


class ContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_stage_that_keeps_its_word_passes(self):
        run = await run_stage(LiarStage, {"how": "honest"})
        self.assertEqual(run.outputs, {"count": 3})

    async def test_each_kind_of_broken_promise_is_named(self):
        cases = {
            "extra": "output 'surprise' is not declared",
            "missing": "required output 'count' is missing",
            "type": "expected int",
        }
        for how, message in cases.items():
            with self.subTest(how=how):
                with self.assertRaises(ContractViolation) as caught:
                    await run_stage(LiarStage, {"how": how})
                self.assertIn(message, str(caught.exception))

    async def test_arguments_are_held_to_the_spec_too(self):
        with self.assertRaisesRegex(ContractViolation, "argument 'nmae' is not declared"):
            await run_stage(GreetStage, {"nmae": "typo"})
        with self.assertRaisesRegex(ContractViolation, "required argument 'name' is missing"):
            await run_stage(GreetStage, {})
        with self.assertRaisesRegex(ContractViolation, "expected string"):
            await run_stage(GreetStage, {"name": 42})

    async def test_the_check_can_be_turned_off(self):
        run = await run_stage(LiarStage, {"how": "extra"}, check_contract=False)
        self.assertTrue(run.outputs["surprise"])


class IsolatedStagesTests(unittest.TestCase):
    def test_registrations_inside_are_forgotten(self):
        before = set(get_stages())
        for _ in range(2):  # the second round would fail without isolation
            with isolated_stages():
                @register_stage("HelperTemporaryStage")
                class Temporary(BaseStage):
                    async def run(self):
                        pass
                self.assertIn("HelperTemporaryStage", get_stages())
        self.assertEqual(set(get_stages()), before)


if __name__ == "__main__":
    unittest.main()
