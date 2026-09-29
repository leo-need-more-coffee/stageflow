import unittest

from stageflow import BaseStage, Context, Limits, Pipeline, Session, register_stage
from stageflow.exceptions import StageContractError


@register_stage("FixedPriceStage")
class FixedPriceStage(BaseStage):
    """
    description: "Reserves a flat amount and charges nothing"
    reserve:
      tokens: 10
      llm_calls: 1
    """

    async def run(self):
        self.set_outputs({"value": "done"})


@register_stage("ByArgsStage")
class ByArgsStage(BaseStage):
    """
    description: "Reserves by the length of what it is given"
    reserve:
      tokens: "size(args.text) * 2"
    """

    async def run(self):
        self.set_outputs({"value": "done"})


@register_stage("SettlingStage")
class SettlingStage(BaseStage):
    """
    description: "Reserves one amount and charges another"
    reserve:
      tokens: 100
    """

    async def run(self):
        self.set_outputs({"value": "done"})
        self.charge(tokens=self.get_arguments()["real"])


@register_stage("ExtraMeterStage")
class ExtraMeterStage(BaseStage):
    """
    description: "Charges a meter it never reserved"
    reserve:
      tokens: 5
    """

    async def run(self):
        self.set_outputs({"value": "done"})
        self.charge(tokens=5, crm_writes=3)


@register_stage("TwiceChargingStage")
class TwiceChargingStage(BaseStage):
    """Charges in two goes."""

    async def run(self):
        self.charge(tokens=10)
        self.charge(tokens=5, rows=1)
        self.set_outputs({"value": "done"})


@register_stage("FailingSpenderStage")
class FailingSpenderStage(BaseStage):
    """
    description: "Spends and then falls over"
    reserve:
      tokens: 40
    """

    charge_on_failure = False

    async def run(self):
        if FailingSpenderStage.charge_on_failure:
            self.charge(tokens=7)
        raise ValueError("the call went out and came back wrong")


@register_stage("BadPriceStage")
class BadPriceStage(BaseStage):
    """
    description: "Declares a price that is not a number"
    reserve:
      tokens: "args.text"
    """

    async def run(self):
        self.set_outputs({"value": "done"})


@register_stage("BrokenPriceStage")
class BrokenPriceStage(BaseStage):
    """
    description: "Declares a price that cannot even be compiled"
    reserve:
      tokens: "this is not CEL ((("
    """

    async def run(self):
        self.set_outputs({"value": "done"})


def _one(stage, arguments=None, retry=None):
    node = {"id": "a", "type": "stage", "stage": stage,
            "outputs": {"value": "v"}, "next": "done"}
    if arguments:
        node["arguments"] = arguments
    if retry:
        node["retry"] = retry
    return {"entry": "start", "nodes": [
        {"id": "start", "type": "entry", "next": "a"},
        node,
        {"id": "done", "type": "terminal", "result": {"status": "ok"},
         "artifacts": ["v"]},
    ]}


async def _run(data, limits=None, variables=None):
    session = Session(id="s", pipeline=Pipeline.from_dict(data),
                      context=Context(vars=variables or {}), limits=limits)
    return await session.run()


class ReserveTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_flat_reservation_is_charged(self):
        result = await _run(_one("FixedPriceStage"), Limits(counters={"tokens": 100}))
        self.assertEqual(result.meters["tokens"], 10)
        self.assertEqual(result.meters["llm_calls"], 1)

    async def test_a_reservation_is_an_expression_over_the_arguments(self):
        data = _one("ByArgsStage", {"const": {"text": "abcde"}})
        result = await _run(data, Limits(counters={"tokens": 100}))
        self.assertEqual(result.meters["tokens"], 10)

    async def test_the_expression_sees_arguments_taken_from_the_frame(self):
        data = _one("ByArgsStage", {"vars": {"text": "message"}})
        result = await _run(data, Limits(counters={"tokens": 100}),
                            {"message": "abcdefg"})
        self.assertEqual(result.meters["tokens"], 14)

    async def test_nothing_is_evaluated_when_nothing_is_limited(self):
        """A host with no budget pays for neither the CEL nor the lookup —
        which is why a price that cannot compile is harmless to it."""
        result = await _run(_one("BrokenPriceStage"))
        self.assertEqual(result.result, {"status": "ok"})
        self.assertNotIn("tokens", result.meters)

    async def test_a_price_that_is_not_a_number_is_a_contract_error(self):
        data = _one("BadPriceStage", {"const": {"text": "not a number"}})
        with self.assertRaises(StageContractError) as caught:
            await _run(data, Limits(counters={"tokens": 100}))
        self.assertIn("is not a number", str(caught.exception))

    async def test_a_price_that_cannot_be_evaluated_surfaces_as_itself(self):
        from stageflow.exceptions import ExpressionError

        with self.assertRaises(ExpressionError):
            await _run(_one("BrokenPriceStage"), Limits(counters={"tokens": 100}))


class SettleTests(unittest.IsolatedAsyncioTestCase):
    async def test_charging_less_than_reserved_gives_the_money_back(self):
        data = _one("SettlingStage", {"const": {"real": 30}})
        result = await _run(data, Limits(counters={"tokens": 1000}))
        self.assertEqual(result.meters["tokens"], 30)

    async def test_charging_more_than_reserved_is_the_amount_that_counts(self):
        data = _one("SettlingStage", {"const": {"real": 250}})
        result = await _run(data, Limits(counters={"tokens": 1000}))
        self.assertEqual(result.meters["tokens"], 250)

    async def test_charging_past_the_ceiling_stops_the_run_after_the_fact(self):
        """The overshoot that cannot be avoided: the call already happened."""
        data = _one("SettlingStage", {"const": {"real": 250}})
        result = await _run(data, Limits(counters={"tokens": 200}))
        self.assertEqual(result.result["status"], "budget_exceeded")
        self.assertEqual(result.meters["tokens"], 250)

    async def test_a_meter_charged_but_not_reserved_is_added(self):
        result = await _run(_one("ExtraMeterStage"), Limits(counters={"tokens": 50}))
        self.assertEqual(result.meters["tokens"], 5)
        self.assertEqual(result.meters["crm_writes"], 3)

    async def test_charging_twice_adds_up_within_one_run(self):
        result = await _run(_one("TwiceChargingStage"), Limits(counters={"tokens": 50}))
        self.assertEqual(result.meters["tokens"], 15)
        self.assertEqual(result.meters["rows"], 1)

    async def test_a_reserved_meter_the_stage_ignores_stays_reserved(self):
        result = await _run(_one("FixedPriceStage"), Limits(counters={"llm_calls": 5}))
        self.assertEqual(result.meters["llm_calls"], 1)

    async def test_the_settlement_is_announced(self):
        result = await _run(_one("TwiceChargingStage"), Limits(counters={"tokens": 50}))
        charged = [e for e in result.history if e.type == "stage_charged"]
        self.assertEqual(len(charged), 1)
        self.assertEqual(charged[0].payload["tokens"], 15)


class FailureTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        FailingSpenderStage.charge_on_failure = False

    async def test_a_failed_stage_keeps_its_reservation(self):
        """A call that went out and then timed out still spent what it spent;
        refunding it by default would be the optimistic lie."""
        session = Session(id="f", pipeline=Pipeline.from_dict(_one("FailingSpenderStage")),
                          context=Context(), limits=Limits(counters={"tokens": 1000}))
        with self.assertRaises(ValueError):
            await session.run()
        self.assertEqual(session.budget.spent("tokens"), 40)

    async def test_a_failing_stage_that_knows_better_charges_the_truth(self):
        FailingSpenderStage.charge_on_failure = True
        session = Session(id="f", pipeline=Pipeline.from_dict(_one("FailingSpenderStage")),
                          context=Context(), limits=Limits(counters={"tokens": 1000}))
        with self.assertRaises(ValueError):
            await session.run()
        self.assertEqual(session.budget.spent("tokens"), 7)

    async def test_a_retried_stage_pays_for_every_attempt(self):
        """Three attempts at a call are three calls, whatever the graph says."""
        data = _one("FailingSpenderStage",
                    retry=[{"error_equals": ["*"], "max_attempts": 3,
                            "interval_seconds": 0}])
        session = Session(id="r", pipeline=Pipeline.from_dict(data),
                          context=Context(), limits=Limits(counters={"tokens": 1000}))
        with self.assertRaises(ValueError):
            await session.run()
        self.assertEqual(session.budget.spent("tokens"), 120)


class GateTests(unittest.IsolatedAsyncioTestCase):
    async def test_the_stage_is_not_even_constructed_when_it_cannot_be_paid(self):
        started = []

        @register_stage("NoticingStage")
        class NoticingStage(BaseStage):
            """
            description: "Notices that it ran"
            reserve:
              tokens: 100
            """

            async def run(self):
                started.append(1)

        result = await _run(_one("NoticingStage"), Limits(counters={"tokens": 10}))
        self.assertEqual(started, [])
        self.assertEqual(result.result["status"], "budget_exceeded")
        self.assertEqual(result.result["meter"], "tokens")

    async def test_the_gate_does_not_spend_what_it_refused(self):
        session = Session(id="g", pipeline=Pipeline.from_dict(_one("FixedPriceStage")),
                          context=Context(), limits=Limits(counters={"tokens": 5}))
        await session.run()
        self.assertEqual(session.budget.spent("tokens"), 0)


if __name__ == "__main__":
    unittest.main()
