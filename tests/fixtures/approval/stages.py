"""The stages of the expense-approval system test — a host's own stages."""
from __future__ import annotations

from stageflow import BaseStage, InputSpec, register_stage

LIMIT_PER_LINE = 1000


@register_stage("CheckLine")
class CheckLine(BaseStage):
    """
    description: "Checks one line of an expense report"
    arguments:
      line: object
    outputs:
      verdict: string
    reserve:
      checks: 1
    """

    async def run(self):
        line = self.get_arguments()["line"]
        if line["amount"] < 0:
            raise ValueError(f"negative amount on '{line['what']}'")
        self.set_outputs({"verdict": "ok" if line["amount"] <= LIMIT_PER_LINE else "high"})


@register_stage("Total")
class Total(BaseStage):
    """
    description: "Adds the amounts up"
    arguments:
      lines: list
    outputs:
      total: number
    """

    async def run(self):
        self.set_outputs({"total": sum(line["amount"] for line in self.get_arguments()["lines"])})


@register_stage("Notify")
class Notify(BaseStage):
    """
    description: "Pretends to tell somebody"
    arguments:
      who: string
    outputs:
      sent: string
    """

    async def run(self):
        self.charge(messages=1)
        self.set_outputs({"sent": f"told {self.get_arguments()['who']}"})


@register_stage("AskApproval")
class AskApproval(BaseStage):
    """
    description: "Waits for a person to approve or reject"
    outputs:
      decision: string
    """

    allowed_inputs = [InputSpec(type="approval")]
    timeout = None

    async def run(self):
        answer = await self.wait_input("approval")
        self.set_outputs({"decision": answer["payload"]["decision"]})
