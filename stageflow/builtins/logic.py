import asyncio
from stageflow.core.stage import BaseStage, register_stage
from stageflow.core.jsonlogic import JsonLogic


@register_stage("AssertStage")
class AssertStage(BaseStage):
    """
    description: "Assert JsonLogic condition holds, else raise"
    config:
      condition: object
      message: string
    """
    category = "builtin.logic"

    async def run(self):
        condition = self.config.get("condition")
        if not condition:
            raise ValueError("AssertStage requires config.condition")
        ok = JsonLogic(condition).evaluate(self.session.context)
        if not ok:
            raise AssertionError(self.config.get("message", "assertion failed"))


@register_stage("FailStage")
class FailStage(BaseStage):
    """
    description: "Always raise an error with message"
    config:
      message: string
    """
    category = "builtin.logic"

    async def run(self):
        msg = self.config.get("message", "fail")
        raise RuntimeError(msg)


@register_stage("LogStage")
class LogStage(BaseStage):
    """
    description: "Emit event with payload from config or arguments"
    config:
      message: string
      paths: object
    """
    category = "builtin.logic"

    async def run(self):
        payload = {"message": self.config.get("message", "")}
        paths = self.config.get("paths", {})
        for key, path in paths.items():
            payload[key] = self.session.context.get(path)
        self.emit("log", payload)


@register_stage("SleepStage")
class SleepStage(BaseStage):
    """
    description: "Async sleep for given seconds"
    config:
      seconds: number
    """
    category = "builtin.logic"

    async def run(self):
        sec = self.config.get("seconds", 0)
        await asyncio.sleep(sec)
