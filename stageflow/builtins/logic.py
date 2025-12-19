import asyncio
from stageflow.core.stage import BaseStage, register_stage
from stageflow.core.jsonlogic import JsonLogic


@register_stage("AssertStage")
class AssertStage(BaseStage):
    """
    description: "Validate JsonLogic condition against context, raise on failure"
    config:
      condition:
        type: object
        description: "JsonLogic condition to check"
      message:
        type: string
        description: "Error message when condition fails"
    outputs: {}
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
    description: "Always raise a runtime error with provided message"
    config:
      message:
        type: string
        description: "Message for raised error"
    outputs: {}
    """
    category = "builtin.logic"

    async def run(self):
        msg = self.config.get("message", "fail")
        raise RuntimeError(msg)


@register_stage("LogStage")
class LogStage(BaseStage):
    """
    description: "Emit log event with message and payload resolved from context paths"
    config:
      message:
        type: string
        description: "Log message"
      paths:
        type: object
        description: "Mapping of payload fields to context paths"
    outputs: {}
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
    description: "Async sleep for configured number of seconds"
    config:
      seconds:
        type: number
        description: "Duration to sleep in seconds"
    outputs: {}
    """
    category = "builtin.logic"

    async def run(self):
        sec = self.config.get("seconds", 0)
        await asyncio.sleep(sec)
