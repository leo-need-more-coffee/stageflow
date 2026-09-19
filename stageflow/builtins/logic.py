import asyncio

from ..core.context import Context
from ..core.stage import BaseStage, register_stage
from ._args import require_number, require_present


@register_stage("AssertStage")
class AssertStage(BaseStage):
    """
    description: "Validate CEL condition against stage arguments, raise on failure"
    icon: "✓"
    arguments:
      condition:
        type: string
        description: "CEL condition; other arguments of the stage are visible as vars.*"
      message:
        type: string
        optional: true
        default: "assertion failed"
        description: "Error message when condition fails"
      "*":
        type: any
        description: "Any other argument is visible to the condition as vars.<name>"
    outputs: {}
    """

    category = "builtin.logic"

    async def run(self):
        args = self.get_arguments()
        condition = require_present("AssertStage", "condition", args.pop("condition", None))
        message = args.pop("message", "assertion failed")
        scope = Context(vars=args)
        if not self.session.cel.eval(condition, scope):
            raise AssertionError(message)


@register_stage("FailStage")
class FailStage(BaseStage):
    """
    description: "Always raise a runtime error with provided message"
    icon: "✕"
    arguments:
      message:
        type: string
        optional: true
        default: "fail"
        description: "Message for raised error"
    outputs: {}
    """

    category = "builtin.logic"

    async def run(self):
        raise RuntimeError(self.get_arguments().get("message", "fail"))


@register_stage("LogStage")
class LogStage(BaseStage):
    """
    description: "Emit log event with message and the stage's own arguments"
    icon: "≡"
    arguments:
      message:
        type: string
        optional: true
        default: ""
        description: "Log message"
      "*":
        type: any
        description: "Anything else passed in arguments is logged as-is"
    outputs: {}
    """

    category = "builtin.logic"

    async def run(self):
        self.emit("log", {"message": "", **self.get_arguments()})


@register_stage("SleepStage")
class SleepStage(BaseStage):
    """
    description: "Async sleep for the given number of seconds"
    icon: "⏱"
    arguments:
      seconds:
        type: number
        optional: true
        default: 0
        description: "Duration to sleep in seconds"
    outputs: {}
    """

    category = "builtin.logic"

    async def run(self):
        seconds = require_number("SleepStage", "seconds", self.get_arguments().get("seconds", 0))
        await asyncio.sleep(seconds)
