"""Стадии работы со значениями и словарями контекста."""
from ..core.stage import BaseStage, register_stage
from ..exceptions import StageContractError
from ._args import require_dict, require_number


@register_stage("SetValueStage")
class SetValueStage(BaseStage):
    """
    description: "Set value from arguments to the target path"
    icon: "="
    arguments:
      value:
        type: any
        description: "Value to set (literal via const, or a variable reference)"
    outputs:
      value:
        type: any
        description: "Value that was written"
    """

    category = "builtin.vars"

    async def run(self):
        args = self.get_arguments()
        if "value" not in args:
            raise StageContractError("SetValueStage: требуется аргумент 'value'")
        self.set_outputs({"value": args["value"]})


@register_stage("CopyValueStage")
class CopyValueStage(BaseStage):
    """
    description: "Copy value to output path, falling back to default when absent"
    icon: "⧉"
    arguments:
      value:
        type: any
        description: "Value to copy"
      default:
        type: any
        optional: true
        description: "Used when 'value' is missing or null"
    outputs:
      value:
        type: any
        description: "Copied value"
    """

    category = "builtin.vars"

    async def run(self):
        args = self.get_arguments()
        value = args.get("value")
        if value is None:
            value = args.get("default")
        self.set_outputs({"value": value})


@register_stage("IncrementStage")
class IncrementStage(BaseStage):
    """
    description: "Increment numeric value by delta"
    icon: "＋"
    arguments:
      current:
        type: number
        description: "Current numeric value"
      delta:
        type: number
        optional: true
        default: 1
        description: "Increment step"
    outputs:
      value:
        type: number
        description: "Result after increment"
    """

    category = "builtin.vars"

    async def run(self):
        args = self.get_arguments()
        current = require_number("IncrementStage", "current", args.get("current", 0))
        delta = require_number("IncrementStage", "delta", args.get("delta", 1))
        self.set_outputs({"value": current + delta})


@register_stage("MergeDictStage")
class MergeDictStage(BaseStage):
    """
    description: "Shallow merge src dict into dst"
    icon: "⋈"
    arguments:
      src:
        type: object
        description: "Dict with overrides"
      dst:
        type: object
        optional: true
        default: {}
        description: "Base dict to merge into (empty by default)"
    outputs:
      merged:
        type: object
        description: "Merged dict result"
    """

    category = "builtin.vars"

    async def run(self):
        args = self.get_arguments()
        src = require_dict("MergeDictStage", "src", args.get("src") or {})
        dst = require_dict("MergeDictStage", "dst", args.get("dst") or {})
        self.set_outputs({"merged": {**dst, **src}})
