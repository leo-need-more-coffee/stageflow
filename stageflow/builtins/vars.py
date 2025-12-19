from stageflow.core.stage import BaseStage, register_stage


@register_stage("SetValueStage")
class SetValueStage(BaseStage):
    """
    description: "Set value from arguments or config to the target path"
    arguments:
      value:
        type: any
        description: "Value to set (overrides config)"
    config:
      value:
        type: any
        description: "Fallback value when argument is missing"
    outputs:
      value:
        type: any
        description: "Value that was written"
    """
    category = "builtin.vars"

    async def run(self):
        args = self.get_arguments()
        value = args.get("value", self.config.get("value"))
        if value is None and "value" not in args and "value" not in self.config:
            raise ValueError("SetValueStage requires value via arguments or config")
        self.set_outputs({"value": value})


@register_stage("CopyValueStage")
class CopyValueStage(BaseStage):
    """
    description: "Copy value from arguments (or default) to output path"
    arguments:
      value:
        type: any
        description: "Value to copy"
    config:
      default:
        type: any
        description: "Default value when argument is missing"
    outputs:
      value:
        type: any
        description: "Copied value"
    """
    category = "builtin.vars"

    async def run(self):
        args = self.get_arguments()
        value = args.get("value", None)
        if value is None:
            value = self.config.get("default")
        self.set_outputs({"value": value})


@register_stage("IncrementStage")
class IncrementStage(BaseStage):
    """
    description: "Increment numeric value by delta (from args or config)"
    arguments:
      current:
        type: number
        description: "Current numeric value"
      delta:
        type: number
        description: "Delta overriding config"
    config:
      delta:
        type: number
        description: "Default delta (1 if missing)"
    outputs:
      value:
        type: number
        description: "Result after increment"
    """
    category = "builtin.vars"

    async def run(self):
        args = self.get_arguments()
        current = args.get("current", 0)
        delta = args.get("delta", self.config.get("delta", 1))
        if not isinstance(current, (int, float)):
            raise ValueError("IncrementStage current is not numeric")
        self.set_outputs({"value": current + delta})


@register_stage("MergeDictStage")
class MergeDictStage(BaseStage):
    """
    description: "Shallow merge src dict into dst (config default if missing)"
    arguments:
      src:
        type: object
        description: "Dict with overrides"
      dst:
        type: object
        description: "Base dict to merge into"
    config:
      default_dst:
        type: object
        description: "Fallback base dict if dst is missing"
    outputs:
      merged:
        type: object
        description: "Merged dict result"
    """
    category = "builtin.vars"

    async def run(self):
        args = self.get_arguments()
        src_val = args.get("src") or {}
        dst_val = args.get("dst") or self.config.get("default_dst", {})
        if not isinstance(src_val, dict) or not isinstance(dst_val, dict):
            raise ValueError("MergeDictStage expects dict values")
        merged = {**dst_val, **src_val}
        self.set_outputs({"merged": merged})
