from stageflow.core.stage import BaseStage, register_stage


@register_stage("SetValueStage")
class SetValueStage(BaseStage):
    """
    description: "Set a value at the given path"
    config:
      value: any
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
    description: "Copy value from src to dst path"
    config:
      default: any
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
    description: "Increment numeric value at path by delta"
    config:
      delta: number
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
    description: "Shallow merge dict from src into dst path"
    config:
      default_dst: object
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
