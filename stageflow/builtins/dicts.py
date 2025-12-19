from stageflow.core.stage import BaseStage, register_stage


@register_stage("PickKeysStage")
class PickKeysStage(BaseStage):
    """
    description: "Pick subset of keys from a dict into output"
    config:
      keys: list
    """
    category = "builtin.dicts"

    async def run(self):
        args = self.get_arguments()
        src_val = args.get("src") or {}
        keys = self.config.get("keys", [])
        if not isinstance(src_val, dict):
            raise ValueError("PickKeysStage expects dict in arguments.src")
        picked = {k: src_val[k] for k in keys if k in src_val}
        self.set_outputs({"result": picked})


@register_stage("DropKeysStage")
class DropKeysStage(BaseStage):
    """
    description: "Drop keys from dict and write result"
    config:
      keys: list
    """
    category = "builtin.dicts"

    async def run(self):
        args = self.get_arguments()
        keys = set(self.config.get("keys", []))
        src_val = args.get("src") or {}
        if not isinstance(src_val, dict):
            raise ValueError("DropKeysStage expects dict in arguments.src")
        cleaned = {k: v for k, v in src_val.items() if k not in keys}
        self.set_outputs({"result": cleaned})
