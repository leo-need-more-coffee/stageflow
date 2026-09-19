from ..core.stage import BaseStage, register_stage
from ._args import require_dict, require_list


@register_stage("PickKeysStage")
class PickKeysStage(BaseStage):
    """
    description: "Pick only specified keys from a dict and return new object"
    icon: "⊂"
    arguments:
      src:
        type: object
        description: "Source dict"
      keys:
        type: list
        description: "Keys to keep in the result"
    outputs:
      result:
        type: object
        description: "Dict containing only picked keys"
    """

    category = "builtin.dicts"

    async def run(self):
        args = self.get_arguments()
        src = require_dict("PickKeysStage", "src", args.get("src") or {})
        keys = require_list("PickKeysStage", "keys", args.get("keys") or [])
        self.set_outputs({"result": {k: src[k] for k in keys if k in src}})


@register_stage("DropKeysStage")
class DropKeysStage(BaseStage):
    """
    description: "Remove specified keys from dict and return cleaned object"
    icon: "⊘"
    arguments:
      src:
        type: object
        description: "Source dict"
      keys:
        type: list
        description: "Keys to remove from the dict"
    outputs:
      result:
        type: object
        description: "Dict without removed keys"
    """

    category = "builtin.dicts"

    async def run(self):
        args = self.get_arguments()
        src = require_dict("DropKeysStage", "src", args.get("src") or {})
        drop = set(require_list("DropKeysStage", "keys", args.get("keys") or []))
        self.set_outputs({"result": {k: v for k, v in src.items() if k not in drop}})
