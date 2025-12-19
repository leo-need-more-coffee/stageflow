from stageflow.core.stage import BaseStage, register_stage


@register_stage("AppendListStage")
class AppendListStage(BaseStage):
    """
    description: "Append value to a list (creates list if missing)"
    config:
      value: any
    """
    category = "builtin.lists"

    async def run(self):
        args = self.get_arguments()
        value = args.get("value", self.config.get("value"))
        lst = args.get("list", None)
        if lst is None:
            lst = []
        if not isinstance(lst, list):
            raise ValueError("AppendListStage expects list")
        lst.append(value)
        self.set_outputs({"list": lst})


@register_stage("ExtendListStage")
class ExtendListStage(BaseStage):
    """
    description: "Extend list with iterable from items"
    config:
      default_list: list
    """
    category = "builtin.lists"

    async def run(self):
        args = self.get_arguments()
        lst = args.get("list", self.config.get("default_list", []))
        if lst is None:
            lst = []
        if not isinstance(lst, list):
            raise ValueError("ExtendListStage expects list")
        src_val = args.get("items", [])
        if not isinstance(src_val, (list, tuple)):
            raise ValueError("Source for extend is not a list/tuple")
        lst.extend(src_val)
        self.set_outputs({"list": lst})
