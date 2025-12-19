from stageflow.core.stage import BaseStage, register_stage


@register_stage("AppendListStage")
class AppendListStage(BaseStage):
    """
    description: "Append value from args/config to list (creates list if missing)"
    arguments:
      list:
        type: list
        description: "List to append to (from context)"
      value:
        type: any
        description: "Value to append (overrides config)"
    config:
      value:
        type: any
        description: "Fallback value when argument is missing"
    outputs:
      list:
        type: list
        description: "Resulting list after append"
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
    description: "Extend list with items from arguments (or default list)"
    arguments:
      list:
        type: list
        description: "Base list to extend"
      items:
        type: list
        description: "Items to extend the list with"
    config:
      default_list:
        type: list
        description: "Fallback list when argument is missing"
    outputs:
      list:
        type: list
        description: "Resulting list after extend"
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
