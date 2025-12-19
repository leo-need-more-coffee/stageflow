from stageflow.core.stage import BaseStage, register_stage
from stageflow.core.jsonlogic import JsonLogic
from stageflow.core.context import Context


@register_stage("FilterListStage")
class FilterListStage(BaseStage):
    """
    description: "Filter list by JsonLogic condition"
    config:
      condition: object
      item_path: string
    """
    category = "builtin.lists"

    async def run(self):
        args = self.get_arguments()
        condition = self.config.get("condition")
        item_path = self.config.get("item_path", "item")
        if condition is None:
            raise ValueError("FilterListStage requires config.condition")
        items = args.get("items", [])
        if not isinstance(items, list):
            raise ValueError("FilterListStage expects list in arguments.items")
        base_payload = self.session.context.payload
        result = []
        for item in items:
            temp_ctx = Context(payload=dict(base_payload))
            temp_ctx.set(item_path, item)
            if JsonLogic(condition).evaluate(temp_ctx):
                result.append(item)
        self.set_outputs({"list": result})


@register_stage("UniqueListStage")
class UniqueListStage(BaseStage):
    """
    description: "Deduplicate list while preserving order"
    config: {}
    """
    category = "builtin.lists"

    async def run(self):
        args = self.get_arguments()
        items = args.get("items", [])
        if not isinstance(items, list):
            raise ValueError("UniqueListStage expects list in arguments.items")
        seen = set()
        out = []
        for item in items:
            if item not in seen:
                seen.add(item)
                out.append(item)
        self.set_outputs({"list": out})


@register_stage("PopListStage")
class PopListStage(BaseStage):
    """
    description: "Pop from list (default last)"
    config:
      index: int
    """
    category = "builtin.lists"

    async def run(self):
        args = self.get_arguments()
        index = args.get("index", self.config.get("index", -1))
        lst = args.get("items", [])
        if not isinstance(lst, list):
            raise ValueError("PopListStage expects list in arguments.items")
        if not lst:
            return
        value = lst.pop(index)
        self.set_outputs({"list": lst, "popped": value})
