from stageflow.core.stage import BaseStage, register_stage
from stageflow.core.jsonlogic import JsonLogic
from stageflow.core.context import Context


@register_stage("FilterListStage")
class FilterListStage(BaseStage):
    """
    description: "Filter list items by JsonLogic condition using item_path binding"
    arguments:
      items:
        type: list
        description: "List to filter"
    config:
      condition:
        type: object
        description: "JsonLogic condition evaluated for each item"
      item_path:
        type: string
        description: "Context key to bind current item during evaluation"
    outputs:
      list:
        type: list
        description: "Filtered list"
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
    description: "Deduplicate list while preserving original order"
    arguments:
      items:
        type: list
        description: "List to deduplicate"
    outputs:
      list:
        type: list
        description: "List with unique items"
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
    description: "Pop element from list (default last) and return list+popped value"
    arguments:
      items:
        type: list
        description: "List to pop from"
      index:
        type: int
        description: "Index to pop (overrides config)"
    config:
      index:
        type: int
        description: "Default index to pop, -1 means last element"
    outputs:
      list:
        type: list
        description: "List after pop"
      popped:
        type: any
        description: "Popped value"
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
