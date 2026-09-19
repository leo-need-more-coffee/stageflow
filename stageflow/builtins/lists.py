from ..core.context import Context
from ..core.stage import BaseStage, register_stage
from ..exceptions import StageContractError
from ._args import require_list


@register_stage("AppendListStage")
class AppendListStage(BaseStage):
    """
    description: "Append value to list (creates list if missing)"
    icon: "⊕"
    arguments:
      list:
        type: list
        optional: true
        default: []
        description: "List to append to (empty list if missing)"
      value:
        type: any
        description: "Value to append"
    outputs:
      list:
        type: list
        description: "Resulting list after append"
    """

    category = "builtin.lists"

    async def run(self):
        args = self.get_arguments()
        if "value" not in args:
            raise StageContractError("AppendListStage: argument 'value' is required")
        value = args["value"]
        base = require_list("AppendListStage", "list", args.get("list") or [])
        self.set_outputs({"list": [*base, value]})


@register_stage("ExtendListStage")
class ExtendListStage(BaseStage):
    """
    description: "Extend list with items from arguments"
    icon: "⊞"
    arguments:
      list:
        type: list
        optional: true
        default: []
        description: "Base list to extend (empty list if missing)"
      items:
        type: list
        description: "Items to extend the list with"
    outputs:
      list:
        type: list
        description: "Resulting list after extend"
    """

    category = "builtin.lists"

    async def run(self):
        args = self.get_arguments()
        base = require_list("ExtendListStage", "list", args.get("list") or [])
        items = args.get("items", [])
        if not isinstance(items, (list, tuple)):
            raise StageContractError(
                f"ExtendListStage: 'items' must be a list, got {type(items).__name__}"
            )
        self.set_outputs({"list": [*base, *items]})


@register_stage("FilterListStage")
class FilterListStage(BaseStage):
    """
    description: "Filter list items by CEL condition; current element is bound as `item`"
    icon: "▽"
    arguments:
      items:
        type: list
        description: "List to filter"
      condition:
        type: string
        description: "CEL condition evaluated per element, e.g. 'item > 2'"
      "*":
        type: any
        description: "Any other argument is visible to the condition as vars.<name>"
    outputs:
      list:
        type: list
        description: "Filtered list"
    """

    category = "builtin.lists"

    async def run(self):
        args = self.get_arguments()
        condition = args.pop("condition", None)
        if condition is None:
            raise StageContractError("FilterListStage: argument 'condition' is required")
        items = require_list("FilterListStage", "items", args.pop("items", []))
        scope = Context(vars=args)
        result = [item for item in items if self.session.cel.eval(condition, scope, item=item)]
        self.set_outputs({"list": result})


@register_stage("UniqueListStage")
class UniqueListStage(BaseStage):
    """
    description: "Deduplicate list while preserving original order"
    icon: "≠"
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
        items = require_list("UniqueListStage", "items", self.get_arguments().get("items", []))
        seen: set = set()
        unique = []
        for item in items:
            if item not in seen:
                seen.add(item)
                unique.append(item)
        self.set_outputs({"list": unique})


@register_stage("PopListStage")
class PopListStage(BaseStage):
    """
    description: "Pop element from list (default last) and return list+popped value"
    icon: "⊟"
    arguments:
      items:
        type: list
        description: "List to pop from"
      index:
        type: int
        optional: true
        default: -1
        description: "Index to pop, -1 means last element"
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
        items = require_list("PopListStage", "items", args.get("items", []))
        if not items:
            raise StageContractError("PopListStage: cannot pop from an empty list")
        index = args.get("index", -1)
        remaining = list(items)
        try:
            popped = remaining.pop(index)
        except IndexError:
            raise StageContractError(
                f"PopListStage: index {index} is out of range for a list of length {len(items)}"
            ) from None
        self.set_outputs({"list": remaining, "popped": popped})
