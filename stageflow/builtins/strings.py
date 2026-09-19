import string

from ..core.stage import BaseStage, register_stage
from ..exceptions import StageContractError
from ._args import require_list, require_present


class _NameOnlyFormatter(string.Formatter):
    def get_field(self, field_name, args, kwargs):
        if not field_name.isidentifier():
            raise StageContractError(
                f"TemplateStage: placeholder '{{{field_name}}}' is not an argument name; "
                "attributes, indexes and positional numbers are not allowed"
            )
        if field_name not in kwargs:
            raise StageContractError(
                f"TemplateStage: template references '{field_name}', "
                f"but there is no such argument (available: {sorted(kwargs)})"
            )
        return kwargs[field_name], field_name


@register_stage("ConcatStage")
class ConcatStage(BaseStage):
    """
    description: "Concatenate stringified parts with separator"
    icon: "⧺"
    arguments:
      parts:
        type: list
        description: "Values to concatenate (non-strings are stringified)"
      separator:
        type: string
        optional: true
        default: ""
        description: "Separator between parts"
    outputs:
      value:
        type: string
        description: "Concatenated string"
    """

    category = "builtin.strings"

    async def run(self):
        args = self.get_arguments()
        parts = require_list("ConcatStage", "parts", args.get("parts", []))
        separator = args.get("separator", "")
        self.set_outputs({"value": separator.join(str(part) for part in parts)})


@register_stage("TemplateStage")
class TemplateStage(BaseStage):
    """
    description: "Format template string with the stage's own arguments"
    icon: "{}"
    arguments:
      template:
        type: string
        description: "Template string; a placeholder is a bare argument name ({name})"
      "*":
        type: any
        description: "Any other argument becomes a template placeholder value"
    outputs:
      value:
        type: string
        description: "Rendered string"
    """

    category = "builtin.strings"

    async def run(self):
        args = self.get_arguments()
        template = require_present("TemplateStage", "template", args.pop("template", None))
        self.set_outputs({"value": _NameOnlyFormatter().vformat(template, (), args)})
