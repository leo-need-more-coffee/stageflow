from stageflow.core.stage import BaseStage, register_stage


@register_stage("ConcatStage")
class ConcatStage(BaseStage):
    """
    description: "Concatenate parts (values or context paths) with separator"
    arguments:
      parts:
        type: list
        description: "List of values or context paths to concatenate"
      separator:
        type: string
        description: "Separator overriding config"
    config:
      parts:
        type: list
        description: "Default parts when argument is missing"
      separator:
        type: string
        description: "Default separator"
      output_key:
        type: string
        description: "Context key for resulting string"
    outputs:
      value:
        type: string
        description: "Concatenated string"
    """
    category = "builtin.strings"

    async def run(self):
        args = self.get_arguments()
        parts = args.get("parts", self.config.get("parts", []))
        sep = args.get("separator", self.config.get("separator", ""))
        out_key = self.config.get("output_key", "value")
        values = []
        for p in parts:
            if isinstance(p, str):
                values.append(str(self.session.context.get(p, p)))
            else:
                values.append(str(p))
        self.set_outputs({out_key: sep.join(values)})


@register_stage("TemplateStage")
class TemplateStage(BaseStage):
    """
    description: "Format template string with values pulled from context paths"
    arguments:
      template:
        type: string
        description: "Template overriding config"
    config:
      template:
        type: string
        description: "Default template string"
      output_key:
        type: string
        description: "Context key for rendered value"
      values:
        type: object
        description: "Mapping placeholder -> context path"
    outputs:
      value:
        type: string
        description: "Rendered string"
    """
    category = "builtin.strings"

    async def run(self):
        args = self.get_arguments()
        template = self.config.get("template") or args.get("template")
        out_key = self.config.get("output_key", "value")
        values_cfg = self.config.get("values", {})
        if template is None:
            raise ValueError("TemplateStage requires template")
        values = {}
        for key, path in values_cfg.items():
            values[key] = self.session.context.get(path)
        self.set_outputs({out_key: template.format(**values)})
