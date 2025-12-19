from stageflow.core.stage import BaseStage, register_stage


@register_stage("ConcatStage")
class ConcatStage(BaseStage):
    """
    description: "Concatenate list of strings/numbers into a single string"
    config:
      parts: list
      separator: string
      output_key: string
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
    description: "Format string with context values using {path} placeholders"
    config:
      template: string
      output_key: string
      values: object
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
