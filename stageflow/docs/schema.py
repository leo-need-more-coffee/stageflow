import yaml
import json
from importlib import resources


def generate_stages_yaml(stage_registry: dict) -> str:
    stages_doc = {}
    for name, cls in stage_registry.items():
        specs = cls.get_specs()
        stages_doc[name] = specs
    return yaml.dump(stages_doc, allow_unicode=True, sort_keys=False, indent=2)


def generate_stages_json(stage_registry: dict) -> str:
    import json

    stages_doc = {}
    for name, cls in stage_registry.items():
        specs = cls.get_specs()
        stages_doc[name] = specs
    return json.dumps(stages_doc, indent=2)


def load_pipeline_schema() -> dict:
    with resources.files("stageflow.docs.schemas").joinpath("pipeline.json").open("r", encoding="utf-8") as f:
        return json.load(f)


def generate_pipeline_schema(stage_registry: dict) -> dict:
    """Return pipeline JSON Schema with stage names enum-injected."""
    schema = load_pipeline_schema()
    stage_names = list(stage_registry.keys())
    # Inject enum into stage node definition.
    try:
        schema["$defs"]["stage_node"]["properties"]["stage"]["enum"] = stage_names
    except Exception:
        pass
    return schema
