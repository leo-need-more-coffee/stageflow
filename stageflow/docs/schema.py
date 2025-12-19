import yaml


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
