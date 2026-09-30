from __future__ import annotations

import json
from functools import cache
from importlib import resources

import yaml


def _collect_specs(stage_registry: dict, locale: str | None = None) -> dict:
    return {name: cls.get_specs(locale) for name, cls in stage_registry.items()}


def generate_stages_yaml(stage_registry: dict, locale: str | None = None) -> str:
    """The whole registry as YAML.

    With no `locale` the prose carries every language it has, which is what a
    client wants: one dump serves every reader, and choosing between them is the
    business of whatever draws the palette. Naming a locale collapses the prose
    to it — for a file meant to be read rather than drawn from.
    """
    return yaml.dump(_collect_specs(stage_registry, locale),
                     allow_unicode=True, sort_keys=False, indent=2)


def generate_stages_json(stage_registry: dict, locale: str | None = None) -> str:
    return json.dumps(_collect_specs(stage_registry, locale), indent=2, ensure_ascii=False)


@cache
def _read_pipeline_schema() -> str:
    schema_file = resources.files("stageflow.docs.schemas").joinpath("pipeline.json")
    return schema_file.read_text(encoding="utf-8")


def load_pipeline_schema() -> dict:
    return json.loads(_read_pipeline_schema())


def generate_pipeline_schema(stage_registry: dict) -> dict:
    schema = load_pipeline_schema()
    schema["$defs"]["stage_node"]["properties"]["stage"]["enum"] = list(stage_registry)
    return schema
