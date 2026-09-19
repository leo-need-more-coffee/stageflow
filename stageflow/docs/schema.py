"""Генерация машиночитаемых описаний: pipeline JSON Schema и спеки стадий."""
from __future__ import annotations

import json
from functools import cache
from importlib import resources

import yaml


def _collect_specs(stage_registry: dict) -> dict:
    return {name: cls.get_specs() for name, cls in stage_registry.items()}


def generate_stages_yaml(stage_registry: dict) -> str:
    return yaml.dump(_collect_specs(stage_registry), allow_unicode=True, sort_keys=False, indent=2)


def generate_stages_json(stage_registry: dict) -> str:
    return json.dumps(_collect_specs(stage_registry), indent=2)


@cache
def _read_pipeline_schema() -> str:
    schema_file = resources.files("stageflow.docs.schemas").joinpath("pipeline.json")
    return schema_file.read_text(encoding="utf-8")


def load_pipeline_schema() -> dict:
    """Схема пайплайна из пакета. Файл читается один раз; каждый вызов отдаёт
    независимую копию — её можно безопасно дорабатывать (см. enum ниже)."""
    return json.loads(_read_pipeline_schema())


def generate_pipeline_schema(stage_registry: dict) -> dict:
    """Схема пайплайна с enum'ом имён зарегистрированных стадий: пайплайн,
    ссылающийся на несуществующую стадию, не пройдёт уже схемную проверку."""
    schema = load_pipeline_schema()
    schema["$defs"]["stage_node"]["properties"]["stage"]["enum"] = list(stage_registry)
    return schema
