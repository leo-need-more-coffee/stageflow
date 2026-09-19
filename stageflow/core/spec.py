from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import yaml


@dataclass(frozen=True, slots=True)
class FieldSpec:
    name: str
    type: str = "any"
    optional: bool = False
    description: str = ""
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "type": self.type,
            "optional": self.optional,
            "description": self.description,
            **self.extra,
        }


def _field_from_entry(name: str, spec: Any) -> FieldSpec:
    if not isinstance(spec, dict):
        return FieldSpec(name=name, type=str(spec))
    known = {
        "type": spec.get("type", "any"),
        "optional": spec.get("optional", False),
        "description": spec.get("description", ""),
    }
    extra = {k: v for k, v in spec.items() if k not in ("name", *known)}
    return FieldSpec(name=name, extra=extra, **known)


def parse_fields(raw: Any) -> list[FieldSpec]:
    if not raw:
        return []
    if isinstance(raw, dict):
        return [_field_from_entry(name, spec) for name, spec in raw.items()]
    if not isinstance(raw, list):
        return []

    fields: list[FieldSpec] = []
    for item in raw:
        if isinstance(item, dict):
            if "name" in item:
                fields.append(_field_from_entry(item["name"], {k: v for k, v in item.items() if k != "name"}))
            elif len(item) == 1:
                name, spec = next(iter(item.items()))
                fields.append(_field_from_entry(name, spec))
        else:
            fields.append(FieldSpec(name=str(item)))
    return fields


def parse_docstring_spec(doc: str | None) -> dict[str, Any]:
    parsed = yaml.safe_load(doc) if doc else None
    return parsed if isinstance(parsed, dict) else {}


def build_stage_spec(stage_cls: type) -> dict[str, Any]:
    doc = parse_docstring_spec(stage_cls.__doc__)
    return {
        "stage_name": stage_cls.stage_name,
        "skipable": stage_cls.skipable,
        "allowed_events": [spec.to_dict() for spec in stage_cls.allowed_events],
        "allowed_inputs": [spec.to_dict() for spec in stage_cls.allowed_inputs],
        "category": stage_cls.category,
        "icon": str(doc.get("icon", "") or ""),
        "icon_mono": bool(doc.get("icon_mono", False)),
        "color": doc.get("color") or None,
        "description": doc.get("description", ""),
        "arguments": [f.to_dict() for f in parse_fields(doc.get("arguments"))],
        "outputs": [f.to_dict() for f in parse_fields(doc.get("outputs"))],
    }
