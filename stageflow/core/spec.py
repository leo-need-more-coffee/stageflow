from __future__ import annotations

import copy
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any

import yaml

from ..i18n import translate, translate_all


@dataclass(frozen=True, slots=True)
class FieldSpec:
    name: str
    type: str = "any"
    optional: bool = False
    #: A string, or a `{locale: string}` mapping resolved by `build_stage_spec`.
    description: Any = ""
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
    """The spec declared in a docstring, or an empty dict. Returns a copy of
    the cached parse."""
    return copy.deepcopy(_parsed(doc))


@lru_cache(maxsize=None)
def _parsed(doc: str | None) -> dict[str, Any]:
    """Parse a docstring as a YAML spec. A docstring that is plain prose (not
    a YAML mapping) declares no spec, so the result is an empty dict."""
    if not doc:
        return {}
    try:
        parsed = yaml.safe_load(doc)
    except yaml.YAMLError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _prose(locale: str | None, domain: str | None):
    """A function that resolves translatable text: all languages when
    `locale` is None (see `translate_all`), one language otherwise."""
    if locale is None:
        return lambda value: translate_all(value, domain)
    return lambda value: translate(value, locale, domain)


def _translated(raw: Any, locale: str | None, domain: str | None) -> list[dict[str, Any]]:
    prose = _prose(locale, domain)
    fields = []
    for spec in parse_fields(raw):
        as_dict = spec.to_dict()
        as_dict["description"] = prose(as_dict.get("description", ""))
        fields.append(as_dict)
    return fields


def _described(specs: list[Any], locale: str | None, domain: str | None) -> list[dict[str, Any]]:
    """Event and input specs with their descriptions translated."""
    prose = _prose(locale, domain)
    out = []
    for spec in specs:
        as_dict = spec.to_dict()
        as_dict["description"] = prose(as_dict.get("description"))
        out.append(as_dict)
    return out


@dataclass(frozen=True)
class StageContract:
    """The parts of a stage spec the runtime needs: argument and output types
    and `reserve`. No translatable text."""

    arguments: dict[str, str | None]
    outputs: dict[str, str | None]
    reserve: dict[str, Any]


def stage_contract(stage_cls: type) -> StageContract:
    """The stage's contract, cached per class. Cheaper than `get_specs()`,
    which also builds every translation."""
    return _contract(stage_cls, stage_cls.__doc__)


@lru_cache(maxsize=None)
def _contract(stage_cls: type, doc: str | None) -> StageContract:
    raw = _parsed(doc)
    return StageContract(
        arguments={f.name: f.type for f in parse_fields(raw.get("arguments"))},
        outputs={f.name: f.type for f in parse_fields(raw.get("outputs"))},
        reserve=dict(raw.get("reserve") or {}),
    )


def build_stage_spec(stage_cls: type, locale: str | None = None) -> dict[str, Any]:
    """Build the stage's spec (the "card" the editor shows).

    Descriptions are translated with the stage's `i18n_domain`, or taken
    from `{locale: text}` mappings in the docstring. With `locale=None`
    every language is included and the client picks one; with a locale, the
    text is collapsed to that language.
    """
    doc = _parsed(stage_cls.__doc__)
    domain = getattr(stage_cls, "i18n_domain", None)
    return {
        "stage_name": stage_cls.stage_name,
        "skipable": stage_cls.skipable,
        "allowed_events": _described(stage_cls.allowed_events, locale, domain),
        "allowed_inputs": _described(stage_cls.allowed_inputs, locale, domain),
        "category": stage_cls.category,
        "icon": str(doc.get("icon", "") or ""),
        "icon_mono": bool(doc.get("icon_mono", False)),
        "color": doc.get("color") or None,
        "description": _prose(locale, domain)(doc.get("description", "")),
        "arguments": _translated(doc.get("arguments"), locale, domain),
        "outputs": _translated(doc.get("outputs"), locale, domain),
        # Amounts to reserve before the stage runs: {meter: number or CEL
        # over `args`}. Declared, so it can be shown and checked up front.
        "reserve": dict(doc.get("reserve") or {}),
        "timeout": stage_cls.timeout,
    }
