from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import yaml

from ..i18n import translate, translate_all


@dataclass(frozen=True, slots=True)
class FieldSpec:
    name: str
    type: str = "any"
    optional: bool = False
    #: prose, and prose is translatable: as written in the docstring this is a
    #: string, or a `{locale: string}` mapping that `build_stage_spec` resolves
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
    """The spec a stage declares in its docstring, or nothing.

    A docstring is first of all a docstring: plenty of them are prose, and
    prose is rarely valid YAML. That is not an error — the stage simply
    declares no spec. Raising here would make `get_specs()` unusable on any
    stage documented in sentences, which is why every caller used to wrap it
    in `except Exception` and swallow the difference between "no spec" and
    "the parser fell over".
    """
    if not doc:
        return {}
    try:
        parsed = yaml.safe_load(doc)
    except yaml.YAMLError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _prose(locale: str | None, domain: str | None):
    """How one piece of prose in a spec is resolved.

    No locale asked for means the spec is not being built for a reader, so every
    language goes in it and whatever draws it chooses — see `translate_all`. A
    locale asked for means one reader, one string.
    """
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
    """Event and input specs, with their one line of prose resolved."""
    prose = _prose(locale, domain)
    out = []
    for spec in specs:
        as_dict = spec.to_dict()
        as_dict["description"] = prose(as_dict.get("description"))
        out.append(as_dict)
    return out


def build_stage_spec(stage_cls: type, locale: str | None = None) -> dict[str, Any]:
    """The card an editor draws this stage from.

    Everything here but the prose is an identifier: a name, a type, a colour, a
    meter. The prose — the stage's description and the description of every
    argument, output, event and input — comes either out of the catalog of the
    stage's `i18n_domain` or out of a per-locale mapping written in the
    docstring. A stage with neither reads the same in every language, which is
    the correct answer for a stage nobody has translated.

    `locale=None`, the default, puts EVERY language in the spec as a `{locale:
    text}` mapping and leaves the choosing to whatever draws it. That is what an
    editor wants: it holds one copy of the specs and its reader may pick a
    language long after they were fetched, so a backend that had chosen for them
    would have to be asked again. Naming a locale collapses the prose to that
    one language, for a caller that really is answering one reader — the docs
    generator, mostly.
    """
    doc = parse_docstring_spec(stage_cls.__doc__)
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
        # what the stage asks to be held before it runs: {meter: number or CEL
        # over `args`}. Read before anything executes, which is why it is
        # declared rather than computed — an editor can show it, and a host
        # can refuse a graph without running it
        "reserve": dict(doc.get("reserve") or {}),
        "timeout": stage_cls.timeout,
    }
