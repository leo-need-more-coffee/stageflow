import copy
from typing import Any, TYPE_CHECKING
import yaml
from stageflow.core import EventSpec, InputSpec
from .utils import validate_schema


STAGE_REGISTRY: dict[str, type["BaseStage"]] = {}

if TYPE_CHECKING:
    from .session import Session


def register_stage(name: str):
    def decorator(cls: type["BaseStage"]):
        if name in STAGE_REGISTRY:
            raise ValueError(f"Stage '{name}' already registered")
        cls.stage_name = name
        STAGE_REGISTRY[name] = cls
        return cls
    return decorator


def get_stage(name: str) -> type["BaseStage"]:
    if name not in STAGE_REGISTRY:
        raise ValueError(f"Stage '{name}' not found in registry")
    return STAGE_REGISTRY[name]


def get_stages() -> dict[str, Any]:
    return STAGE_REGISTRY


def get_stages_by_category() -> dict[str, list[type["BaseStage"]]]:
    categories: dict[str, list[type["BaseStage"]]] = {}
    for stage in STAGE_REGISTRY.values():
        category = stage.category or "default"
        categories.setdefault(category, []).append(stage)
    return categories


def _normalize_field_entry(name: str | None, spec: Any) -> dict[str, Any]:
    if isinstance(spec, dict):
        entry = {"name": name, **spec} if name else dict(spec)
    else:
        entry = {"name": name, "type": spec}
    entry.setdefault("type", "any")
    entry.setdefault("optional", False)
    entry.setdefault("description", "")
    return entry


def _normalize_fields(raw: Any) -> list[dict[str, Any]]:
    if not raw:
        return []
    normalized: list[dict[str, Any]] = []
    if isinstance(raw, dict):
        for name, spec in raw.items():
            normalized.append(_normalize_field_entry(name, spec))
        return normalized

    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                if "name" in item:
                    normalized.append(_normalize_field_entry(item.get("name"), {k: v for k, v in item.items() if k != "name"}))
                elif len(item) == 1:
                    name, spec = next(iter(item.items()))
                    normalized.append(_normalize_field_entry(name, spec))
            else:
                normalized.append(_normalize_field_entry(str(item), {"type": "any"}))
        return normalized

    return normalized


class BaseStage:
    skipable: bool = False
    stage_name: str = "BaseStage"
    category: str | None = None
    allowed_events: list[EventSpec] = []
    allowed_inputs: list[InputSpec] = []
    timeout: float | None = 30
    retries: int = 0

    def __init__(self, stage_id: str, config: dict, arguments: dict, outputs: dict, session: "Session"):
        self.stage_id = stage_id
        self.config = config or {}
        self.arguments_paths = arguments or {}
        self.outputs_paths = outputs or {}
        self.session = session

    def get_arguments(self) -> dict:
        arguments = dict()
        for key, path in self.arguments_paths.items():
            arguments[key] = copy.deepcopy(self.session.context.get(path))
        return arguments

    def set_outputs(self, outputs: dict):
        for key, value in outputs.items():
            if key in self.outputs_paths:
                path = self.outputs_paths[key]
                self.session.context.set(path, value)

    async def run(self):
        raise NotImplementedError

    def emit(self, event_type: str, payload: dict | None = None):
        from .event import Event
        if self.allowed_events:
            allowed = {spec.type for spec in self.allowed_events if spec.type}
            if allowed and event_type not in allowed:
                raise ValueError(f"Event type '{event_type}' is not allowed for stage '{self.stage_name}'")
            matching = next((spec for spec in self.allowed_events if spec.type == event_type), None)
            if matching and matching.payload_schema is not None:
                validate_schema(payload or {}, matching.payload_schema, "Event payload")
        self.session.emit(Event(
            type=event_type,
            session_id=self.session.id,
            stage_id=self.stage_id,
            payload=payload or {},
        ))

    async def wait_input(self, type_: str, timeout: float | None = None):
        if self.allowed_inputs:
            allowed = {spec.type for spec in self.allowed_inputs if spec.type}
            if allowed and type_ not in allowed:
                raise ValueError(f"Input type '{type_}' is not allowed for stage '{self.stage_name}'")
            matching = next((spec for spec in self.allowed_inputs if spec.type == type_), None)
        else:
            matching = None
        result = await self.session.wait_input(type_, timeout=timeout)
        if result is None:
            return None
        if matching and matching.payload_schema is not None:
            validate_schema(result.get("payload", {}), matching.payload_schema, "Input payload")
        return result

    @classmethod
    def get_specs(cls) -> dict[str, Any]:
        parsed_description = yaml.safe_load(cls.__doc__) if cls.__doc__ else {}
        return {
            "stage_name": cls.stage_name,
            "skipable": cls.skipable,
            "allowed_events": [e.__dict__ for e in cls.allowed_events],
            "allowed_inputs": [i.__dict__ for i in cls.allowed_inputs],
            "category": cls.category,
            "description": parsed_description.get("description", "") if isinstance(parsed_description, dict) else "",
            "arguments": _normalize_fields(parsed_description.get("arguments", [])) if isinstance(parsed_description, dict) else [],
            "config": _normalize_fields(parsed_description.get("config", [])) if isinstance(parsed_description, dict) else [],
            "outputs": _normalize_fields(parsed_description.get("outputs", [])) if isinstance(parsed_description, dict) else [],
        }
