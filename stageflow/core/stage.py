import copy
from typing import Any
import yaml
from stageflow.core import EventSpec, InputSpec


STAGE_REGISTRY: dict[str, type["BaseStage"]] = {}


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


class BaseStage:
    skipable: bool = False
    stage_name: str = "BaseStage"
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
        self.session.emit(Event(
            type=event_type,
            session_id=self.session.id,
            stage_id=self.stage_id,
            payload=payload or {},
        ))

    async def wait_input(self, type_: str, timeout: float | None = None):
        return await self.session.wait_input(type_, timeout=timeout)

    @classmethod
    def get_specs(cls) -> dict[str, Any]:
        parsed_description = yaml.safe_load(cls.__doc__) if cls.__doc__ else {}
        return {
            "stage_name": cls.stage_name,
            "skipable": cls.skipable,
            "allowed_events": [e.__dict__ for e in cls.allowed_events],
            "allowed_inputs": [i.__dict__ for i in cls.allowed_inputs],
            "description": parsed_description.get("description", ""),
            "arguments": parsed_description.get("arguments", {}),
            "config": parsed_description.get("config", {}),
            "outputs": parsed_description.get("outputs", {}),
        }
