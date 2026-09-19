from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from ..exceptions import StageContractError
from .event import Event, EventSpec, InputSpec
from .payload_schema import validate_schema
from .registry import Registry
from .spec import build_stage_spec

if TYPE_CHECKING:  # pragma: no cover
    from .session import Session

_stages: Registry[type["BaseStage"]] = Registry("stage")


def register_stage(name: str):
    def decorator(cls: type["BaseStage"]) -> type["BaseStage"]:
        cls.stage_name = name
        _stages.add(name, cls)
        return cls

    return decorator


def get_stage(name: str) -> type["BaseStage"]:
    return _stages.get(name)


def get_stages() -> dict[str, type["BaseStage"]]:
    return _stages.as_dict()


def get_stages_by_category() -> dict[str, list[type["BaseStage"]]]:
    categories: dict[str, list[type["BaseStage"]]] = {}
    for stage_cls in _stages.as_dict().values():
        categories.setdefault(stage_cls.category or "default", []).append(stage_cls)
    return categories


class BaseStage:
    stage_name: str = "BaseStage"
    category: str | None = None
    skipable: bool = False
    allowed_events: list[EventSpec] = []
    allowed_inputs: list[InputSpec] = []
    timeout: float | None = 30

    def __init__(self, stage_id: str, arguments: dict, session: "Session"):
        self.stage_id = stage_id
        self.arguments = arguments or {}
        self.session = session
        self.collected_outputs: dict[str, Any] = {}

    def get_arguments(self) -> dict[str, Any]:
        return dict(self.arguments)

    def set_outputs(self, outputs: dict[str, Any]) -> None:
        self.collected_outputs.update(outputs)

    async def run(self) -> None:
        raise NotImplementedError

    def emit(self, event_type: str, payload: dict | None = None) -> None:
        spec = self._check_allowed(event_type, self.allowed_events, "Event")
        if spec is not None and spec.payload_schema is not None:
            validate_schema(payload or {}, spec.payload_schema, "Event payload")
        self.session.emit(
            Event(
                type=event_type,
                session_id=self.session.id,
                stage_id=self.stage_id,
                payload=payload or {},
            )
        )

    def start_wait_input(self, type_: str) -> asyncio.Future:
        self._check_allowed(type_, self.allowed_inputs, "Input")
        return self.session.start_wait_input(type_)

    async def finish_wait_input(
        self, type_: str, fut: asyncio.Future, timeout: float | None = None
    ) -> dict | None:
        result = await self.session.finish_wait_input(type_, fut, timeout=timeout)
        return self._validated_input(type_, result)

    async def wait_input(self, type_: str, timeout: float | None = None) -> dict | None:
        self._check_allowed(type_, self.allowed_inputs, "Input")
        result = await self.session.wait_input(type_, timeout=timeout)
        return self._validated_input(type_, result)

    def _validated_input(self, type_: str, result: dict | None) -> dict | None:
        if result is None:
            return None
        spec = self._check_allowed(type_, self.allowed_inputs, "Input")
        if spec is not None and spec.payload_schema is not None:
            validate_schema(result.get("payload", {}), spec.payload_schema, "Input payload")
        return result

    def _check_allowed(
        self, type_: str, specs: list[EventSpec] | list[InputSpec], kind: str
    ) -> EventSpec | InputSpec | None:
        if not specs:
            return None
        declared = {spec.type for spec in specs if spec.type}
        if declared and type_ not in declared:
            raise StageContractError(
                f"{kind} type '{type_}' is not allowed for stage '{self.stage_name}'"
            )
        return next((spec for spec in specs if spec.type == type_), None)

    @classmethod
    def get_specs(cls) -> dict[str, Any]:
        return build_stage_spec(cls)
