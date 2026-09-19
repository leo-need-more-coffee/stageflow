"""События телеметрии и декларации контрактов стадии.

``Event`` — единица истории исполнения: копится в ``Session.event_history``
и уходит во внешний ``event_handler``. ``EventSpec``/``InputSpec`` — то, что
стадия декларирует о себе: какие события она вправе эмитить и какой ввод
принимать (со схемами payload, см. ``payload_schema``).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from .payload_schema import schema_to_jsonable


@dataclass(frozen=True, slots=True)
class InputSpec:
    """Декларация типа пользовательского ввода, допустимого для стадии."""

    type: str | None = None
    description: str | None = None
    required: bool = True
    default: str | int | float | bool | None = None
    payload_schema: object | None = None

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "description": self.description,
            "required": self.required,
            "default": self.default,
            "payload_schema": schema_to_jsonable(self.payload_schema),
        }


@dataclass(frozen=True, slots=True)
class EventSpec:
    """Декларация типа события, которое стадия вправе эмитить."""

    type: str
    description: str | None = None
    payload_schema: object | None = None

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "description": self.description,
            "payload_schema": schema_to_jsonable(self.payload_schema),
        }


@dataclass(slots=True)
class Event:
    """Запись телеметрии исполнения; сериализуется в снапшоты и наружу."""

    type: str
    session_id: str
    node_id: str | None = None
    stage_id: str | None = None
    action_id: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "session_id": self.session_id,
            "node_id": self.node_id,
            "stage_id": self.stage_id,
            "action_id": self.action_id,
            "payload": self.payload,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Event":
        raw_ts = data.get("timestamp")
        return cls(
            type=data.get("type"),
            session_id=data.get("session_id"),
            node_id=data.get("node_id"),
            stage_id=data.get("stage_id"),
            action_id=data.get("action_id"),
            payload=data.get("payload") or {},
            timestamp=datetime.fromisoformat(raw_ts) if raw_ts else datetime.now(UTC),
        )
