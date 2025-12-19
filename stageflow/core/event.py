from dataclasses import dataclass
from datetime import datetime


@dataclass
class InputSpec:
    type: str | None = None
    description: str | None = None
    required: bool = True
    default: str | int | float | bool | None = None
    payload_schema: object | None = None


@dataclass
class EventSpec:
    type: str
    description: str | None = None
    payload_schema: object | None = None


class Event:
    type: str
    session_id: str
    node_id: str | None
    stage_id: str | None
    action_id: str | None
    payload: dict
    timestamp: datetime

    def __init__(
        self,
        type: str,
        session_id: str,
        node_id: str | None = None,
        stage_id: str | None = None,
        action_id: str | None = None,
        payload: dict = None,
    ):
        self.type = type
        self.session_id = session_id
        self.node_id = node_id
        self.stage_id = stage_id
        self.action_id = action_id
        self.payload = payload or {}
        self.timestamp = datetime.utcnow()

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
