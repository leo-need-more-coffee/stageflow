from __future__ import annotations

from typing import Any

from ..exceptions import StageContractError


def require_list(stage_name: str, field: str, value: Any) -> list:
    if not isinstance(value, list):
        raise StageContractError(
            f"{stage_name}: '{field}' must be a list, got {type(value).__name__}"
        )
    return value


def require_dict(stage_name: str, field: str, value: Any) -> dict:
    if not isinstance(value, dict):
        raise StageContractError(
            f"{stage_name}: '{field}' must be an object, got {type(value).__name__}"
        )
    return value


def require_number(stage_name: str, field: str, value: Any) -> int | float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise StageContractError(
            f"{stage_name}: '{field}' must be a number, got {type(value).__name__}"
        )
    return value


def require_present(stage_name: str, field: str, value: Any) -> Any:
    if value is None:
        raise StageContractError(f"{stage_name}: argument '{field}' is required")
    return value
