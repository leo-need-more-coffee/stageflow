from __future__ import annotations

from typing import Any

from ..exceptions import StageContractError
from ..i18n import _


def require_list(stage_name: str, field: str, value: Any) -> list:
    if not isinstance(value, list):
        raise StageContractError(
            _("{stage}: '{field}' must be a list, got {got}",
              stage=stage_name, field=field, got=type(value).__name__)
        )
    return value


def require_dict(stage_name: str, field: str, value: Any) -> dict:
    if not isinstance(value, dict):
        raise StageContractError(
            _("{stage}: '{field}' must be an object, got {got}",
              stage=stage_name, field=field, got=type(value).__name__)
        )
    return value


def require_number(stage_name: str, field: str, value: Any) -> int | float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise StageContractError(
            _("{stage}: '{field}' must be a number, got {got}",
              stage=stage_name, field=field, got=type(value).__name__)
        )
    return value


def require_present(stage_name: str, field: str, value: Any) -> Any:
    if value is None:
        raise StageContractError(
            _("{stage}: argument '{field}' is required", stage=stage_name, field=field)
        )
    return value
