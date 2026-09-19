"""Валидация payload по схемам из type-хинтов и их сериализация в JSON.

Схема — это обычные питоновские хинты и литералы: ``int``, ``list[str]``,
``Optional[int]``, ``{"user": {"id": int}}``, ``[{"id": int}]``. Используется
для ``payload_schema`` в ``EventSpec``/``InputSpec``.
"""
from __future__ import annotations

from typing import Any, Union, get_args, get_origin

from ..exceptions import PayloadValidationError


def validate_schema(value: Any, schema: object, path: str = "payload") -> None:
    """Проверяет ``value`` против ``schema``; кидает
    :class:`PayloadValidationError` с точным путём до несовпадения."""
    if schema is None or schema is Any or schema is object:
        return

    origin = get_origin(schema)
    args = get_args(schema)

    if origin is Union and args:
        for variant in args:
            try:
                validate_schema(value, variant, path)
                return
            except PayloadValidationError:
                continue
        raise PayloadValidationError(
            f"{path} expected one of {args}, got {type(value).__name__}"
        )

    if origin is list and args:
        _require(value, list, path)
        for idx, item in enumerate(value):
            validate_schema(item, args[0], f"{path}[{idx}]")
        return

    if origin is dict and args:
        _require(value, dict, path)
        key_schema, value_schema = args
        for key, item in value.items():
            validate_schema(key, key_schema, f"{path} (key)")
            validate_schema(item, value_schema, f"{path}[{key}]")
        return

    if isinstance(schema, dict):
        _require(value, dict, path)
        for key, sub_schema in schema.items():
            if key not in value:
                raise PayloadValidationError(f"{path} missing required field '{key}'")
            validate_schema(value[key], sub_schema, f"{path}.{key}")
        return

    if isinstance(schema, list) and len(schema) == 1:
        _require(value, list, path)
        for idx, item in enumerate(value):
            validate_schema(item, schema[0], f"{path}[{idx}]")
        return

    if isinstance(schema, tuple):
        expected: tuple[type, ...] = schema
    elif isinstance(schema, type):
        expected = (schema,)
    else:
        return
    if not isinstance(value, expected):
        raise PayloadValidationError(
            f"{path} expected {schema}, got {type(value).__name__}"
        )


def _require(value: Any, type_: type, path: str) -> None:
    if not isinstance(value, type_):
        raise PayloadValidationError(
            f"{path} expected {type_.__name__}, got {type(value).__name__}"
        )


_TYPE_NAMES = {
    str: "str",
    int: "int",
    float: "float",
    bool: "bool",
    dict: "object",
    list: "list",
    type(None): "null",
}


def schema_to_jsonable(schema: object) -> object:
    """Переводит схему из type-хинтов в JSON-сериализуемую структуру
    (для ``stages.json`` и HTML-документации)."""
    if schema is None:
        return None
    if schema is Any or schema is object:
        return "any"

    origin = get_origin(schema)
    args = get_args(schema)

    if origin is Union and args:
        return {"anyOf": [schema_to_jsonable(arg) for arg in args]}
    if origin is list and args:
        return [schema_to_jsonable(args[0])]
    if origin is dict and args:
        return {"key": schema_to_jsonable(args[0]), "value": schema_to_jsonable(args[1])}

    if isinstance(schema, dict):
        return {key: schema_to_jsonable(value) for key, value in schema.items()}
    if isinstance(schema, (list, tuple)):
        return [schema_to_jsonable(value) for value in schema]
    if isinstance(schema, type):
        return _TYPE_NAMES.get(schema, getattr(schema, "__name__", str(schema)))
    return str(schema)
