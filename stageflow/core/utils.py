from typing import Any, get_origin, get_args, Union


def validate_schema(value: Any, schema: object, path: str = "payload"):
    if schema is None:
        return

    origin = get_origin(schema)
    args = get_args(schema)

    if schema is Any or schema is object:
        return

    if origin is Union and args:
        for variant in args:
            try:
                validate_schema(value, variant, path)
                return
            except ValueError:
                continue
        raise ValueError(f"{path} expected one of {args}, got {type(value).__name__}")

    if origin is list and args:
        if not isinstance(value, list):
            raise ValueError(f"{path} expected list, got {type(value).__name__}")
        elem_schema = args[0]
        for idx, item in enumerate(value):
            validate_schema(item, elem_schema, f"{path}[{idx}]")
        return
    if origin is dict and args:
        key_schema, val_schema = args
        if not isinstance(value, dict):
            raise ValueError(f"{path} expected dict, got {type(value).__name__}")
        for k, v in value.items():
            validate_schema(k, key_schema, f"{path} (key)")
            validate_schema(v, val_schema, f"{path}[{k}]")
        return

    if isinstance(schema, dict):
        if not isinstance(value, dict):
            raise ValueError(f"{path} expected dict, got {type(value).__name__}")
        for k, sub_schema in schema.items():
            if k not in value:
                raise ValueError(f"{path} missing required field '{k}'")
            validate_schema(value[k], sub_schema, f"{path}.{k}")
        return

    if isinstance(schema, list) and len(schema) == 1:
        if not isinstance(value, list):
            raise ValueError(f"{path} expected list, got {type(value).__name__}")
        for idx, item in enumerate(value):
            validate_schema(item, schema[0], f"{path}[{idx}]")
        return

    if isinstance(schema, tuple):
        expected = schema
    elif isinstance(schema, type):
        expected = (schema,)
    else:
        return
    if not isinstance(value, expected):
        raise ValueError(f"{path} expected {schema}, got {type(value).__name__}")


_TYPE_NAME_OVERRIDES = {
    str: "str",
    int: "int",
    float: "float",
    bool: "bool",
    dict: "object",
    list: "list",
    type(None): "null",
}


def schema_to_jsonable(schema: object) -> object:
    """Convert payload schema hint into a JSON-serializable structure."""
    origin = get_origin(schema)
    args = get_args(schema)

    if schema is None:
        return None

    if schema is Any or schema is object:
        return "any"

    if origin is Union and args:
        return {"anyOf": [schema_to_jsonable(arg) for arg in args]}

    if origin is list and args:
        return [schema_to_jsonable(args[0])]

    if origin is dict and args:
        return {"key": schema_to_jsonable(args[0]), "value": schema_to_jsonable(args[1])}

    if isinstance(schema, dict):
        return {k: schema_to_jsonable(v) for k, v in schema.items()}

    if isinstance(schema, list):
        return [schema_to_jsonable(v) for v in schema]

    if isinstance(schema, tuple):
        return [schema_to_jsonable(v) for v in schema]

    if isinstance(schema, type):
        return _TYPE_NAME_OVERRIDES.get(schema, getattr(schema, "__name__", str(schema)))

    return str(schema)
