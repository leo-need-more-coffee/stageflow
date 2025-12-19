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
