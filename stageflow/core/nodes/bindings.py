from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ...exceptions import PipelineDefinitionError, StageOutputError
from ..context import Context

if TYPE_CHECKING:  # pragma: no cover
    from ..cel import CelEngine
    from ..typesys import TypeSystem

CEL_SUFFIX = ".$"


def _normalize_bucket(bucket: Any) -> dict[str, str]:
    if bucket is None:
        return {}
    if isinstance(bucket, list):
        return {name: name for name in bucket}
    if isinstance(bucket, dict):
        return dict(bucket)
    raise PipelineDefinitionError(
        f"Bucket must be a list or an object, got {type(bucket).__name__}"
    )


def resolve_arguments(
    arguments: dict, ctx: Context, cel: "CelEngine", **extra: Any
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}

    for key, value in (arguments.get("const") or {}).items():
        if key.endswith(CEL_SUFFIX):
            kwargs[key[: -len(CEL_SUFFIX)]] = cel.eval(value, ctx, **extra)
        else:
            kwargs[key] = value

    for arg_name, ref in _normalize_bucket(arguments.get("vars")).items():
        if arg_name.endswith(CEL_SUFFIX):
            kwargs[arg_name[: -len(CEL_SUFFIX)]] = cel.eval(ref, ctx, **extra)
        else:
            kwargs[arg_name] = ctx.get_var(ref)

    return kwargs


def _as_output_namespace(result: Any) -> dict[str, Any]:
    if result is None:
        return {}
    if isinstance(result, dict):
        return result
    if hasattr(result, "__dict__"):
        return vars(result)
    return {"value": result}


def apply_outputs(
    outputs: dict,
    ctx: Context,
    result: Any,
    cel: "CelEngine",
    types: "TypeSystem | None" = None,
    owner: str = "?",
    **extra: Any,
) -> Context:
    output_ns = _as_output_namespace(result)
    frame = ctx

    writes: list[tuple[str, Any]] = []
    for key, spec in (outputs or {}).items():
        if key.endswith(CEL_SUFFIX):
            dest = key[: -len(CEL_SUFFIX)]
            value = cel.eval(spec, frame, output=output_ns, **extra)
        else:
            dest = spec
            if key not in output_ns:
                raise StageOutputError(
                    f"Стадия не вернула поле '{key}' (есть: {sorted(output_ns)})"
                )
            value = output_ns[key]
        writes.append((dest, value))

    for dest, value in writes:
        if types is not None:
            types.check_write(dest, value, owner)
        ctx = ctx.with_var(dest, value)

    return ctx
