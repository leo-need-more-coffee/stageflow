from __future__ import annotations

from typing import TYPE_CHECKING

from ...exceptions import PipelineDefinitionError, RegistryError
from ..context import Context
from ..stage import BaseStage, get_stage
from .base import Node, register_node
from .bindings import CEL_SUFFIX, _normalize_bucket, apply_outputs, resolve_arguments
from .recovery import run_with_retry

if TYPE_CHECKING:  # pragma: no cover
    from ..pipeline import Pipeline
    from ..session import Session


@register_node("stage")
class StageNode(Node):
    def __init__(
        self,
        id: str,
        stage: str,
        arguments: dict | None = None,
        outputs: dict | None = None,
        consume: list[str] | None = None,
        next: str | None = None,
        **common,
    ):
        super().__init__(id, **common)
        self.stage = stage
        self.arguments = arguments or {}
        self.outputs = outputs or {}
        self.consume = consume or []
        self.next = next

    @classmethod
    def _parse(cls, data: dict) -> "StageNode":
        stage = data.get("stage")
        if not stage:
            raise PipelineDefinitionError(f"Node '{data.get('id')}': field 'stage' is required")
        return cls(
            stage=stage,
            arguments=data.get("arguments", {}),
            outputs=data.get("outputs", {}),
            consume=data.get("consume", []),
            next=data.get("next"),
            **Node._common(data),
        )

    def get_stage_class(self) -> type[BaseStage]:
        return get_stage(self.stage)

    def validate(self, pipeline: "Pipeline") -> list[str]:
        errors = self._validate_common(pipeline)
        try:
            get_stage(self.stage)
        except RegistryError as exc:
            errors.append(f"{self.id}: {exc}")
        if self.next and not pipeline.has_node(self.next):
            errors.append(f"{self.id}: next '{self.next}' not found in the graph")
        for bucket in ("vars", "const"):
            value = self.arguments.get(bucket)
            if value is not None and not isinstance(value, (dict, list)):
                errors.append(f"{self.id}: arguments.{bucket} must be an object or a list")
        if self.outputs and not isinstance(self.outputs, dict):
            errors.append(f"{self.id}: outputs must be an object")
        errors.extend(self._validate_legacy_scope())
        errors.extend(self._validate_output_fields())
        errors.extend(self._validate_variable_types(pipeline))
        return errors

    def _validate_legacy_scope(self) -> list[str]:
        return [
            f"{self.id}: the '{key}' scope level in outputs was removed in 0.7.0 — "
            'keys are flat: {"result_field": "variable"}'
            for key in ("local", "global")
            if isinstance((self.outputs or {}).get(key), dict)
        ]

    def _validate_output_fields(self) -> list[str]:
        try:
            spec = get_stage(self.stage).get_specs()
        except Exception:  # noqa: BLE001
            return []

        declared = {field["name"] for field in spec.get("outputs", [])}
        if not declared or "*" in declared:
            return []

        return [
            f"{self.id}: stage {self.stage} does not return field '{key}' "
            f"(available: {sorted(declared)})"
            for key in (self.outputs or {})
            if not key.endswith(CEL_SUFFIX) and key not in declared
        ]

    def _validate_variable_types(self, pipeline: "Pipeline") -> list[str]:
        ts = pipeline.typesystem
        if not ts.has_declarations():
            return []
        try:
            spec = get_stage(self.stage).get_specs()
        except Exception:  # noqa: BLE001
            return []

        arg_hints = {f["name"]: f.get("type") for f in spec.get("arguments", [])}
        out_hints = {f["name"]: f.get("type") for f in spec.get("outputs", [])}
        errors: list[str] = []

        bucket = self.arguments.get("vars")
        if isinstance(bucket, (dict, list, type(None))):
            for arg_name, var_ref in _normalize_bucket(bucket).items():
                if arg_name.endswith(CEL_SUFFIX):
                    continue
                declared = ts.declared(var_ref)
                hint = arg_hints.get(arg_name)
                if declared is not None and not ts.hint_compatible(declared, hint):
                    errors.append(
                        f"{self.id}: argument '{arg_name}' of stage {self.stage} expects "
                        f"'{hint}', but vars.{var_ref} is declared as '{declared}'"
                    )

        for out_field, dest in (self.outputs or {}).items():
            if not isinstance(dest, str) or out_field.endswith(CEL_SUFFIX):
                continue
            declared = ts.declared(dest)
            hint = out_hints.get(out_field)
            if declared is not None and not ts.hint_compatible(declared, hint):
                errors.append(
                    f"{self.id}: output '{out_field}' of stage {self.stage} has type "
                    f"'{hint}', but is written to vars.{dest} typed '{declared}'"
                )
        return errors

    async def execute(self, session: "Session", ctx: Context) -> tuple[Node | None, Context]:
        async def body() -> tuple[Node | None, Context]:
            kwargs = resolve_arguments(self.arguments, ctx, session.cel)
            result = await session.run_stage(self, kwargs)

            new_ctx = apply_outputs(
                self.outputs, ctx, result, session.cel,
                types=session.pipeline.typesystem, owner=self.id,
            )
            for name in self.consume:
                new_ctx = new_ctx.without_var(name)
            new_ctx = self.apply_expose(session, new_ctx)
            return self._goto(session, self.next), new_ctx

        return await run_with_retry(self, session, ctx, body)
