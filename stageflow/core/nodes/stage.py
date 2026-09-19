"""Узел ``stage`` — исполнение зарегистрированной стадии."""
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
    """Порядок обработки фрейма: ``arguments`` (чтение) -> стадия ->
    ``outputs`` (запись) -> ``consume`` (удаление) -> ``expose``
    (копия/переименование) -> фрейм уходит дальше по ``next``."""

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
            raise PipelineDefinitionError(f"Node '{data.get('id')}': поле 'stage' обязательно")
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
            errors.append(f"{self.id}: next '{self.next}' не найден в графе")
        for bucket in ("vars", "const"):
            value = self.arguments.get(bucket)
            if value is not None and not isinstance(value, (dict, list)):
                errors.append(f"{self.id}: arguments.{bucket} должен быть объектом или списком")
        if self.outputs and not isinstance(self.outputs, dict):
            errors.append(f"{self.id}: outputs должен быть объектом")
        errors.extend(self._validate_legacy_scope())
        errors.extend(self._validate_output_fields())
        errors.extend(self._validate_variable_types(pipeline))
        return errors

    def _validate_legacy_scope(self) -> list[str]:
        """Уровень скоупа в ``outputs`` (до 0.7.0 — ``{"local": {...}}``).

        Без этой проверки такой пайплайн тоже отвергается, но с бесполезным
        текстом «стадия не возвращает поле 'local'»: ключ верхнего уровня
        теперь и есть имя поля результата.
        """
        return [
            f"{self.id}: уровень скоупа '{key}' в outputs убран в 0.7.0 — "
            'ключи плоские: {"поле_результата": "переменная"}'
            for key in ("local", "global")
            if isinstance((self.outputs or {}).get(key), dict)
        ]

    def _validate_output_fields(self) -> list[str]:
        """Сверка ключей ``outputs`` с полями результата, объявленными стадией.

        Ключ бакета ``outputs`` — это ИМЯ ПОЛЯ в результате стадии, и поле,
        которого стадия не возвращает, гарантированно падает в рантайме
        (:class:`StageOutputError` в ``apply_outputs``). Спека стадии знает
        состав результата, поэтому ошибка ловится до запуска.

        Пустая секция ``outputs`` в спеке = контракт не объявлен, проверять
        нечего — та же конвенция, что у ``BaseStage._check_allowed`` для
        событий и ввода; ``*`` означает «поля произвольные».
        """
        try:
            spec = get_stage(self.stage).get_specs()
        except Exception:  # noqa: BLE001 - незарегистрированная стадия/битая спека
            return []      # уже отражено другими проверками

        declared = {field["name"] for field in spec.get("outputs", [])}
        if not declared or "*" in declared:
            return []

        return [
            f"{self.id}: стадия {self.stage} не возвращает поле '{key}' "
            f"(есть: {sorted(declared)})"
            for key in (self.outputs or {})
            if not key.endswith(CEL_SUFFIX) and key not in declared
        ]

    def _validate_variable_types(self, pipeline: "Pipeline") -> list[str]:
        """Статическая сверка объявленных типов переменных со спекой стадии.

        Объявления в ``variables`` — «провода», спека стадии (docstring) —
        «разъёмы»: аргумент, читающий переменную, и выход, пишущий в неё,
        обязаны быть совместимы по роду значения. Ключи с ``.$`` (CEL)
        не проверяются — тип выражения статически не выводится.
        """
        ts = pipeline.typesystem
        if not ts.has_declarations():
            return []
        try:
            spec = get_stage(self.stage).get_specs()
        except Exception:  # noqa: BLE001 - незарегистрированная стадия/битая спека
            return []      # уже отражены другими проверками

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
                        f"{self.id}: аргумент '{arg_name}' стадии {self.stage} ожидает "
                        f"'{hint}', но vars.{var_ref} объявлена как '{declared}'"
                    )

        for out_field, dest in (self.outputs or {}).items():
            if not isinstance(dest, str) or out_field.endswith(CEL_SUFFIX):
                continue
            declared = ts.declared(dest)
            hint = out_hints.get(out_field)
            if declared is not None and not ts.hint_compatible(declared, hint):
                errors.append(
                    f"{self.id}: выход '{out_field}' стадии {self.stage} имеет тип "
                    f"'{hint}', но пишется в vars.{dest} с типом '{declared}'"
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
