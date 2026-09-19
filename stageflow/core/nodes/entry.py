"""Узел ``entry`` — точка входа графа: посев переменных пайплайна.

Зачем отдельный тип узла, а не поле пайплайна. Стартовый набор переменных
принадлежит ПАЙПЛАЙНУ, а не стадии: до 0.7.0 его приходилось либо собирать
на стороне вызывающего кода (``Context(vars={...})``), либо изображать
стадией-пустышкой (``SetValueStage`` + ``outputs``), то есть заявлять, что
значение вернула стадия. Первое делает пайплайн незапускаемым как есть —
JSON описывает не всё, что нужно для прогона; второе врёт про происхождение
данных и стоит хода графа на каждую переменную.

Семантика посева — ЗНАЧЕНИЯ ПО УМОЛЧАНИЮ: имя, уже присутствующее во входном
фрейме, узел не трогает. Поэтому один и тот же пайплайн запускается и сам по
себе (берутся значения из JSON), и параметризованно — фреймом сессии или
``inputs`` родителя для субпайплайна, — без правки описания.
"""
from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from ...exceptions import PipelineDefinitionError, TypeCheckError
from ..context import Context
from .base import Node, register_node
from .bindings import CEL_SUFFIX

#: Ссылка на переменную фрейма внутри CEL-выражения. Форм две, потому что
#: точечную CEL разрешает только для ASCII-идентификаторов: имя вроде ``итог``
#: адресуется индексом (``vars['итог']``), и такую ссылку тоже надо видеть —
#: иначе порядок связывания посчитается по неполному графу зависимостей.
_VAR_REF = re.compile(r"""\bvars(?:\.([^\W\d]\w*)|\[\s*['"]([^'"]+)['"]\s*\])""")


def _refs_in(expr: str) -> list[str]:
    """Имена переменных, на которые ссылается выражение (обе формы доступа)."""
    return [dot or indexed for dot, indexed in _VAR_REF.findall(expr)]

if TYPE_CHECKING:  # pragma: no cover
    from ..cel import CelEngine
    from ..pipeline import Pipeline
    from ..session import Session
    from ..typesys import TypeSystem


@register_node("entry")
class EntryNode(Node):
    """Начало графа: объявляет переменные, с которыми пайплайн стартует.

    ``variables`` — плоский словарь ``имя -> значение``; суффикс ``.$`` на
    ключе означает, как и везде в пайплайне, что значение — CEL-выражение
    (``{"total.$": "size(vars.items)"}``).

    Переменные связываются В ПОРЯДКЕ ЗАВИСИМОСТЕЙ, а не в порядке ключей:
    выражение может сослаться на соседнюю переменную того же узла, и результат
    от порядка в JSON не зависит (цикл — ошибка валидации). Это отличается от
    ``outputs`` у ``stage``, где записи одновременны (MEMORY_MODEL.md §2.1), и
    отличается обоснованно: там голый ключ — имя поля результата стадии, а
    ``vars.x`` в соседней строке однозначно значит «x до узла»; здесь стадии
    нет, ``output`` нет, и единственное, на что вообще может ссылаться
    выражение, — переменные, которые этот узел и заводит. Одновременность
    сделала бы `.$` на entry бесполезным: на самостоятельном прогоне входной
    фрейм пуст.
    """

    def __init__(
        self,
        id: str,
        variables: dict[str, Any] | None = None,
        next: str | None = None,
        **common,
    ):
        super().__init__(id, **common)
        self.variables = variables or {}
        self.next = next

    @classmethod
    def _parse(cls, data: dict) -> "EntryNode":
        variables = data.get("variables", {})
        if not isinstance(variables, dict):
            raise PipelineDefinitionError(
                f"Node '{data.get('id')}': 'variables' должен быть объектом"
            )
        return cls(variables=variables, next=data.get("next"), **Node._common(data))

    # ------------------------------------------------------------- посев

    def seed(
        self,
        ctx: Context,
        cel: "CelEngine",
        types: "TypeSystem | None" = None,
    ) -> Context:
        """Досевает во фрейм отсутствующие переменные. Уже занятое имя —
        значение пришло снаружи, — не перезаписывается и не вычисляется."""
        for name, is_cel, spec in self.seed_plan():
            if ctx.has_var(name):
                continue
            value = cel.eval(spec, ctx) if is_cel else spec
            if types is not None:
                types.check_write(name, value, self.id)
            ctx = ctx.with_var(name, value)
        return ctx

    def seed_plan(self) -> list[tuple[str, bool, Any]]:
        """Порядок вычисления переменных: ``(имя, выражение ли, значение)``.

        Сначала те, от кого зависят остальные (алгоритм Кана, при равных
        правах — порядок объявления). Имена в цикле отдаются как объявлены:
        сообщить о цикле — дело валидации, ``seed`` не должен зависать.
        """
        items: dict[str, tuple[bool, Any]] = {}
        declared: list[str] = []
        for key, spec in self.variables.items():
            is_cel = key.endswith(CEL_SUFFIX)
            name = key[: -len(CEL_SUFFIX)] if is_cel else key
            items[name] = (is_cel, spec)
            declared.append(name)

        deps = {name: set(self._own_refs(*items[name], own=set(items))) for name in items}
        plan: list[tuple[str, bool, Any]] = []
        done: set[str] = set()
        remaining = list(dict.fromkeys(declared))
        while remaining:
            ready = [name for name in remaining if deps[name] <= done] or remaining
            for name in ready:
                plan.append((name, *items[name]))
                done.add(name)
            remaining = [name for name in remaining if name not in done]
        return plan

    @staticmethod
    def _own_refs(is_cel: bool, spec: Any, own: set[str]) -> list[str]:
        """Переменные ЭТОГО ЖЕ узла, на которые ссылается выражение."""
        if not (is_cel and isinstance(spec, str)):
            return []
        return [ref for ref in dict.fromkeys(_refs_in(spec)) if ref in own]

    # --------------------------------------------------------- валидация

    def validate(self, pipeline: "Pipeline") -> list[str]:
        errors = self._validate_common(pipeline)
        if self.next and not pipeline.has_node(self.next):
            errors.append(f"{self.id}: next '{self.next}' не найден в графе")
        errors.extend(self._validate_names())
        errors.extend(self._validate_cycles())
        errors.extend(self._validate_seed_types(pipeline))
        return errors

    def _validate_names(self) -> list[str]:
        errors: list[str] = []
        seen: set[str] = set()
        for key, spec in self.variables.items():
            is_cel = key.endswith(CEL_SUFFIX)
            name = key[: -len(CEL_SUFFIX)] if is_cel else key
            if not name.isidentifier():
                errors.append(f"{self.id}: '{name}' — недопустимое имя переменной")
            elif name in seen:
                errors.append(f"{self.id}: переменная '{name}' объявлена дважды")
            seen.add(name)
            if is_cel and not (isinstance(spec, str) and spec.strip()):
                errors.append(f"{self.id}: пустое выражение у переменной '{name}'")
        return errors

    def _validate_cycles(self) -> list[str]:
        """Циклическая зависимость между переменными узла.

        Связывание идёт по зависимостям, поэтому цикл (``a`` через ``b``, ``b``
        через ``a``) вычислить нельзя ни в каком порядке — и молчать об этом
        нельзя: на исполнении он выродился бы в ``ExpressionError`` или, хуже,
        в значение из входного фрейма.
        """
        items: dict[str, tuple[bool, Any]] = {}
        for key, spec in self.variables.items():
            is_cel = key.endswith(CEL_SUFFIX)
            items[key[: -len(CEL_SUFFIX)] if is_cel else key] = (is_cel, spec)

        own = set(items)
        deps = {name: set(self._own_refs(*items[name], own=own)) for name in items}
        done: set[str] = set()
        remaining = set(items)
        while True:
            ready = {name for name in remaining if deps[name] <= done}
            if not ready:
                break
            done |= ready
            remaining -= ready
        if not remaining:
            return []
        return [
            f"{self.id}: циклическая зависимость переменных: "
            f"{', '.join(sorted(remaining))}"
        ]

    def _validate_seed_types(self, pipeline: "Pipeline") -> list[str]:
        """Литеральные значения посева сверяются с объявленными типами
        статически: значение известно уже в описании пайплайна, поэтому
        несоответствие ловится до запуска, а не первым же ``check_write``.
        Ключи с ``.$`` не проверяются — тип выражения статически не выводится.
        """
        types = pipeline.typesystem
        errors: list[str] = []
        for key, value in self.variables.items():
            if key.endswith(CEL_SUFFIX):
                continue
            try:
                types.check_write(key, value, self.id)
            except TypeCheckError as exc:
                errors.append(str(exc))
        return errors

    # -------------------------------------------------------- исполнение

    async def execute(self, session: "Session", ctx: Context) -> tuple[Node | None, Context]:
        ctx = self.seed(ctx, session.cel, session.pipeline.typesystem)
        ctx = self.apply_expose(session, ctx)
        session.emit_node_event("entry_seeded", self, {"variables": sorted(self.variables)})
        return self._goto(session, self.next), ctx
