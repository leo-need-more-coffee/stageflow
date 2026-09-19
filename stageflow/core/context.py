"""Контекст исполнения: иммутабельный фрейм данных.

См. MEMORY_MODEL.md §1.

``vars`` — фрейм, который течёт вдоль пройденного пути исполнения.
Неизменяемый (``immutables.Map``): любая запись возвращает НОВЫЙ Context,
поэтому ветки ``parallel`` расходятся независимо без блокировок и без
deepcopy на чтение, а один и тот же объект можно без опаски отдать нескольким
конкурентным потребителям.

Скоуп один. Session-wide ``global`` удалён в 0.6.0 (REFACTORING.md §10),
поэтому во всём движке не осталось разделяемого мутабельного состояния
данных, — а в 0.7.0 вслед за ним ушло и само слово ``local``: имя скоупа,
у которого нет второго, ничего не различало (REFACTORING.md §12).
"""
from __future__ import annotations

from typing import Any

import immutables

from ..exceptions import PipelineDefinitionError


class Context:
    __slots__ = ("vars",)

    def __init__(self, vars: immutables.Map | dict | None = None):
        if isinstance(vars, dict):
            vars = immutables.Map(vars)
        self.vars: immutables.Map = vars if vars is not None else immutables.Map()

    def get_var(self, name: str, default: Any = None) -> Any:
        return self.vars.get(name, default)

    def has_var(self, name: str) -> bool:
        return name in self.vars

    def with_var(self, name: str, value: Any) -> "Context":
        return Context(vars=self.vars.set(name, value))

    def without_var(self, name: str) -> "Context":
        if name not in self.vars:
            return self
        return Context(vars=self.vars.delete(name))

    def var_names(self) -> frozenset[str]:
        return frozenset(self.vars.keys())

    def to_dict(self) -> dict[str, Any]:
        return {"vars": dict(self.vars)}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Context":
        if "local" in data and "vars" not in data:
            # снапшот до 0.7.0: молча отдать пустой фрейм — худший вариант,
            # сессия восстановилась бы без данных и упала бы позже и не там
            raise PipelineDefinitionError(
                "Снапшот содержит скоуп 'local' (до 0.7.0); фрейм теперь "
                "называется 'vars' — см. REFACTORING.md §12"
            )
        return cls(vars=data.get("vars", {}))

    def __repr__(self) -> str:
        return f"Context(vars={dict(self.vars)!r})"
