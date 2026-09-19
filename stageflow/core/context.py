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
            raise PipelineDefinitionError(
                "Snapshot contains the 'local' scope (pre-0.7.0); "
                "the frame is now called 'vars'"
            )
        return cls(vars=data.get("vars", {}))

    def __repr__(self) -> str:
        return f"Context(vars={dict(self.vars)!r})"
