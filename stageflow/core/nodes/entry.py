from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from ...exceptions import PipelineDefinitionError, TypeCheckError
from ..context import Context
from .base import Node, register_node
from .bindings import CEL_SUFFIX

_VAR_REF = re.compile(r"""\bvars(?:\.([^\W\d]\w*)|\[\s*['"]([^'"]+)['"]\s*\])""")


def _refs_in(expr: str) -> list[str]:
    return [dot or indexed for dot, indexed in _VAR_REF.findall(expr)]


if TYPE_CHECKING:  # pragma: no cover
    from ..cel import CelEngine
    from ..pipeline import Pipeline
    from ..session import Session
    from ..typesys import TypeSystem


@register_node("entry")
class EntryNode(Node):
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
                f"Node '{data.get('id')}': 'variables' must be an object"
            )
        return cls(variables=variables, next=data.get("next"), **Node._common(data))

    def seed(
        self,
        ctx: Context,
        cel: "CelEngine",
        types: "TypeSystem | None" = None,
    ) -> Context:
        for name, is_cel, spec in self.seed_plan():
            if ctx.has_var(name):
                continue
            value = cel.eval(spec, ctx) if is_cel else spec
            if types is not None:
                types.check_write(name, value, self.id)
            ctx = ctx.with_var(name, value)
        return ctx

    def seed_plan(self) -> list[tuple[str, bool, Any]]:
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
        if not (is_cel and isinstance(spec, str)):
            return []
        return [ref for ref in dict.fromkeys(_refs_in(spec)) if ref in own]

    def validate(self, pipeline: "Pipeline") -> list[str]:
        errors = self._validate_common(pipeline)
        if self.next and not pipeline.has_node(self.next):
            errors.append(f"{self.id}: next '{self.next}' not found in the graph")
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
                errors.append(f"{self.id}: '{name}' is not a valid variable name")
            elif name in seen:
                errors.append(f"{self.id}: variable '{name}' is declared twice")
            seen.add(name)
            if is_cel and not (isinstance(spec, str) and spec.strip()):
                errors.append(f"{self.id}: empty expression for variable '{name}'")
        return errors

    def _validate_cycles(self) -> list[str]:
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
            f"{self.id}: cyclic variable dependency: "
            f"{', '.join(sorted(remaining))}"
        ]

    def _validate_seed_types(self, pipeline: "Pipeline") -> list[str]:
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

    async def execute(self, session: "Session", ctx: Context) -> tuple[Node | None, Context]:
        ctx = self.seed(ctx, session.cel, session.pipeline.typesystem)
        ctx = self.apply_expose(session, ctx)
        session.emit_node_event("entry_seeded", self, {"variables": sorted(self.variables)})
        return self._goto(session, self.next), ctx
