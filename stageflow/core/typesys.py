from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterator

from ..exceptions import TypeCheckError, TypeDeclarationError
from .context import Context

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class VarType:
    def check(self, value: Any, path: str, registry: "TypeRegistry") -> None:
        raise NotImplementedError

    def kinds(self, registry: "TypeRegistry") -> frozenset[str]:
        raise NotImplementedError

    def refs(self) -> Iterator[str]:
        return iter(())


def _fail(path: str, expected: "VarType", value: Any) -> None:
    raise TypeCheckError(f"{path}: ожидался {expected}, получен {type(value).__name__}")


@dataclass(frozen=True)
class AnyType(VarType):
    def check(self, value, path, registry):
        return

    def kinds(self, registry):
        return frozenset({"any"})

    def __str__(self):
        return "any"


@dataclass(frozen=True)
class NullType(VarType):
    def check(self, value, path, registry):
        if value is not None:
            _fail(path, self, value)

    def kinds(self, registry):
        return frozenset({"null"})

    def __str__(self):
        return "null"


@dataclass(frozen=True)
class PrimitiveType(VarType):
    name: str

    def check(self, value, path, registry):
        is_bool = isinstance(value, bool)
        ok = {
            "string": isinstance(value, str),
            "int": isinstance(value, int) and not is_bool,
            "float": isinstance(value, (int, float)) and not is_bool,
            "number": isinstance(value, (int, float)) and not is_bool,
            "bool": is_bool,
        }[self.name]
        if not ok:
            _fail(path, self, value)

    def kinds(self, registry):
        if self.name in ("float", "number"):
            return frozenset({"int", "float"})
        return frozenset({self.name})

    def __str__(self):
        return self.name


@dataclass(frozen=True)
class ListType(VarType):
    item: VarType

    def check(self, value, path, registry):
        if not isinstance(value, list):
            _fail(path, self, value)
        for idx, element in enumerate(value):
            self.item.check(element, f"{path}[{idx}]", registry)

    def kinds(self, registry):
        return frozenset({"list"})

    def refs(self):
        yield from self.item.refs()

    def __str__(self):
        return f"list<{self.item}>"


@dataclass(frozen=True)
class MapType(VarType):
    value: VarType

    def check(self, value, path, registry):
        if not isinstance(value, dict):
            _fail(path, self, value)
        for key, element in value.items():
            if not isinstance(key, str):
                raise TypeCheckError(
                    f"{path}: ключи map должны быть строками, найден {type(key).__name__}"
                )
            self.value.check(element, f"{path}.{key}", registry)

    def kinds(self, registry):
        return frozenset({"map"})

    def refs(self):
        yield from self.value.refs()

    def __str__(self):
        return f"map<{self.value}>"


@dataclass(frozen=True)
class UnionType(VarType):
    options: tuple[VarType, ...]

    def check(self, value, path, registry):
        for option in self.options:
            try:
                option.check(value, path, registry)
                return
            except TypeCheckError:
                continue
        _fail(path, self, value)

    def kinds(self, registry):
        combined: frozenset[str] = frozenset()
        for option in self.options:
            combined |= option.kinds(registry)
        return combined

    def refs(self):
        for option in self.options:
            yield from option.refs()

    def __str__(self):
        return " | ".join(str(option) for option in self.options)


@dataclass(frozen=True)
class StructField:
    type: VarType
    optional: bool = False


@dataclass(frozen=True)
class StructType(VarType):
    name: str
    fields: dict[str, StructField]
    strict: bool = False

    def check(self, value, path, registry):
        if not isinstance(value, dict):
            _fail(path, self, value)
        for field_name, spec in self.fields.items():
            if field_name not in value:
                if spec.optional:
                    continue
                raise TypeCheckError(
                    f"{path}: в структуре {self.name} нет обязательного поля '{field_name}'"
                )
            spec.type.check(value[field_name], f"{path}.{field_name}", registry)
        if self.strict:
            extra = set(value) - set(self.fields)
            if extra:
                raise TypeCheckError(
                    f"{path}: структура {self.name} не допускает лишних полей: {sorted(extra)}"
                )

    def kinds(self, registry):
        return frozenset({"struct"})

    def refs(self):
        for spec in self.fields.values():
            yield from spec.type.refs()

    def __str__(self):
        return self.name


@dataclass(frozen=True)
class NamedRef(VarType):
    name: str

    def check(self, value, path, registry):
        registry.resolve(self.name).check(value, path, registry)

    def kinds(self, registry):
        return registry.resolve(self.name).kinds(registry)

    def refs(self):
        yield self.name

    def __str__(self):
        return self.name


_PRIMITIVES: dict[str, VarType] = {
    "any": AnyType(),
    "null": NullType(),
    "string": PrimitiveType("string"),
    "str": PrimitiveType("string"),
    "int": PrimitiveType("int"),
    "float": PrimitiveType("float"),
    "number": PrimitiveType("number"),
    "bool": PrimitiveType("bool"),
}


def _split_top_level(expr: str, sep: str) -> list[str]:
    parts, depth, start = [], 0, 0
    for idx, char in enumerate(expr):
        if char == "<":
            depth += 1
        elif char == ">":
            depth -= 1
        elif char == sep and depth == 0:
            parts.append(expr[start:idx])
            start = idx + 1
    parts.append(expr[start:])
    return parts


def parse_type(expr: str) -> VarType:
    if not isinstance(expr, str) or not expr.strip():
        raise TypeDeclarationError(f"Пустое или нестроковое типовое выражение: {expr!r}")
    expr = expr.strip()

    union_parts = _split_top_level(expr, "|")
    if len(union_parts) > 1:
        return UnionType(tuple(parse_type(part) for part in union_parts))

    if expr.endswith("?"):
        return UnionType((parse_type(expr[:-1]), NullType()))

    if expr in _PRIMITIVES:
        return _PRIMITIVES[expr]

    for container, cls in (("list", ListType), ("map", MapType)):
        prefix = container + "<"
        if expr.startswith(prefix):
            if not expr.endswith(">"):
                raise TypeDeclarationError(f"Незакрытый '{container}<' в выражении {expr!r}")
            return cls(parse_type(expr[len(prefix):-1]))

    if not _IDENT_RE.match(expr):
        raise TypeDeclarationError(f"Некорректное типовое выражение: {expr!r}")
    return NamedRef(expr)


def _parse_type_value(name: str, spec: Any) -> VarType:
    if isinstance(spec, str):
        return parse_type(spec)
    if isinstance(spec, dict):
        return parse_struct(name, spec)
    raise TypeDeclarationError(
        f"Тип '{name}' должен быть выражением или структурой, получен {type(spec).__name__}"
    )


def parse_struct(name: str, data: dict) -> StructType:
    if "fields" in data:
        fields_raw = data["fields"]
        strict = bool(data.get("strict", False))
        if not isinstance(fields_raw, dict):
            raise TypeDeclarationError(f"Тип '{name}': 'fields' должен быть объектом")
    else:
        fields_raw, strict = data, False

    fields: dict[str, StructField] = {}
    for raw_name, spec in fields_raw.items():
        optional = raw_name.endswith("?")
        field_name = raw_name[:-1] if optional else raw_name
        if not _IDENT_RE.match(field_name):
            raise TypeDeclarationError(f"Тип '{name}': некорректное имя поля {raw_name!r}")
        fields[field_name] = StructField(
            type=_parse_type_value(f"{name}.{field_name}", spec), optional=optional
        )
    return StructType(name=name, fields=fields, strict=strict)


class TypeRegistry:
    def __init__(self, types: dict[str, VarType] | None = None):
        self._types = types or {}

    @classmethod
    def from_dict(cls, data: dict | None) -> "TypeRegistry":
        types: dict[str, VarType] = {}
        for name, spec in (data or {}).items():
            if not _IDENT_RE.match(name):
                raise TypeDeclarationError(f"Некорректное имя типа: {name!r}")
            types[name] = _parse_type_value(name, spec)
        return cls(types)

    def resolve(self, name: str) -> VarType:
        try:
            return self._types[name]
        except KeyError:
            raise TypeDeclarationError(f"Неизвестный тип '{name}'") from None

    def __contains__(self, name: str) -> bool:
        return name in self._types

    def collect_errors(self) -> list[str]:
        errors = []
        for name, declared in self._types.items():
            for ref in declared.refs():
                if ref not in self._types:
                    errors.append(f"тип '{name}' ссылается на неизвестный тип '{ref}'")
        return errors


_HINT_KINDS: dict[str, frozenset[str]] = {
    "string": frozenset({"string"}),
    "str": frozenset({"string"}),
    "int": frozenset({"int"}),
    "float": frozenset({"int", "float"}),
    "number": frozenset({"int", "float"}),
    "bool": frozenset({"bool"}),
    "object": frozenset({"map", "struct"}),
    "dict": frozenset({"map", "struct"}),
    "list": frozenset({"list"}),
}


class TypeSystem:
    def __init__(self, registry: TypeRegistry, variables: dict[str, VarType]):
        self._registry = registry
        self._variables = variables

    @classmethod
    def empty(cls) -> "TypeSystem":
        return cls(TypeRegistry(), {})

    @classmethod
    def from_dict(cls, types_data: dict | None, variables_data: dict | None) -> "TypeSystem":
        registry = TypeRegistry.from_dict(types_data)
        declarations = variables_data or {}
        for legacy in ("local", "global"):
            if legacy in declarations and isinstance(declarations[legacy], dict):
                raise TypeDeclarationError(
                    f"variables: уровень скоупа '{legacy}' убран в 0.7.0 — "
                    'объявления плоские: {"n": "int"}'
                )
        variables = {
            name: _parse_type_value(f"vars.{name}", spec)
            for name, spec in declarations.items()
        }
        return cls(registry, variables)

    def has_declarations(self) -> bool:
        return bool(self._variables)

    def declared(self, name: str) -> VarType | None:
        return self._variables.get(name)

    def collect_errors(self) -> list[str]:
        errors = self._registry.collect_errors()
        for name, declared in self._variables.items():
            for ref in declared.refs():
                if ref not in self._registry:
                    errors.append(
                        f"переменная vars.{name} ссылается на неизвестный тип '{ref}'"
                    )
        return errors

    def hint_compatible(self, declared: VarType, hint: Any) -> bool:
        if not isinstance(hint, str):
            return True
        allowed = _HINT_KINDS.get(hint.lower())
        if allowed is None:
            return True
        declared_kinds = declared.kinds(self._registry)
        return bool(declared_kinds & allowed) or "any" in declared_kinds or "null" in declared_kinds

    def expose_compatible(self, src: VarType, dst: VarType) -> bool:
        if isinstance(src, AnyType) or isinstance(dst, AnyType):
            return True
        return src == dst

    def check_write(self, name: str, value: Any, where: str) -> None:
        declared = self.declared(name)
        if declared is None:
            return
        try:
            declared.check(value, f"vars.{name}", self._registry)
        except TypeCheckError as exc:
            raise TypeCheckError(f"{where}: {exc}") from None

    def check_context(self, ctx: Context, where: str) -> None:
        for name in self._variables:
            if ctx.has_var(name):
                self.check_write(name, ctx.get_var(name), where)
