from __future__ import annotations

from typing import Generic, Iterator, TypeVar

from ..exceptions import RegistryError
from ..i18n import _

T = TypeVar("T")

# The same sentence per kind of thing, written out rather than assembled from a
# noun and a template.
#
# Dropping the English noun into a translated sentence is the classic way to get
# an ungrammatical one: in Russian `стадия` is feminine and `тип узла` is
# masculine, so "already registered" has two different endings and one template
# cannot carry both. A kind nobody spelled out still gets a message — the
# generic form, with the noun as it was written.


def _taken(kind: str, name: str) -> str:
    if kind == "stage":
        return _("stage '{name}' already registered", name=name)
    if kind == "node type":
        return _("node type '{name}' already registered", name=name)
    return _("{kind} '{name}' already registered", kind=kind, name=name)


def _missing(kind: str, name: str) -> str:
    if kind == "stage":
        return _("stage '{name}' not found in registry", name=name)
    if kind == "node type":
        return _("node type '{name}' not found in registry", name=name)
    return _("{kind} '{name}' not found in registry", kind=kind, name=name)


class Registry(Generic[T]):
    def __init__(self, kind: str):
        self._kind = kind
        self._items: dict[str, T] = {}

    def add(self, name: str, item: T) -> None:
        if name in self._items:
            raise RegistryError(_taken(self._kind, name))
        self._items[name] = item

    def get(self, name: str) -> T:
        try:
            return self._items[name]
        except KeyError:
            raise RegistryError(_missing(self._kind, name)) from None

    def __contains__(self, name: str) -> bool:
        return name in self._items

    def __iter__(self) -> Iterator[str]:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)

    def as_dict(self) -> dict[str, T]:
        return dict(self._items)
