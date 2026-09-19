from __future__ import annotations

from typing import Generic, Iterator, TypeVar

from ..exceptions import RegistryError

T = TypeVar("T")


class Registry(Generic[T]):
    def __init__(self, kind: str):
        self._kind = kind
        self._items: dict[str, T] = {}

    def add(self, name: str, item: T) -> None:
        if name in self._items:
            raise RegistryError(f"{self._kind} '{name}' already registered")
        self._items[name] = item

    def get(self, name: str) -> T:
        try:
            return self._items[name]
        except KeyError:
            raise RegistryError(f"{self._kind} '{name}' not found in registry") from None

    def __contains__(self, name: str) -> bool:
        return name in self._items

    def __iter__(self) -> Iterator[str]:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)

    def as_dict(self) -> dict[str, T]:
        return dict(self._items)
