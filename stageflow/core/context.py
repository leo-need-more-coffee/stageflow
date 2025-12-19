from collections import defaultdict
from typing import Any


class DotDict(dict):
    def __getattr__(self, item):
        try:
            value = self[item]
            if isinstance(value, dict) and not isinstance(value, DotDict):
                value = DotDict(value)
                self[item] = value
            return value
        except KeyError:
            raise AttributeError(item)

    def __setattr__(self, key, value):
        self[key] = value

    def __delattr__(self, key):
        try:
            del self[key]
        except KeyError:
            raise AttributeError(key)


class Context:
    def __init__(self, inputs: dict[str, Any] | None = None):
        self.inputs: DotDict = DotDict(inputs or {})
        self.vars: DotDict = DotDict()
        self.docs: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.functions: DotDict = DotDict()
        self.scratch: DotDict = DotDict()

    def to_dict(self) -> dict[str, Any]:
        return {
            "inputs": dict(self.inputs),
            "vars": dict(self.vars),
            "docs": dict(self.docs),
            "functions": dict(self.functions),
            "scratch": dict(self.scratch)
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Context":
        ctx = cls(inputs=data.get("inputs", {}))
        ctx.vars = DotDict(data.get("vars", {}))
        ctx.docs = defaultdict(list, data.get("docs", {}))
        ctx.functions = DotDict(data.get("functions", {}))
        ctx.scratch = DotDict(data.get("scratch", {}))
        return ctx

    def get(self, path: str, default=None):
        parts = path.split(".")
        cur: Any = self.to_dict()
        for p in parts:
            if isinstance(cur, dict):
                cur = cur.get(p, default)
            elif isinstance(cur, list):
                try:
                    idx = int(p)
                    cur = cur[idx]
                except (ValueError, IndexError):
                    return default
            else:
                return default
        return cur

    def set(self, path: str, value: Any):
        parts = path.split(".")
        root = parts[0]

        if root == "inputs":
            target = self.inputs
        elif root == "vars":
            target = self.vars
        elif root == "docs":
            target = self.docs
        elif root == "functions":
            target = self.functions
        elif root == "scratch":
            target = self.scratch
        else:
            raise ValueError(f"Unknown context root: {root}")

        parts = parts[1:]
        cur = target
        for p in parts[:-1]:
            if isinstance(cur, dict):
                cur = cur.setdefault(p, DotDict())
            elif isinstance(cur, list):
                idx = int(p)
                while len(cur) <= idx:
                    cur.append(DotDict())
                cur = cur[idx]
            else:
                raise ValueError(f"Can't traverse into {type(cur)}")

        last = parts[-1]
        if isinstance(cur, dict):
            cur[last] = value
        elif isinstance(cur, list):
            idx = int(last)
            while len(cur) <= idx:
                cur.append(None)
            cur[idx] = value
        else:
            raise ValueError(f"Can't set into {type(cur)}")
