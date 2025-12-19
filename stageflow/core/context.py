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
    def __init__(self, payload: dict[str, Any] | None = None):
        self.payload: DotDict = DotDict(payload or {})

    def to_dict(self) -> dict[str, Any]:
        return dict(self._deep_copy(self.payload))

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Context":
        return cls(payload=data)

    def get(self, path: str, default=None):
        parts = path.split(".")
        if parts and parts[0] == "payload":
            parts = parts[1:]
        cur: Any = self.payload
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
        if parts and parts[0] == "payload":
            parts = parts[1:]
        cur = self.payload
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

    def _deep_copy(self, obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: self._deep_copy(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._deep_copy(v) for v in obj]
        return obj
