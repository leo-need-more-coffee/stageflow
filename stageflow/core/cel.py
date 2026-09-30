from __future__ import annotations

from typing import Any

from ..exceptions import ExpressionError
from ..i18n import _
from .context import Context

CelError = ExpressionError

_BACKEND: str | None = None

try:
    import cel as _cel  # type: ignore

    _BACKEND = "cel"
except ImportError:  # pragma: no cover
    _cel = None
    try:
        import celpy as _celpy  # type: ignore

        _BACKEND = "celpy"
    except ImportError:
        _celpy = None


class CelEngine:
    def __init__(self):
        self._compiled: dict[str, Any] = {}
        if _BACKEND is None:  # pragma: no cover
            raise ExpressionError(
                _("No CEL backend found. Install "
                  "'common-expression-language' (recommended) or 'cel-python'.")
            )

    @property
    def backend(self) -> str:
        return _BACKEND or "none"

    def _program(self, expr: str) -> Any:
        program = self._compiled.get(expr)
        if program is None:
            try:
                if _BACKEND == "cel":
                    program = _cel.compile(expr)
                else:  # pragma: no cover
                    env = _celpy.Environment()
                    program = env.program(env.compile(expr))
            except Exception as exc:  # noqa: BLE001
                raise ExpressionError(
                    _("Cannot compile CEL {expr}: {reason}", expr=repr(expr), reason=exc)
                ) from exc
            self._compiled[expr] = program
        return program

    def eval(
        self,
        expr: str,
        ctx: Context,
        output: dict | None = None,
        error: dict | None = None,
        item: Any = None,
        args: dict | None = None,
    ) -> Any:
        activation: dict[str, Any] = {"vars": dict(ctx.vars)}
        if output is not None:
            activation["output"] = output
        if args is not None:
            activation["args"] = args
        if error is not None:
            activation["error"] = error
        if item is not None:
            activation["item"] = item

        program = self._program(expr)
        try:
            if _BACKEND == "cel":
                return program.execute(activation)
            return program.evaluate(activation)  # pragma: no cover
        except Exception as exc:  # noqa: BLE001
            raise ExpressionError(
                _("Cannot evaluate CEL {expr}: {reason}", expr=repr(expr), reason=exc)
            ) from exc
