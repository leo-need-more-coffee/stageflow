"""Единый язык выражений — CEL.

См. MEMORY_MODEL.md §3. Используется везде, где в пайплайне встречается
выражение: ``condition`` у condition-ноды, ``when`` у switch-ноды и любое
значение с суффиксом ``.$`` в ``arguments``/``outputs``/``variables``.
Переменные фрейма живут в активации под namespace ``vars`` (до 0.7.0—
``local``, см. REFACTORING.md §12); префикс оставлен, чтобы переменная с
именем функции CEL (``size``, ``type``, ``has``) ни с чем не схлопывалась.

Байндинг по умолчанию — ``common-expression-language`` (нативный, ``import
cel``), с ``cel-python`` (``celpy``) как чистопитоновский fallback.
"""
from __future__ import annotations

from typing import Any

from ..exceptions import ExpressionError
from .context import Context

# Совместимость со старым именем исключения.
CelError = ExpressionError

_BACKEND: str | None = None

try:  # предпочтительный нативный байндинг
    import cel as _cel  # type: ignore

    _BACKEND = "cel"
except ImportError:  # pragma: no cover - fallback path
    _cel = None
    try:
        import celpy as _celpy  # type: ignore

        _BACKEND = "celpy"
    except ImportError:
        _celpy = None


class CelEngine:
    """Компилирует каждое выражение один раз: пайплайн переиспользует одни и
    те же строки выражений на каждом прогоне."""

    def __init__(self):
        self._compiled: dict[str, Any] = {}
        if _BACKEND is None:  # pragma: no cover - зависит от окружения
            raise ExpressionError(
                "Не найден ни один CEL-байндинг. Поставь "
                "'common-expression-language' (рекомендуется) или 'cel-python'."
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
                else:  # pragma: no cover - fallback path
                    env = _celpy.Environment()
                    program = env.program(env.compile(expr))
            except Exception as exc:  # noqa: BLE001 - оборачиваем любой сбой бэкенда
                raise ExpressionError(f"Не удалось скомпилировать CEL {expr!r}: {exc}") from exc
            self._compiled[expr] = program
        return program

    def eval(
        self,
        expr: str,
        ctx: Context,
        output: dict | None = None,
        error: dict | None = None,
        item: Any = None,
    ) -> Any:
        """Вычисляет выражение. В активации всегда есть ``vars``;
        ``output`` доступен при вычислении ``outputs.$`` (сырой результат
        стадии), ``error`` — в узле, куда ушёл обработчик ``except``, ``item`` — в
        построчных стадиях вроде FilterListStage."""
        activation: dict[str, Any] = {"vars": dict(ctx.vars)}
        if output is not None:
            activation["output"] = output
        if error is not None:
            activation["error"] = error
        if item is not None:
            activation["item"] = item

        program = self._program(expr)
        try:
            if _BACKEND == "cel":
                return program.execute(activation)
            return program.evaluate(activation)  # pragma: no cover - fallback path
        except Exception as exc:  # noqa: BLE001 - оборачиваем любой сбой бэкенда
            raise ExpressionError(f"Ошибка вычисления CEL {expr!r}: {exc}") from exc
