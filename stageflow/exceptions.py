"""Иерархия исключений StageFlow.

Каждое исключение наследует и :class:`StageFlowError`, и подходящий встроенный
тип (``ValueError``/``KeyError``/``RuntimeError``). Это даёт две возможности
одновременно: ловить всё, что связано с фреймворком, одним ``except
StageFlowError``, и не ломать существующий код, который ловил встроенные типы.
"""
from __future__ import annotations


class StageFlowError(Exception):
    """Базовый класс всех ошибок фреймворка.

    ``__str__`` задан здесь намеренно: часть наследников смешана с
    ``KeyError``, а у него ``__str__`` — это ``repr`` аргумента, и сообщение
    приезжало бы в кавычках всюду, где берётся ``str(exc)`` (события
    ``try_caught``, ``error_payload.message``, логи, панель отладки).
    """

    def __str__(self) -> str:
        if len(self.args) == 1 and isinstance(self.args[0], str):
            return self.args[0]
        return Exception.__str__(self)


class RegistryError(StageFlowError, ValueError):
    """Проблема реестра: повторная регистрация или запрос незарегистрированного
    имени (стадии, типа узла)."""


class PipelineDefinitionError(StageFlowError, ValueError):
    """Описание пайплайна синтаксически некорректно: не прошло JSON Schema
    или у узла отсутствует обязательное поле. Возникает на этапе разбора."""


class PipelineValidationError(StageFlowError, ValueError):
    """Граф пайплайна не прошёл перекрёстные проверки (битые ссылки ``next``,
    дубли id и т.п.). Полный список нарушений — в :attr:`errors`."""

    def __init__(self, errors: list[str]):
        self.errors = list(errors)
        super().__init__("Pipeline validation failed: " + "; ".join(errors))


class ExpressionError(StageFlowError, RuntimeError):
    """Ошибка компиляции или вычисления CEL-выражения."""


class PayloadValidationError(StageFlowError, ValueError):
    """Payload события или ввода не соответствует объявленной схеме."""


class StageContractError(StageFlowError, ValueError):
    """Стадия использована вне своего контракта: не тот тип события/ввода
    или некорректные аргументы."""


class StageOutputError(StageFlowError, KeyError):
    """Стадия не вернула поле, объявленное в ``outputs`` узла."""


class ArtifactNotFoundError(StageFlowError, KeyError):
    """Субпайплайн не отдал артефакт, объявленный в ``artifact_outputs``."""


class BranchError(StageFlowError, RuntimeError):
    """Ошибка parallel-узла: падение ветки или конфликт записи при слиянии."""


class TypeDeclarationError(StageFlowError, ValueError):
    """Некорректное объявление типа: синтаксис выражения, битая структура
    или ссылка на неизвестный именованный тип."""


class TypeCheckError(StageFlowError, TypeError):
    """Значение переменной не соответствует объявленному типу."""
