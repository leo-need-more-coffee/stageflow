"""Слово ``local`` убрано из JSON пайплайна.

Скоуп остался один ещё в 0.6.0, поэтому его имя перестало что-либо различать:
уровень ``{"local": {...}}`` в ``outputs``/``variables`` был единственным
возможным, а префикс ``local.`` в ``expose`` схема и так требовала с обеих
сторон. Namespace выражений переименован в ``vars`` — он не исчез, потому что
защищает переменную с именем функции CEL (``size``, ``type``, ``has``).

Старую форму нужно отвергать ГРОМКО: принятая молча, она означала бы пайплайн,
в котором переменная называется ``local``, а ни один объявленный тип не
проверен.
"""
import asyncio
import unittest

from stageflow import BaseStage, Context, Pipeline, Session, register_stage
from stageflow.exceptions import (
    ExpressionError,
    PipelineDefinitionError,
    TypeDeclarationError,
)


@register_stage("PassValueStage")
class PassValueStage(BaseStage):
    """
    description: "Отдаёт аргумент value обратно"
    arguments:
      value:
        type: any
    outputs:
      value:
        type: any
    """

    async def run(self):
        self.set_outputs({"value": self.get_arguments().get("value")})


def _graph(node: dict, **top) -> dict:
    return {"nodes": [node, {"id": "end", "type": "terminal", "result": {"status": "ok"}}], **top}


def _stage_node(**fields) -> dict:
    return {"id": "p", "type": "stage", "stage": "PassValueStage", "next": "end", **fields}


class RemovedLocalScopeTests(unittest.TestCase):
    def _definition_error(self, data: dict) -> str:
        with self.assertRaises((PipelineDefinitionError, TypeDeclarationError)) as caught:
            Pipeline.from_dict(data)
        return str(caught.exception)

    def test_local_bucket_in_arguments_rejected(self):
        message = self._definition_error(_graph(
            _stage_node(arguments={"local": {"value": "seed"}}), entry="p"))
        self.assertIn("local", message)

    def test_local_level_in_outputs_rejected(self):
        """Схема тут пропускает: ключ верхнего уровня теперь и есть имя поля
        результата. Ловит валидация — прицельным сообщением."""
        pipeline = Pipeline.from_dict(_graph(
            _stage_node(arguments={"const": {"value": 1}},
                        outputs={"local": {"value": "total"}}), entry="p"))
        errors = " ".join(pipeline.collect_errors())
        self.assertIn("уровень скоупа 'local' в outputs убран в 0.7.0", errors)

    def test_local_level_in_variables_rejected(self):
        message = self._definition_error(_graph(
            {"id": "p", "type": "entry", "next": "end"},
            variables={"local": {"total": "int"}}))
        self.assertIn("local", message)

    def test_local_prefix_in_expose_rejected(self):
        message = self._definition_error(_graph(
            {"id": "p", "type": "entry", "expose": {"local.a": "local.b"}, "next": "end"}))
        self.assertIn("local.a", message)

    def test_result_local_on_except_rejected(self):
        message = self._definition_error({
            "nodes": [
                {"id": "start", "type": "entry", "next": "guard"},
                {"id": "guard", "type": "try", "body": "end", "next": "end",
                 "except": [{"error_equals": ["*"], "next": "end", "result_local": "err"}]},
                {"id": "end", "type": "terminal", "result": {"status": "ok"}},
            ]})
        self.assertIn("result_local", message)

    def test_local_namespace_gone_from_expressions(self):
        """``local.n`` в выражении — уже не «пустое значение», а ошибка:
        namespace с таким именем в активацию не кладётся вообще."""
        pipeline = Pipeline.from_dict({
            "nodes": [
                {"id": "start", "type": "entry", "variables": {"n": 5}, "next": "check"},
                {"id": "check", "type": "condition", "condition": "local.n > 1",
                 "then": "end"},
                {"id": "end", "type": "terminal", "result": {"status": "ok"}},
            ]})
        with self.assertRaises(ExpressionError):
            asyncio.run(Session(id="t", pipeline=pipeline).run())

    def test_vars_namespace_works(self):
        pipeline = Pipeline.from_dict({
            "nodes": [
                {"id": "start", "type": "entry", "variables": {"n": 5}, "next": "check"},
                {"id": "check", "type": "condition", "condition": "vars.n > 1",
                 "then": "end"},
                {"id": "end", "type": "terminal", "result": {"status": "ok"}},
            ]})
        result = asyncio.run(Session(id="t", pipeline=pipeline).run())
        self.assertEqual(result.result, {"status": "ok"})

    def test_context_has_no_local_api(self):
        ctx = Context(vars={"a": 1})
        for attr in ("local", "get_local", "has_local", "with_local",
                     "without_local", "local_keys"):
            self.assertFalse(hasattr(ctx, attr), f"Context всё ещё несёт '{attr}'")
        self.assertEqual(ctx.to_dict(), {"vars": {"a": 1}})

    def test_old_snapshot_context_rejected(self):
        """Снапшот до 0.7.0: восстановить его как пустой фрейм — худший из
        вариантов, сессия упала бы позже и не там."""
        with self.assertRaises(PipelineDefinitionError):
            Context.from_dict({"local": {"a": 1}})


if __name__ == "__main__":
    unittest.main()
