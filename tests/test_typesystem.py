import unittest

from stageflow import (
    BaseStage,
    Context,
    Pipeline,
    Session,
    TypeSystem,
    parse_type,
    register_stage,
)
from stageflow.core.typesys import TypeRegistry
from stageflow.exceptions import (
    PipelineValidationError,
    TypeCheckError,
    TypeDeclarationError,
)


@register_stage("ProduceStage")
class ProduceStage(BaseStage):
    """
    description: "Returns its 'value' argument as output"
    arguments:
      value:
        type: any
    outputs:
      value:
        type: any
    """

    async def run(self):
        self.set_outputs({"value": self.get_arguments().get("value")})


@register_stage("TypedIntStage")
class TypedIntStage(BaseStage):
    """
    description: "Declares typed int argument and string output"
    arguments:
      count:
        type: int
    outputs:
      text:
        type: string
    """

    async def run(self):
        self.set_outputs({"text": str(self.get_arguments().get("count"))})


def _check(expr: str, value, types: dict | None = None):
    registry = TypeRegistry.from_dict(types)
    parse_type(expr).check(value, "vars.x", registry)


class TypeExpressionTests(unittest.TestCase):
    def test_primitives(self):
        _check("int", 5)
        _check("string", "hi")
        _check("bool", True)
        _check("number", 1.5)
        _check("float", 3)  # int допустим там, где ждут float
        with self.assertRaises(TypeCheckError):
            _check("int", True)  # bool — не int
        with self.assertRaises(TypeCheckError):
            _check("string", 5)

    def test_containers_nested(self):
        _check("list<int>", [1, 2, 3])
        _check("list<list<string>>", [["a"], []])
        _check("map<int>", {"a": 1})
        with self.assertRaises(TypeCheckError) as caught:
            _check("list<int>", [1, "oops"])
        self.assertIn("vars.x[1]", str(caught.exception))
        with self.assertRaises(TypeCheckError):
            _check("map<int>", {"a": "oops"})

    def test_union_and_optional(self):
        _check("int|string", 5)
        _check("int|string", "five")
        _check("int?", None)
        _check("int?", 7)
        with self.assertRaises(TypeCheckError):
            _check("int|string", [])
        with self.assertRaises(TypeCheckError):
            _check("int?", "not int")

    def test_struct_with_optional_and_nested(self):
        types = {
            "User": {
                "id": "int",
                "name": "string",
                "email?": "string",
                "settings": {"theme": "string"},
            },
        }
        _check("User", {"id": 1, "name": "a", "settings": {"theme": "dark"}}, types)
        _check("User", {"id": 1, "name": "a", "email": "e", "settings": {"theme": "d"}}, types)
        with self.assertRaises(TypeCheckError) as caught:
            _check("User", {"id": 1, "settings": {"theme": "d"}}, types)
        self.assertIn("name", str(caught.exception))
        with self.assertRaises(TypeCheckError) as caught:
            _check("User", {"id": 1, "name": "a", "settings": {"theme": 5}}, types)
        self.assertIn("vars.x.settings.theme", str(caught.exception))

    def test_strict_struct_rejects_extras(self):
        types = {"Point": {"fields": {"x": "int", "y": "int"}, "strict": True}}
        _check("Point", {"x": 1, "y": 2}, types)
        with self.assertRaises(TypeCheckError):
            _check("Point", {"x": 1, "y": 2, "z": 3}, types)

    def test_recursive_struct(self):
        types = {"Tree": {"value": "int", "children": "list<Tree>"}}
        tree = {"value": 1, "children": [{"value": 2, "children": []}]}
        _check("Tree", tree, types)
        bad = {"value": 1, "children": [{"value": "x", "children": []}]}
        with self.assertRaises(TypeCheckError):
            _check("Tree", bad, types)

    def test_alias_and_list_of_named(self):
        types = {"UserId": "int", "User": {"id": "UserId"}}
        _check("list<User>", [{"id": 1}, {"id": 2}], types)
        with self.assertRaises(TypeCheckError):
            _check("list<User>", [{"id": "nope"}], types)

    def test_parse_errors(self):
        for bad in ("", "list<int", "спец-символы", "int||string"):
            with self.assertRaises(TypeDeclarationError):
                parse_type(bad)

    def test_unknown_named_type_is_collected(self):
        ts = TypeSystem.from_dict({"A": {"b": "Missing"}}, {"x": "Nope"})
        errors = ts.collect_errors()
        self.assertTrue(any("Missing" in e for e in errors))
        self.assertTrue(any("Nope" in e for e in errors))


def _typed_pipeline(const_value, declared="int") -> dict:
    return {
        "entry": "produce",
        "variables": {"n": declared},
        "nodes": [
            {"id": "produce", "type": "stage", "stage": "ProduceStage",
             "arguments": {"const": {"value": const_value}},
             "outputs": {"value": "n"}, "next": "end"},
            {"id": "end", "type": "terminal", "result": {"status": "ok"}, "artifacts": ["n"]},
        ],
    }


class RuntimeTypeCheckTests(unittest.IsolatedAsyncioTestCase):
    async def test_valid_write_passes(self):
        pipeline = Pipeline.from_dict(_typed_pipeline(42))
        result = await Session(id="t", pipeline=pipeline, context=Context()).run()
        self.assertEqual(result.artifacts["n"], 42)

    async def test_bad_write_fails_with_node_and_path(self):
        pipeline = Pipeline.from_dict(_typed_pipeline("not int"))
        session = Session(id="t", pipeline=pipeline, context=Context())
        with self.assertRaises(TypeCheckError) as caught:
            await session.run()
        message = str(caught.exception)
        self.assertIn("produce", message)
        self.assertIn("vars.n", message)

    async def test_struct_write_checked(self):
        pipeline_data = {
            "entry": "produce",
            "types": {"User": {"id": "int", "name": "string"}},
            "variables": {"user": "User"},
            "nodes": [
                {"id": "produce", "type": "stage", "stage": "ProduceStage",
                 "arguments": {"const": {"value": {"id": 1, "name": 2}}},
                 "outputs": {"value": "user"}, "next": "end"},
                {"id": "end", "type": "terminal", "result": {"status": "ok"}},
            ],
        }
        session = Session(id="t", pipeline=Pipeline.from_dict(pipeline_data), context=Context())
        with self.assertRaises(TypeCheckError) as caught:
            await session.run()
        self.assertIn("vars.user.name", str(caught.exception))

    async def test_seed_context_is_checked(self):
        pipeline = Pipeline.from_dict(_typed_pipeline(1))
        session = Session(id="t", pipeline=pipeline, context=Context(vars={"n": "bad seed"}))
        with self.assertRaises(TypeCheckError):
            await session.run()

    async def test_undeclared_variables_are_not_checked(self):
        pipeline_data = _typed_pipeline(1)
        pipeline_data["nodes"][0]["outputs"] = {"value": "untyped"}
        pipeline_data["nodes"][1]["artifacts"] = ["untyped"]
        pipeline = Pipeline.from_dict(pipeline_data)
        ctx = Context(vars={"whatever": object()})
        result = await Session(id="t", pipeline=pipeline, context=ctx).run()
        self.assertEqual(result.artifacts["untyped"], 1)

    async def test_expose_write_is_type_checked(self):
        """``expose`` — тоже запись во фрейм, значит проходит ту же проверку."""
        pipeline_data = {
            "entry": "produce",
            "variables": {"copy": "int"},
            "nodes": [
                {"id": "produce", "type": "stage", "stage": "ProduceStage",
                 "arguments": {"const": {"value": "oops"}},
                 "outputs": {"value": "src"},
                 "expose": {"src": "copy"}, "next": "end"},
                {"id": "end", "type": "terminal", "result": {"status": "ok"}},
            ],
        }
        session = Session(id="t", pipeline=Pipeline.from_dict(pipeline_data), context=Context())
        with self.assertRaises(TypeCheckError):
            await session.run()


class StaticTypeCheckTests(unittest.TestCase):
    def test_argument_hint_mismatch_fails_validation(self):
        pipeline_data = {
            "entry": "typed",
            "variables": {"n": "string"},  # стадия ждёт int
            "nodes": [
                {"id": "typed", "type": "stage", "stage": "TypedIntStage",
                 "arguments": {"vars": {"count": "n"}}, "next": "end"},
                {"id": "end", "type": "terminal", "result": {"status": "ok"}},
            ],
        }
        pipeline = Pipeline.from_dict(pipeline_data)
        with self.assertRaises(PipelineValidationError) as caught:
            pipeline.validate()
        self.assertIn("count", str(caught.exception))

    def test_output_hint_mismatch_fails_validation(self):
        pipeline_data = {
            "entry": "typed",
            "variables": {"n": "int", "out": "int"},  # выход стадии — string
            "nodes": [
                {"id": "typed", "type": "stage", "stage": "TypedIntStage",
                 "arguments": {"vars": {"count": "n"}},
                 "outputs": {"text": "out"}, "next": "end"},
                {"id": "end", "type": "terminal", "result": {"status": "ok"}},
            ],
        }
        pipeline = Pipeline.from_dict(pipeline_data)
        with self.assertRaises(PipelineValidationError) as caught:
            pipeline.validate()
        self.assertIn("text", str(caught.exception))

    def test_compatible_declarations_validate(self):
        pipeline_data = {
            "entry": "typed",
            "variables": {"n": "int", "out": "string"},
            "nodes": [
                {"id": "typed", "type": "stage", "stage": "TypedIntStage",
                 "arguments": {"vars": {"count": "n"}},
                 "outputs": {"text": "out"}, "next": "end"},
                {"id": "end", "type": "terminal", "result": {"status": "ok"}},
            ],
        }
        Pipeline.from_dict(pipeline_data).validate()

    def test_optional_int_is_compatible_with_int_hint(self):
        pipeline_data = {
            "entry": "typed",
            "variables": {"n": "int?"},
            "nodes": [
                {"id": "typed", "type": "stage", "stage": "TypedIntStage",
                 "arguments": {"vars": {"count": "n"}}, "next": "end"},
                {"id": "end", "type": "terminal", "result": {"status": "ok"}},
            ],
        }
        Pipeline.from_dict(pipeline_data).validate()

    def test_expose_type_mismatch_fails_validation(self):
        pipeline_data = {
            "entry": "cond",
            "variables": {"flag": "bool", "flag_copy": "int"},
            "nodes": [
                {"id": "cond", "type": "condition", "condition": "vars.flag",
                 "expose": {"flag": "flag_copy"},
                 "then": "end"},
                {"id": "end", "type": "terminal", "result": {"status": "ok"}},
            ],
        }
        pipeline = Pipeline.from_dict(pipeline_data)
        with self.assertRaises(PipelineValidationError) as caught:
            pipeline.validate()
        self.assertIn("expose", str(caught.exception))

    def test_bad_declaration_fails_at_parse(self):
        pipeline_data = _typed_pipeline(1, declared="list<int")
        with self.assertRaises(ValueError):
            Pipeline.from_dict(pipeline_data)


class SubpipelineTypeTests(unittest.IsolatedAsyncioTestCase):
    async def test_child_inherits_parent_named_types(self):
        pipeline_data = {
            "entry": "child",
            "types": {"User": {"id": "int"}},
            "variables": {"user": "User"},
            "nodes": [
                {"id": "child", "type": "subpipeline", "subpipeline_id": "make_user",
                 "artifact_outputs": {"user": "made"}, "next": "end"},
                {"id": "end", "type": "terminal", "result": {"status": "ok"}, "artifacts": ["user"]},
            ],
            "subpipelines": {
                "make_user": {
                    "entry": "produce",
                    "variables": {"made": "User"},  # тип из родителя
                    "nodes": [
                        {"id": "produce", "type": "stage", "stage": "ProduceStage",
                         "arguments": {"const": {"value": {"id": 7}}},
                         "outputs": {"value": "made"}, "next": "done"},
                        {"id": "done", "type": "terminal", "artifacts": ["made"]},
                    ],
                },
            },
        }
        pipeline = Pipeline.from_dict(pipeline_data)
        result = await Session(id="t", pipeline=pipeline, context=Context()).run()
        self.assertEqual(result.artifacts["user"], {"id": 7})

    async def test_artifact_output_write_is_checked_in_parent(self):
        pipeline_data = {
            "entry": "child",
            "variables": {"n": "int"},
            "nodes": [
                {"id": "child", "type": "subpipeline", "subpipeline_id": "make",
                 "artifact_outputs": {"n": "made"}, "next": "end"},
                {"id": "end", "type": "terminal", "result": {"status": "ok"}},
            ],
            "subpipelines": {
                "make": {
                    "entry": "produce",
                    "nodes": [
                        {"id": "produce", "type": "stage", "stage": "ProduceStage",
                         "arguments": {"const": {"value": "not int"}},
                         "outputs": {"value": "made"}, "next": "done"},
                        {"id": "done", "type": "terminal", "artifacts": ["made"]},
                    ],
                },
            },
        }
        session = Session(id="t", pipeline=Pipeline.from_dict(pipeline_data), context=Context())
        with self.assertRaises(TypeCheckError):
            await session.run()

class RemovedGlobalScopeTests(unittest.TestCase):
    """Скоуп ``global`` удалён. Пайплайн, который
    им пользовался, должен отвергаться ГРОМКО — иначе запись просто исчезала бы
    молча, а это худший из возможных вариантов миграции."""

    def _rejected(self, data):
        from stageflow.exceptions import PipelineDefinitionError
        with self.assertRaises((PipelineDefinitionError, TypeDeclarationError)) as caught:
            Pipeline.from_dict(data)
        return str(caught.exception)

    def test_global_bucket_in_arguments_rejected(self):
        message = self._rejected({
            "entry": "p", "nodes": [
                {"id": "p", "type": "stage", "stage": "ProduceStage",
                 "arguments": {"global": {"value": "seed"}}, "next": "end"},
                {"id": "end", "type": "terminal", "result": {"status": "ok"}},
            ]})
        self.assertIn("global", message)

    def test_global_bucket_in_outputs_rejected(self):
        """С 0.7.0 ключ верхнего уровня в ``outputs`` — имя поля результата, и
        схема такую форму пропускает; ловит её валидация графа, отдельным
        сообщением про убранный уровень скоупа (иначе текст был бы
        «стадия не возвращает поле 'global'»)."""
        pipeline = Pipeline.from_dict({
            "entry": "p", "nodes": [
                {"id": "p", "type": "stage", "stage": "ProduceStage",
                 "arguments": {"const": {"value": 1}},
                 "outputs": {"global": {"value": "total"}}, "next": "end"},
                {"id": "end", "type": "terminal", "result": {"status": "ok"}},
            ]})
        errors = " ".join(pipeline.collect_errors())
        self.assertIn("global", errors)
        self.assertIn("убран в 0.7.0", errors)

    def test_global_scope_in_variables_rejected(self):
        message = self._rejected({
            "entry": "end", "variables": {"global": {"total": "int"}},
            "nodes": [{"id": "end", "type": "terminal", "result": {"status": "ok"}}]})
        self.assertIn("global", message)

    def test_global_path_in_expose_rejected(self):
        message = self._rejected({
            "entry": "end", "nodes": [
                {"id": "end", "type": "terminal", "result": {"status": "ok"},
                 "expose": {"a": "global.a"}}]})
        self.assertIn("global", message)

    def test_context_has_no_global_api(self):
        ctx = Context(vars={"a": 1})
        for attr in ("global_", "get_global", "set_global", "fork", "resolve"):
            self.assertFalse(hasattr(ctx, attr), f"Context всё ещё несёт '{attr}'")


if __name__ == "__main__":
    unittest.main()
