"""Unit: how arguments are read from the frame and outputs written back."""
import unittest
from types import SimpleNamespace

from stageflow import CelEngine, Context, TypeSystem
from stageflow.core.nodes.bindings import (
    apply_outputs,
    normalize_bucket,
    resolve_arguments,
)
from stageflow.exceptions import (
    PipelineDefinitionError,
    StageOutputError,
    TypeCheckError,
)

CEL = CelEngine()


class NormalizeBucketTests(unittest.TestCase):
    def test_shapes(self):
        self.assertEqual(normalize_bucket(None), {})
        self.assertEqual(normalize_bucket(["a", "b"]), {"a": "a", "b": "b"})
        self.assertEqual(normalize_bucket({"arg": "var"}), {"arg": "var"})

    def test_a_dict_is_copied(self):
        source = {"arg": "var"}
        normalize_bucket(source)["x"] = "y"
        self.assertEqual(source, {"arg": "var"})

    def test_anything_else_is_a_definition_error(self):
        for bad in ("a", 1, 2.5, True):
            with self.subTest(bad=bad):
                with self.assertRaises(PipelineDefinitionError):
                    normalize_bucket(bad)


class ResolveArgumentsTests(unittest.TestCase):
    CTX = Context(vars={"name": "Alice", "n": 2, "items": [1, 2, 3]})

    def test_constants_as_written(self):
        got = resolve_arguments({"const": {"greeting": "hi", "times": 3}}, self.CTX, CEL)
        self.assertEqual(got, {"greeting": "hi", "times": 3})

    def test_variables_by_reference(self):
        got = resolve_arguments({"vars": {"who": "name"}}, self.CTX, CEL)
        self.assertEqual(got, {"who": "Alice"})

    def test_a_list_of_variables_keeps_their_names(self):
        got = resolve_arguments({"vars": ["name", "n"]}, self.CTX, CEL)
        self.assertEqual(got, {"name": "Alice", "n": 2})

    def test_a_missing_variable_is_none(self):
        got = resolve_arguments({"vars": {"who": "nobody"}}, self.CTX, CEL)
        self.assertEqual(got, {"who": None})

    def test_cel_in_either_bucket(self):
        got = resolve_arguments({
            "const": {"double.$": "vars.n * 2"},
            "vars": {"count.$": "size(vars.items)"},
        }, self.CTX, CEL)
        self.assertEqual(got, {"double": 4, "count": 3})

    def test_a_variable_wins_over_a_constant_of_the_same_name(self):
        got = resolve_arguments({"const": {"who": "Bob"}, "vars": {"who": "name"}},
                                self.CTX, CEL)
        self.assertEqual(got, {"who": "Alice"})

    def test_extra_names_reach_the_expression(self):
        got = resolve_arguments({"const": {"x.$": "item + 1"}}, self.CTX, CEL, item=41)
        self.assertEqual(got, {"x": 42})

    def test_no_buckets_no_arguments(self):
        self.assertEqual(resolve_arguments({}, self.CTX, CEL), {})


class ApplyOutputsTests(unittest.TestCase):
    CTX = Context(vars={"keep": 1})

    def test_fields_are_written_to_their_variables(self):
        ctx = apply_outputs({"greeting": "g"}, self.CTX, {"greeting": "hi"}, CEL)
        self.assertEqual(ctx.get_var("g"), "hi")
        self.assertEqual(ctx.get_var("keep"), 1)
        self.assertFalse(self.CTX.has_var("g"), "the old frame is untouched")

    def test_a_missing_field_says_what_there_was(self):
        with self.assertRaisesRegex(StageOutputError, r"'nope'.*\['greeting'\]"):
            apply_outputs({"nope": "x"}, self.CTX, {"greeting": "hi"}, CEL)

    def test_cel_sees_the_output_and_the_frame(self):
        ctx = apply_outputs({"total.$": "output.a + vars.keep"},
                            self.CTX, {"a": 10}, CEL)
        self.assertEqual(ctx.get_var("total"), 11)

    def test_cel_sees_the_frame_as_it_was_before_this_node(self):
        # writes are collected first and applied after: one output cannot
        # see another output of the same node through `vars`
        ctx = apply_outputs({"a": "keep", "b.$": "vars.keep"},
                            self.CTX, {"a": 99}, CEL)
        self.assertEqual((ctx.get_var("keep"), ctx.get_var("b")), (99, 1))

    def test_an_object_result_is_read_by_attribute(self):
        ctx = apply_outputs({"x": "x"}, self.CTX, SimpleNamespace(x=5), CEL)
        self.assertEqual(ctx.get_var("x"), 5)

    def test_a_bare_value_is_called_value(self):
        ctx = apply_outputs({"value": "v"}, self.CTX, 42, CEL)
        self.assertEqual(ctx.get_var("v"), 42)

    def test_none_is_no_fields(self):
        self.assertIs(apply_outputs({}, self.CTX, None, CEL), self.CTX)
        with self.assertRaises(StageOutputError):
            apply_outputs({"x": "x"}, self.CTX, None, CEL)

    def test_declared_types_are_checked_on_write(self):
        types = TypeSystem.from_dict(None, {"count": "int"})
        apply_outputs({"n": "count"}, self.CTX, {"n": 3}, CEL, types=types)
        with self.assertRaises(TypeCheckError):
            apply_outputs({"n": "count"}, self.CTX, {"n": "three"}, CEL,
                          types=types, owner="node")


if __name__ == "__main__":
    unittest.main()
