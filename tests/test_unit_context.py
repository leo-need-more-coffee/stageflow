"""Unit: the frame — one immutable map of variables."""
import unittest

import immutables

from stageflow import Context
from stageflow.exceptions import PipelineDefinitionError


class ContextTests(unittest.TestCase):
    def test_empty_by_default(self):
        ctx = Context()
        self.assertEqual(ctx.var_names(), frozenset())
        self.assertIsNone(ctx.get_var("x"))
        self.assertEqual(ctx.get_var("x", 5), 5)

    def test_a_dict_or_a_map(self):
        self.assertEqual(Context({"a": 1}).get_var("a"), 1)
        self.assertEqual(Context(immutables.Map(a=1)).get_var("a"), 1)

    def test_a_write_is_a_new_frame(self):
        base = Context({"a": 1})
        changed = base.with_var("a", 2).with_var("b", 3)
        self.assertEqual((base.get_var("a"), base.has_var("b")), (1, False))
        self.assertEqual((changed.get_var("a"), changed.get_var("b")), (2, 3))

    def test_dropping(self):
        base = Context({"a": 1, "b": 2})
        self.assertEqual(base.without_var("a").var_names(), frozenset({"b"}))
        self.assertTrue(base.has_var("a"))
        self.assertIs(base.without_var("zzz"), base, "nothing to drop, same frame")

    def test_a_value_none_is_still_present(self):
        ctx = Context({"a": None})
        self.assertTrue(ctx.has_var("a"))

    def test_round_trip(self):
        ctx = Context({"a": 1, "nested": {"b": [1, 2]}})
        self.assertEqual(Context.from_dict(ctx.to_dict()).to_dict(), ctx.to_dict())
        self.assertEqual(Context.from_dict({}).var_names(), frozenset())

    def test_a_pre_0_7_snapshot_is_refused_by_name(self):
        with self.assertRaisesRegex(PipelineDefinitionError, "'local'"):
            Context.from_dict({"local": {"a": 1}})

    def test_the_map_itself_cannot_be_written(self):
        with self.assertRaises((TypeError, AttributeError)):
            Context({"a": 1}).vars["a"] = 2  # type: ignore[index]


if __name__ == "__main__":
    unittest.main()
