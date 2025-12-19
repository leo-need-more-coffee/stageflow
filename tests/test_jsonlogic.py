import unittest

from stageflow.core.jsonlogic import JsonLogic
from stageflow.core.context import Context


class JsonLogicTests(unittest.TestCase):
    def test_var_flat_and_payload_prefixed(self):
        ctx = Context(payload={"a": 1, "nested": {"b": 2}})
        self.assertEqual(JsonLogic({"var": "a"}).evaluate(ctx), 1)
        self.assertEqual(JsonLogic({"var": "nested.b"}).evaluate(ctx), 2)
        # payload. prefix should also work
        self.assertEqual(JsonLogic({"var": "payload.a"}).evaluate(ctx), 1)

    def test_operators_and_or_eq(self):
        ctx = Context(payload={"x": 5, "y": 10})
        cond = JsonLogic({"and": [
            {"==": [{"var": "x"}, 5]},
            {">": [{"var": "y"}, 3]},
        ]})
        self.assertTrue(cond.evaluate(ctx))
        cond2 = JsonLogic({"or": [
            {"==": [{"var": "x"}, 1]},
            {"==": [{"var": "y"}, 10]},
        ]})
        self.assertTrue(cond2.evaluate(ctx))

    def test_missing_var_returns_none(self):
        ctx = Context(payload={"x": 1})
        self.assertIsNone(JsonLogic({"var": "unknown"}).evaluate(ctx))


if __name__ == "__main__":
    unittest.main()
