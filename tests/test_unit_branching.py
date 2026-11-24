"""Unit: the `condition` and `switch` nodes, one at a time, on a stub session."""
import unittest

from stageflow import Context, Pipeline, TerminalNode
from stageflow.core.nodes import ConditionNode, Node, SwitchNode
from stageflow.exceptions import ExpressionError

from _support import StubSession, run


def _pipeline(*nodes):
    """The branching node plus terminals for every id it may jump to."""
    ids = {"yes", "no", "a", "b", "other"}
    return Pipeline(entry=nodes[0].id,
                    nodes=[*nodes, *(TerminalNode(id=i) for i in sorted(ids))])


class ConditionParseTests(unittest.TestCase):
    def test_from_json(self):
        node = Node.from_dict({"id": "c", "type": "condition",
                               "condition": "vars.x > 1", "then": "yes", "else": "no"})
        self.assertIsInstance(node, ConditionNode)
        self.assertEqual((node.condition, node.then, node.else_),
                         ("vars.x > 1", "yes", "no"))
        self.assertEqual(node.order_targets(), ["yes", "no"])

    def test_else_is_optional(self):
        node = Node.from_dict({"id": "c", "type": "condition",
                               "condition": "true", "then": "yes"})
        self.assertIsNone(node.else_)
        self.assertEqual(node.order_targets(), ["yes"])


class ConditionValidateTests(unittest.TestCase):
    def test_a_valid_condition_has_no_errors(self):
        node = ConditionNode(id="c", condition="true", then="yes", else_="no")
        self.assertEqual(node.validate(_pipeline(node)), [])

    def test_unknown_targets_are_named(self):
        node = ConditionNode(id="c", condition="true", then="ghost", else_="phantom")
        errors = node.validate(Pipeline(entry="c", nodes=[node]))
        self.assertEqual(len(errors), 2)
        self.assertIn("then 'ghost' not found", errors[0])
        self.assertIn("else 'phantom' not found", errors[1])


class ConditionExecuteTests(unittest.TestCase):
    def _go(self, node, **vars):
        session = StubSession(_pipeline(node))
        target, ctx = run(node.execute(session, Context(vars=vars)))
        return target, ctx, session

    def test_true_takes_then(self):
        node = ConditionNode(id="c", condition="vars.x > 1", then="yes", else_="no")
        target, _, session = self._go(node, x=5)
        self.assertEqual(target.id, "yes")
        self.assertEqual(session.events,
                         [("condition_evaluated", "c", {"result": True, "next": "yes"})])

    def test_false_takes_else(self):
        node = ConditionNode(id="c", condition="vars.x > 1", then="yes", else_="no")
        target, _, _ = self._go(node, x=0)
        self.assertEqual(target.id, "no")

    def test_false_without_else_ends_the_road(self):
        node = ConditionNode(id="c", condition="false", then="yes")
        target, _, session = self._go(node)
        self.assertIsNone(target)
        self.assertEqual(session.events[0][2], {"result": False, "next": None})

    def test_expose_is_applied_on_the_way_through(self):
        node = ConditionNode(id="c", condition="true", then="yes",
                             expose={"x": "copy"})
        _, ctx, _ = self._go(node, x=7)
        self.assertEqual(ctx.get_var("copy"), 7)

    def test_a_broken_expression_is_an_expression_error(self):
        node = ConditionNode(id="c", condition="vars.missing > 1", then="yes")
        with self.assertRaises(ExpressionError):
            self._go(node)


class SwitchParseTests(unittest.TestCase):
    def test_from_json(self):
        node = Node.from_dict({
            "id": "s", "type": "switch",
            "cases": [{"when": "vars.n == 1", "next": "a"},
                      {"when": "vars.n == 2", "next": "b"}],
            "default": "other",
        })
        self.assertIsInstance(node, SwitchNode)
        self.assertEqual(node.default, "other")
        self.assertEqual(node.order_targets(), ["a", "b", "other"])

    def test_no_cases_and_no_default(self):
        node = Node.from_dict({"id": "s", "type": "switch"})
        self.assertEqual((node.cases, node.default, node.order_targets()), ([], None, []))


class SwitchValidateTests(unittest.TestCase):
    def test_a_valid_switch_has_no_errors(self):
        node = SwitchNode(id="s", cases=[{"when": "true", "next": "a"}], default="other")
        self.assertEqual(node.validate(_pipeline(node)), [])

    def test_a_case_without_when_or_next_is_an_error(self):
        node = SwitchNode(id="s", cases=[{"next": "a"}, {"when": "true"}])
        errors = node.validate(_pipeline(node))
        self.assertEqual(len(errors), 2)
        self.assertTrue(all("needs 'when' and 'next'" in e for e in errors))

    def test_unknown_targets_are_named(self):
        node = SwitchNode(id="s", cases=[{"when": "true", "next": "ghost"}],
                          default="phantom")
        errors = node.validate(Pipeline(entry="s", nodes=[node]))
        self.assertIn("case next 'ghost' not found", errors[0])
        self.assertIn("default 'phantom' not found", errors[1])


class SwitchExecuteTests(unittest.TestCase):
    CASES = [{"when": "vars.n == 1", "next": "a"},
             {"when": "vars.n >= 1", "next": "b"}]

    def _go(self, node, **vars):
        session = StubSession(_pipeline(node))
        target, ctx = run(node.execute(session, Context(vars=vars)))
        return target, ctx, session

    def test_the_first_matching_case_wins(self):
        node = SwitchNode(id="s", cases=self.CASES, default="other")
        target, _, session = self._go(node, n=1)
        self.assertEqual(target.id, "a", "both cases match; order decides")
        self.assertEqual(session.events,
                         [("switch_evaluated", "s", {"matched": "vars.n == 1", "next": "a"})])

    def test_a_later_case(self):
        node = SwitchNode(id="s", cases=self.CASES, default="other")
        target, _, _ = self._go(node, n=5)
        self.assertEqual(target.id, "b")

    def test_no_match_takes_the_default(self):
        node = SwitchNode(id="s", cases=self.CASES, default="other")
        target, _, session = self._go(node, n=0)
        self.assertEqual(target.id, "other")
        self.assertIsNone(session.events[0][2]["matched"])

    def test_no_match_and_no_default_ends_the_road(self):
        node = SwitchNode(id="s", cases=self.CASES)
        target, _, _ = self._go(node, n=0)
        self.assertIsNone(target)

    def test_later_cases_are_not_evaluated_once_one_matched(self):
        # the second case would fail to evaluate; reaching it would be a bug
        node = SwitchNode(id="s", cases=[{"when": "true", "next": "a"},
                                         {"when": "vars.missing", "next": "b"}])
        target, _, _ = self._go(node)
        self.assertEqual(target.id, "a")

    def test_expose(self):
        node = SwitchNode(id="s", cases=[], default="other", expose={"n": "seen"})
        _, ctx, _ = self._go(node, n=3)
        self.assertEqual(ctx.get_var("seen"), 3)


if __name__ == "__main__":
    unittest.main()
