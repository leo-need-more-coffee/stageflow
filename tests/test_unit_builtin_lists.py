"""Unit: the built-in list stages, each run on its own."""
import unittest

from stageflow.builtins._args import (
    require_dict,
    require_list,
    require_number,
    require_present,
)
from stageflow.builtins.lists import (
    AppendListStage,
    ExtendListStage,
    FilterListStage,
    PopListStage,
    UniqueListStage,
)
from stageflow.exceptions import ExpressionError, StageContractError

from _support import run, run_stage


class ArgumentGuardsTests(unittest.TestCase):
    def test_the_right_shape_passes_through(self):
        self.assertEqual(require_list("S", "f", [1]), [1])
        self.assertEqual(require_dict("S", "f", {"a": 1}), {"a": 1})
        self.assertEqual(require_number("S", "f", 2.5), 2.5)
        self.assertEqual(require_present("S", "f", 0), 0)

    def test_the_wrong_shape_names_stage_field_and_type(self):
        cases = [
            (require_list, "x", "'f' must be a list, got str"),
            (require_dict, [], "'f' must be an object, got list"),
            (require_number, "1", "'f' must be a number, got str"),
            (require_present, None, "argument 'f' is required"),
        ]
        for guard, value, message in cases:
            with self.subTest(guard=guard.__name__):
                with self.assertRaisesRegex(StageContractError, f"S: {message}"):
                    guard("S", "f", value)

    def test_a_bool_is_not_a_number(self):
        with self.assertRaisesRegex(StageContractError, "got bool"):
            require_number("S", "f", True)


class AppendExtendTests(unittest.TestCase):
    def test_append(self):
        self.assertEqual(run(run_stage(AppendListStage, list=[1], value=2)), {"list": [1, 2]})
        self.assertEqual(run(run_stage(AppendListStage, value=1)), {"list": [1]})

    def test_append_needs_a_value_and_none_is_one(self):
        self.assertEqual(run(run_stage(AppendListStage, value=None)), {"list": [None]})
        with self.assertRaisesRegex(StageContractError, "'value' is required"):
            run(run_stage(AppendListStage, list=[1]))

    def test_append_to_a_non_list(self):
        with self.assertRaises(StageContractError):
            run(run_stage(AppendListStage, list="abc", value=1))

    def test_extend(self):
        self.assertEqual(run(run_stage(ExtendListStage, list=[1], items=[2, 3])),
                         {"list": [1, 2, 3]})
        self.assertEqual(run(run_stage(ExtendListStage, items=(4,))), {"list": [4]})

    def test_extend_with_a_non_list(self):
        with self.assertRaisesRegex(StageContractError, "'items' must be a list"):
            run(run_stage(ExtendListStage, items="abc"))


class FilterTests(unittest.TestCase):
    def test_by_condition(self):
        got = run(run_stage(FilterListStage, items=[1, 2, 3, 4], condition="item > 2"))
        self.assertEqual(got, {"list": [3, 4]})

    def test_other_arguments_are_vars(self):
        got = run(run_stage(FilterListStage, items=[1, 5, 9],
                            condition="item >= vars.floor", floor=5))
        self.assertEqual(got, {"list": [5, 9]})

    def test_the_condition_is_required(self):
        with self.assertRaisesRegex(StageContractError, "'condition' is required"):
            run(run_stage(FilterListStage, items=[1]))

    def test_a_null_element_is_an_element(self):
        got = run(run_stage(FilterListStage, items=[None, 1, None, 2],
                            condition="item != null"))
        self.assertEqual(got, {"list": [1, 2]})

    def test_a_broken_condition_is_an_expression_error(self):
        with self.assertRaises(ExpressionError):
            run(run_stage(FilterListStage, items=[1], condition="item >"))


class UniqueTests(unittest.TestCase):
    def test_order_is_kept(self):
        got = run(run_stage(UniqueListStage, items=[3, 1, 3, 2, 1]))
        self.assertEqual(got, {"list": [3, 1, 2]})

    def test_objects_and_lists_are_compared_by_value(self):
        items = [{"id": 1}, {"id": 2}, {"id": 1}, [1, 2], [1, 2]]
        got = run(run_stage(UniqueListStage, items=items))
        self.assertEqual(got, {"list": [{"id": 1}, {"id": 2}, [1, 2]]})

    def test_one_and_true_stay_apart(self):
        # Python says 1 == True; JSON and CEL do not
        got = run(run_stage(UniqueListStage, items=[1, True, 1.0, "1"]))
        self.assertEqual(got, {"list": [1, True, "1"]})


class PopTests(unittest.TestCase):
    def test_last_by_default(self):
        got = run(run_stage(PopListStage, items=[1, 2, 3]))
        self.assertEqual(got, {"list": [1, 2], "popped": 3})

    def test_by_index(self):
        got = run(run_stage(PopListStage, items=[1, 2, 3], index=0))
        self.assertEqual(got, {"list": [2, 3], "popped": 1})

    def test_out_of_range(self):
        with self.assertRaisesRegex(StageContractError, "index 5 is out of range"):
            run(run_stage(PopListStage, items=[1], index=5))

    def test_the_input_is_not_changed(self):
        source = [1, 2]
        run(run_stage(PopListStage, items=source))
        self.assertEqual(source, [1, 2])


if __name__ == "__main__":
    unittest.main()
