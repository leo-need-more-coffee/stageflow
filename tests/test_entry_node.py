import asyncio
import unittest

from stageflow import (
    BaseStage,
    Context,
    EntryNode,
    Pipeline,
    Session,
    register_stage,
)
from stageflow.exceptions import PipelineDefinitionError, PipelineValidationError


@register_stage("EchoLocalStage")
class EchoLocalStage(BaseStage):
    """
    description: "Echo the received argument back"
    arguments:
      value:
        type: any
    outputs:
      value:
        type: any
    """

    async def run(self):
        self.set_outputs({"value": self.get_arguments().get("value")})


def _pipeline(entry_node: dict, *rest: dict, **top) -> dict:
    return {"nodes": [entry_node, *rest], **top}


def _run(data: dict, context: Context | None = None):
    pipeline = Pipeline.from_dict(data)
    session = Session(id="t", pipeline=pipeline, context=context)
    return asyncio.run(session.run())


class EntryNodeSeedTests(unittest.TestCase):
    def test_literals_land_in_frame(self):
        result = _run(_pipeline(
            {"id": "start", "type": "entry",
             "variables": {"n": 5, "items": [1, 2, 3], "flag": True, "name": "мир"},
             "next": "done"},
            {"id": "done", "type": "terminal", "result": {"status": "ok"},
             "artifacts": ["n", "items", "flag", "name"]},
        ))
        self.assertEqual(
            result.artifacts,
            {"n": 5, "items": [1, 2, 3], "flag": True, "name": "мир"},
        )

    def test_cel_variable(self):
        result = _run(_pipeline(
            {"id": "start", "type": "entry",
             "variables": {"attempts.$": "0", "greeting.$": "'привет ' + vars.who"},
             "next": "done"},
            {"id": "done", "type": "terminal", "artifacts": ["attempts", "greeting"]},
        ), context=Context(vars={"who": "мир"}))
        self.assertEqual(result.artifacts, {"attempts": 0, "greeting": "привет мир"})

    def test_seeded_variable_feeds_next_stage(self):
        result = _run(_pipeline(
            {"id": "start", "type": "entry", "variables": {"n": 7}, "next": "echo"},
            {"id": "echo", "type": "stage", "stage": "EchoLocalStage",
             "arguments": {"vars": {"value": "n"}},
             "outputs": {"value": "seen"}, "next": "done"},
            {"id": "done", "type": "terminal", "artifacts": ["seen"]},
        ))
        self.assertEqual(result.artifacts, {"seen": 7})

    def test_entry_event_emitted(self):
        events = []
        pipeline = Pipeline.from_dict(_pipeline(
            {"id": "start", "type": "entry", "variables": {"n": 1}, "next": "done"},
            {"id": "done", "type": "terminal"},
        ))
        session = Session(id="t", pipeline=pipeline, event_handler=events.append)
        asyncio.run(session.run())
        seeded = [e for e in events if e.type == "entry_seeded"]
        self.assertEqual(len(seeded), 1)
        self.assertEqual(seeded[0].stage_id, "start")
        self.assertEqual(seeded[0].payload, {"variables": ["n"]})

    def test_expose_works_on_entry(self):
        result = _run(_pipeline(
            {"id": "start", "type": "entry", "variables": {"n": 3},
             "expose": {"n": "копия"}, "next": "done"},
            {"id": "done", "type": "terminal", "artifacts": ["копия"]},
        ))
        self.assertEqual(result.artifacts, {"копия": 3})


class EntryDefaultsTests(unittest.TestCase):
    def test_incoming_value_wins(self):
        result = _run(_pipeline(
            {"id": "start", "type": "entry", "variables": {"n": 5}, "next": "done"},
            {"id": "done", "type": "terminal", "artifacts": ["n"]},
        ), context=Context(vars={"n": 42}))
        self.assertEqual(result.artifacts, {"n": 42})

    def test_default_applies_when_absent(self):
        result = _run(_pipeline(
            {"id": "start", "type": "entry", "variables": {"n": 5, "m": 1}, "next": "done"},
            {"id": "done", "type": "terminal", "artifacts": ["n", "m"]},
        ), context=Context(vars={"n": 42}))
        self.assertEqual(result.artifacts, {"n": 42, "m": 1})

    def test_occupied_name_skips_expression(self):
        result = _run(_pipeline(
            {"id": "start", "type": "entry", "variables": {"n.$": "vars.missing + 1"},
             "next": "done"},
            {"id": "done", "type": "terminal", "artifacts": ["n"]},
        ), context=Context(vars={"n": 9}))
        self.assertEqual(result.artifacts, {"n": 9})

    def test_defaults_fill_subpipeline_inputs(self):
        result = _run({
            "entry": "call",
            "nodes": [
                {"id": "call", "type": "subpipeline", "subpipeline_id": "child",
                 "inputs": {"given": "outer"}, "artifact_outputs": {"got": "sum"},
                 "next": "done"},
                {"id": "done", "type": "terminal", "artifacts": ["got"]},
            ],
            "subpipelines": {
                "child": {"nodes": [
                    {"id": "begin", "type": "entry",
                     "variables": {"given": 0, "extra": 10, "sum.$": "vars.given + 10"},
                     "next": "end"},
                    {"id": "end", "type": "terminal", "artifacts": ["sum"]},
                ]},
            },
        }, context=Context(vars={"outer": 5}))
        self.assertEqual(result.artifacts, {"got": 15})


class EntrySeedOrderTests(unittest.TestCase):
    def _run_pair(self, variables: dict):
        return _run(_pipeline(
            {"id": "start", "type": "entry", "variables": variables, "next": "done"},
            {"id": "done", "type": "terminal", "artifacts": ["n", "total"]},
        )).artifacts

    def test_expression_sees_sibling(self):
        self.assertEqual(
            self._run_pair({"n": 5, "total.$": "vars.n * 2"}),
            {"n": 5, "total": 10},
        )

    def test_key_order_does_not_matter(self):
        self.assertEqual(
            self._run_pair({"total.$": "vars.n * 2", "n": 5}),
            self._run_pair({"n": 5, "total.$": "vars.n * 2"}),
        )

    def test_chain_of_three(self):
        result = _run(_pipeline(
            {"id": "start", "type": "entry", "variables": {
                "c.$": "vars.b + 1", "a": 1, "b.$": "vars.a + 1",
            }, "next": "done"},
            {"id": "done", "type": "terminal", "artifacts": ["a", "b", "c"]},
        ))
        self.assertEqual(result.artifacts, {"a": 1, "b": 2, "c": 3})

    def test_incoming_value_wins_over_dependency_chain(self):
        result = _run(_pipeline(
            {"id": "start", "type": "entry",
             "variables": {"n": 5, "total.$": "vars.n * 2"}, "next": "done"},
            {"id": "done", "type": "terminal", "artifacts": ["n", "total"]},
        ), context=Context(vars={"n": 10}))
        self.assertEqual(result.artifacts, {"n": 10, "total": 20})


class EntryNonAsciiNameTests(unittest.TestCase):
    def test_name_is_usable_through_index_access(self):
        result = _run(_pipeline(
            {"id": "start", "type": "entry", "variables": {
                "n": 5,
                "итог.$": "vars.n * 2",
                "вывод.$": "'итог = ' + string(vars['итог'])",
            }, "next": "done"},
            {"id": "done", "type": "terminal", "artifacts": ["итог", "вывод"]},
        ))
        self.assertEqual(result.artifacts, {"итог": 10, "вывод": "итог = 10"})

    def test_index_access_counts_as_dependency(self):
        node = Pipeline.from_dict(_pipeline(
            {"id": "start", "type": "entry", "variables": {
                "вывод.$": "string(vars['итог'])", "итог.$": "vars.n * 2", "n": 5,
            }, "next": "done"},
            {"id": "done", "type": "terminal"},
        )).get_node("start")
        self.assertEqual([name for name, _, _ in node.seed_plan()],
                         ["n", "итог", "вывод"])

    def test_cycle_through_index_access_rejected(self):
        pipeline = Pipeline.from_dict(_pipeline(
            {"id": "start", "type": "entry",
             "variables": {"x.$": "vars['y']", "y.$": "vars.x"}, "next": "done"},
            {"id": "done", "type": "terminal"},
        ))
        self.assertIn("циклическая зависимость переменных: x, y",
                      " ".join(pipeline.collect_errors()))


class EntryPointDerivationTests(unittest.TestCase):
    def test_entry_field_optional(self):
        pipeline = Pipeline.from_dict(_pipeline(
            {"id": "start", "type": "entry", "next": "done"},
            {"id": "done", "type": "terminal"},
        ))
        self.assertEqual(pipeline.entry, "start")
        self.assertEqual(pipeline.collect_errors(), [])
        self.assertIsInstance(pipeline.get_entry_node(), EntryNode)

    def test_entry_field_still_required_without_entry_node(self):
        pipeline = Pipeline.from_dict({"nodes": [{"id": "done", "type": "terminal"}]})
        self.assertIn("В пайплайне не задан entry", pipeline.collect_errors())

    def test_matching_entry_field_accepted(self):
        pipeline = Pipeline.from_dict({"entry": "start", "nodes": [
            {"id": "start", "type": "entry", "next": "done"},
            {"id": "done", "type": "terminal"},
        ]})
        self.assertEqual(pipeline.collect_errors(), [])

    def test_conflicting_entry_field_rejected(self):
        pipeline = Pipeline.from_dict({"entry": "done", "nodes": [
            {"id": "start", "type": "entry", "next": "done"},
            {"id": "done", "type": "terminal"},
        ]})
        errors = " ".join(pipeline.collect_errors())
        self.assertIn("не является точкой входа", errors)

    def test_two_entry_nodes_rejected(self):
        pipeline = Pipeline.from_dict({"entry": "a", "nodes": [
            {"id": "a", "type": "entry", "next": "done"},
            {"id": "b", "type": "entry", "next": "done"},
            {"id": "done", "type": "terminal"},
        ]})
        errors = " ".join(pipeline.collect_errors())
        self.assertIn("больше одного", errors)

    def test_two_entry_nodes_without_field_report_only_the_cause(self):
        pipeline = Pipeline.from_dict(_pipeline(
            {"id": "a", "type": "entry", "next": "done"},
            {"id": "b", "type": "entry", "next": "done"},
            {"id": "done", "type": "terminal"},
        ))
        self.assertEqual(
            pipeline.collect_errors(),
            ["узлов entry больше одного: a, b — точка входа должна быть одна"],
        )

    def test_jump_back_into_entry_rejected(self):
        pipeline = Pipeline.from_dict(_pipeline(
            {"id": "start", "type": "entry", "next": "back"},
            {"id": "back", "type": "condition", "condition": "true",
             "then": "start", "else": "done"},
            {"id": "done", "type": "terminal"},
        ))
        errors = " ".join(pipeline.collect_errors())
        self.assertIn("переход в точку входа", errors)


class EntryValidationTests(unittest.TestCase):
    def test_bad_next(self):
        pipeline = Pipeline.from_dict(_pipeline(
            {"id": "start", "type": "entry", "next": "нет-такого"},
            {"id": "done", "type": "terminal"},
        ))
        self.assertIn("start: next 'нет-такого' не найден в графе",
                      pipeline.collect_errors())

    def test_cycle_rejected(self):
        pipeline = Pipeline.from_dict(_pipeline(
            {"id": "start", "type": "entry",
             "variables": {"a.$": "vars.b + 1", "b.$": "vars.a + 1"}, "next": "done"},
            {"id": "done", "type": "terminal"},
        ))
        errors = " ".join(pipeline.collect_errors())
        self.assertIn("циклическая зависимость переменных: a, b", errors)

    def test_empty_expression_rejected(self):
        pipeline = Pipeline.from_dict(_pipeline(
            {"id": "start", "type": "entry", "variables": {"n.$": "  "}, "next": "done"},
            {"id": "done", "type": "terminal"},
        ))
        self.assertIn("пустое выражение", " ".join(pipeline.collect_errors()))

    def test_bad_variable_name_rejected_by_schema(self):
        with self.assertRaises(PipelineDefinitionError):
            Pipeline.from_dict(_pipeline(
                {"id": "start", "type": "entry", "variables": {"не имя": 1},
                 "next": "done"},
                {"id": "done", "type": "terminal"},
            ))
        ok = Pipeline.from_dict(_pipeline(
            {"id": "start", "type": "entry", "variables": {"счётчик": 1}, "next": "done"},
            {"id": "done", "type": "terminal"},
        ))
        self.assertEqual(ok.collect_errors(), [])

    def test_declared_type_checked_statically(self):
        pipeline = Pipeline.from_dict(_pipeline(
            {"id": "start", "type": "entry", "variables": {"n": "пять"}, "next": "done"},
            {"id": "done", "type": "terminal"},
            variables={"n": "int"},
        ))
        self.assertIn("ожидался int", " ".join(pipeline.collect_errors()))
        with self.assertRaises(PipelineValidationError):
            Session(id="t", pipeline=pipeline)

    def test_unknown_field_rejected_by_schema(self):
        with self.assertRaises(PipelineDefinitionError):
            Pipeline.from_dict(_pipeline(
                {"id": "start", "type": "entry", "outputs": {"a": "b"},
                 "next": "done"},
                {"id": "done", "type": "terminal"},
            ))

    def test_scalar_variables_rejected_by_schema(self):
        with self.assertRaises(PipelineDefinitionError):
            Pipeline.from_dict(_pipeline(
                {"id": "start", "type": "entry", "variables": 5, "next": "done"},
                {"id": "done", "type": "terminal"},
            ))


if __name__ == "__main__":
    unittest.main()
