"""Визуализатор ``stageflow.docs.graph`` против текущего ядра.

Диаграмма — не тест, её никто не запускает, поэтому она тихо отстаёт от
схемы: узел ``try`` появился в 0.4.0, а рисовался как обычная стадия, тело и
обработчики висели сиротами, и картинка показывала неверный поток управления.
Здесь проверяется ровно это — что визуализатор знает все типы узлов и что
демо-пайплайн всё ещё валиден по актуальной схеме.
"""
import re
import unittest

import jsonschema

from stageflow import get_node_types
from stageflow.docs.graph import DEMO_PIPELINE, _SHAPES, to_mermaid
from stageflow.docs.schema import load_pipeline_schema

TRY_PIPELINE = {
    "nodes": [
        {"id": "s", "type": "entry", "next": "guard"},
        {"id": "guard", "type": "try", "body": "work", "next": "done",
         "except": [
             {"error_equals": ["TimeoutError"], "next": "retry_later", "result_var": "error"},
             {"error_equals": ["*"], "next": "give_up", "result_var": "error"},
         ]},
        {"id": "work", "type": "stage", "stage": "SetValueStage",
         "arguments": {"const": {"value": 1}}, "outputs": {"value": "v"}},
        {"id": "retry_later", "type": "stage", "stage": "SetValueStage",
         "arguments": {"const": {"value": 0}}, "outputs": {"value": "v"}, "next": "done"},
        {"id": "give_up", "type": "terminal", "result": {"status": "failed"}},
        {"id": "done", "type": "terminal", "result": {"status": "ok"}, "artifacts": ["v"]},
    ],
}


_ARROW = re.compile(r"-->|-\.->|==>")


def _edge_lines(mermaid: str) -> list[str]:
    """Только строки-рёбра: сплошная, пунктирная или жирная стрелка."""
    return [line.strip() for line in mermaid.splitlines() if _ARROW.search(line)]


def _linked_ids(mermaid: str) -> set[str]:
    """Идентификаторы, реально связанные стрелками. Подписи (``|...|``)
    выкидываются: слово из подписи не должно сойти за ребро."""
    ids: set[str] = set()
    for line in _edge_lines(mermaid):
        bare = re.sub(r"\|[^|]*\|", " ", line)
        ids.update(re.findall(r"[^\W\d]\w*", bare))
    return ids


class NodeShapeTests(unittest.TestCase):
    def test_every_registered_node_type_has_a_shape(self):
        """Новый тип узла легко зарегистрировать и не завести ему форму —
        тогда он молча рисуется прямоугольником стадии."""
        self.assertEqual(set(_SHAPES), set(get_node_types()))


class TryBlockDrawingTests(unittest.TestCase):
    def setUp(self):
        self.mermaid = to_mermaid(TRY_PIPELINE)

    def test_body_and_handlers_have_edges(self):
        self.assertIn("guard -->|body| work", self.mermaid)
        self.assertIn('guard -.->|"except TimeoutError"| retry_later', self.mermaid)
        self.assertIn('guard -.->|"except *"| give_up', self.mermaid)

    def test_block_exit_is_drawn(self):
        self.assertIn("guard --> done", self.mermaid)

    def test_no_orphan_nodes(self):
        """Сирота на диаграмме означает не просто пропущенную стрелку, а
        неверно показанный поток управления."""
        linked = _linked_ids(self.mermaid)
        orphans = [n["id"] for n in TRY_PIPELINE["nodes"] if n["id"] not in linked]
        self.assertEqual(orphans, [])

    def test_handler_result_var_is_a_write(self):
        from stageflow.docs.graph import _writes
        guard = next(n for n in TRY_PIPELINE["nodes"] if n["id"] == "guard")
        self.assertEqual(_writes(guard), ["error", "error"])


class DemoPipelineTests(unittest.TestCase):
    def test_demo_matches_the_current_schema(self):
        """``python -m stageflow.docs.graph`` без аргументов рисует именно
        этот пайплайн — он не должен быть заведомо невалидным."""
        validator = jsonschema.Draft202012Validator(load_pipeline_schema())
        errors = [e.message for e in validator.iter_errors(DEMO_PIPELINE)]
        self.assertEqual(errors, [])

    def test_demo_shows_every_construct_it_declares(self):
        mermaid = to_mermaid(DEMO_PIPELINE)
        linked = _linked_ids(mermaid)
        orphans = [n["id"] for n in DEMO_PIPELINE["nodes"] if n["id"] not in linked]
        self.assertEqual(orphans, [])


if __name__ == "__main__":
    unittest.main()
