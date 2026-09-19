"""Пошаговая отладка сессии.

Проверяется то, ради чего отладчик вообще появился в ядре: остановка МЕЖДУ
узлами (снаружи её не сделать), видимый в этой точке фрейм и правка фрейма,
которая влияет на дальнейшее исполнение — вплоть до выбора ветки.
"""
import asyncio
import time
import unittest

from stageflow import Context, Pipeline, Session, StepDebugger
from stageflow.exceptions import StageFlowError


def chain_pipeline():
    """start → bump (n += 5) → done."""
    return Pipeline.from_dict({
        "nodes": [
            {"id": "start", "type": "entry", "variables": {"n": 1}, "next": "bump"},
            {"id": "bump", "type": "stage", "stage": "IncrementStage",
             "arguments": {"vars": {"current": "n"}, "const": {"delta": 5}},
             "outputs": {"value": "n"}, "next": "done"},
            {"id": "done", "type": "terminal", "artifacts": ["n"]},
        ],
    })


def branch_pipeline():
    """start → check(n > 3) → big / small."""
    return Pipeline.from_dict({
        "nodes": [
            {"id": "start", "type": "entry", "variables": {"n": 1}, "next": "check"},
            {"id": "check", "type": "condition", "condition": "vars.n > 3",
             "then": "big", "else": "small"},
            {"id": "big", "type": "terminal", "result": {"branch": "big"}},
            {"id": "small", "type": "terminal", "result": {"branch": "small"}},
        ],
    })


async def wait_until(predicate, timeout=2.0):
    """Ждёт условия, не занимая цикл событий (сессия крутится рядом)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.01)
    return False


class StepModeTests(unittest.TestCase):
    """Шаг за шагом: сессия стоит перед каждым узлом, пока не скажут «дальше»."""

    def test_steps_node_by_node(self):
        async def scenario():
            events = []
            debugger = StepDebugger(mode="step", on_event=events.append)
            session = Session("s", chain_pipeline(), debugger=debugger)
            task = asyncio.create_task(session.run())

            self.assertTrue(await wait_until(lambda: debugger.waiting))
            self.assertEqual(debugger.node, "start")
            self.assertFalse(task.done(), "сессия не должна идти дальше без команды")

            debugger.step()
            self.assertTrue(await wait_until(lambda: debugger.node == "bump" and debugger.waiting))
            self.assertEqual(debugger.vars.get("n"), 1, "фрейм показан до исполнения узла")

            debugger.step()
            self.assertTrue(await wait_until(lambda: debugger.node == "done" and debugger.waiting))
            self.assertEqual(debugger.vars.get("n"), 6, "после стадии фрейм обновился")

            debugger.resume()
            result = await asyncio.wait_for(task, 2)
            self.assertEqual(result.artifacts, {"n": 6})

            entered = [e["node"] for e in events if e["type"] == "node_enter"]
            self.assertEqual(entered, ["start", "bump", "done"])
            return events

        events = asyncio.run(scenario())
        self.assertTrue(any(e["type"] == "paused" for e in events))

    def test_step_count_runs_several_nodes(self):
        async def scenario():
            debugger = StepDebugger(mode="step")
            session = Session("s", chain_pipeline(), debugger=debugger)
            task = asyncio.create_task(session.run())
            self.assertTrue(await wait_until(lambda: debugger.waiting))
            debugger.step(2)  # пустить два узла и снова встать
            self.assertTrue(await wait_until(lambda: debugger.node == "done" and debugger.waiting))
            self.assertFalse(task.done())
            debugger.resume()
            await asyncio.wait_for(task, 2)

        asyncio.run(scenario())

    def test_pause_stops_before_next_node(self):
        async def scenario():
            debugger = StepDebugger(mode="run")
            session = Session("s", chain_pipeline(), debugger=debugger)
            debugger.pause()  # ещё до старта: встанет на первом же узле
            task = asyncio.create_task(session.run())
            self.assertTrue(await wait_until(lambda: debugger.waiting))
            self.assertEqual(debugger.node, "start")
            debugger.resume()
            await asyncio.wait_for(task, 2)

        asyncio.run(scenario())

    def test_stop_while_paused_ends_session(self):
        async def scenario():
            debugger = StepDebugger(mode="step")
            session = Session("s", chain_pipeline(), debugger=debugger)
            task = asyncio.create_task(session.run())
            self.assertTrue(await wait_until(lambda: debugger.waiting))
            session.stop()
            result = await asyncio.wait_for(task, 2)
            self.assertEqual(result.result, {"result": "stopped"})

        asyncio.run(scenario())


class FrameEditingTests(unittest.TestCase):
    """Правка фрейма в точке остановки — то, чего нельзя сделать снаружи."""

    def test_edit_changes_branch(self):
        async def scenario():
            debugger = StepDebugger(mode="step")
            session = Session("s", branch_pipeline(), debugger=debugger)
            task = asyncio.create_task(session.run())

            self.assertTrue(await wait_until(lambda: debugger.node == "start" and debugger.waiting))
            debugger.step()
            self.assertTrue(await wait_until(lambda: debugger.node == "check" and debugger.waiting))
            debugger.set_vars({"n": 10})  # применится перед узлом `check`
            debugger.resume()
            result = await asyncio.wait_for(task, 2)
            # без правки условие `vars.n > 3` было бы ложным (n = 1)
            self.assertEqual(result.result, {"branch": "big"})

        asyncio.run(scenario())

    def test_drop_variable(self):
        async def scenario():
            debugger = StepDebugger(mode="step")
            session = Session("s", chain_pipeline(), debugger=debugger)
            task = asyncio.create_task(session.run())
            self.assertTrue(await wait_until(lambda: debugger.node == "start" and debugger.waiting))
            debugger.step()
            self.assertTrue(await wait_until(lambda: debugger.node == "bump" and debugger.waiting))
            debugger.set_vars(drop=["n"])
            debugger.resume()
            # аргумент `current` ссылается на удалённую переменную — это ошибка
            # исполнения, а не молчаливый None
            with self.assertRaises(StageFlowError):
                await asyncio.wait_for(task, 2)

        asyncio.run(scenario())

    def test_type_violation_rejected_without_killing_session(self):
        async def scenario():
            events = []
            debugger = StepDebugger(mode="step", on_event=events.append)
            pipeline = Pipeline.from_dict({
                "variables": {"n": "int"},
                "nodes": [
                    {"id": "start", "type": "entry", "variables": {"n": 1}, "next": "done"},
                    {"id": "done", "type": "terminal", "artifacts": ["n"]},
                ],
            })
            session = Session("s", pipeline, debugger=debugger)
            task = asyncio.create_task(session.run())
            self.assertTrue(await wait_until(lambda: debugger.waiting))
            debugger.step()
            self.assertTrue(await wait_until(lambda: debugger.node == "done" and debugger.waiting))
            debugger.set_vars({"n": "не число"})
            debugger.resume()
            result = await asyncio.wait_for(task, 2)
            self.assertEqual(result.artifacts, {"n": 1}, "отвергнутая правка не должна применяться")
            rejected = [e for e in events if e["type"] == "var_rejected"]
            self.assertEqual(len(rejected), 1)
            self.assertEqual(rejected[0]["name"], "n")

        asyncio.run(scenario())


class DelayTests(unittest.TestCase):
    def test_delay_between_nodes(self):
        async def scenario():
            debugger = StepDebugger(mode="run", delay=0.05)
            session = Session("s", chain_pipeline(), debugger=debugger)
            started = time.monotonic()
            await asyncio.wait_for(session.run(), 3)
            return time.monotonic() - started

        elapsed = asyncio.run(scenario())
        self.assertGreaterEqual(elapsed, 0.12, "три узла по 0.05 с задержкой")


class CoverageTests(unittest.TestCase):
    """Отладчик обязан видеть узлы везде, где они исполняются."""

    def test_sees_try_body_and_handler(self):
        pipeline = Pipeline.from_dict({
            "nodes": [
                {"id": "start", "type": "entry", "variables": {}, "next": "guard"},
                {"id": "guard", "type": "try", "body": "boom", "next": "done",
                 "except": [{"error_equals": ["*"], "next": "rescue"}]},
                {"id": "boom", "type": "stage", "stage": "FailStage",
                 "arguments": {"const": {"message": "бум"}}},
                {"id": "rescue", "type": "stage", "stage": "SetValueStage",
                 "arguments": {"const": {"value": 1}}, "outputs": {"value": "saved"},
                 "next": "done"},
                {"id": "done", "type": "terminal", "artifacts": []},
            ],
        })

        async def scenario():
            events = []
            debugger = StepDebugger(mode="run", on_event=events.append)
            await asyncio.wait_for(Session("s", pipeline, debugger=debugger).run(), 3)
            return [e["node"] for e in events if e["type"] == "node_enter"]

        entered = asyncio.run(scenario())
        self.assertIn("boom", entered, "узел внутри тела try должен быть виден отладчику")
        self.assertIn("rescue", entered, "обработчик except — тоже узел")

    def test_sees_parallel_branches_and_subpipeline(self):
        pipeline = Pipeline.from_dict({
            "nodes": [
                {"id": "start", "type": "entry", "variables": {"n": 1}, "next": "fork"},
                {"id": "fork", "type": "parallel", "next": "child",
                 "branches": [{"id": "a", "entry": "one"}, {"id": "b", "entry": "two"}]},
                {"id": "one", "type": "stage", "stage": "SetValueStage",
                 "arguments": {"const": {"value": 1}}, "outputs": {"value": "x"}},
                {"id": "two", "type": "stage", "stage": "SetValueStage",
                 "arguments": {"const": {"value": 2}}, "outputs": {"value": "y"}},
                {"id": "child", "type": "subpipeline", "subpipeline_id": "inner", "next": "done"},
                {"id": "done", "type": "terminal", "artifacts": []},
            ],
            "subpipelines": {
                "inner": {
                    "nodes": [
                        {"id": "inner_start", "type": "entry", "variables": {}, "next": "inner_end"},
                        {"id": "inner_end", "type": "terminal", "artifacts": []},
                    ],
                },
            },
        })

        async def scenario():
            events = []
            debugger = StepDebugger(mode="run", on_event=events.append)
            await asyncio.wait_for(Session("s", pipeline, debugger=debugger).run(), 3)
            return [e["node"] for e in events if e["type"] == "node_enter"]

        entered = asyncio.run(scenario())
        for node in ("one", "two", "inner_start", "inner_end"):
            self.assertIn(node, entered, f"узел '{node}' не дошёл до отладчика")

    def test_without_debugger_nothing_changes(self):
        async def scenario():
            return await asyncio.wait_for(Session("s", chain_pipeline()).run(), 3)

        result = asyncio.run(scenario())
        self.assertEqual(result.artifacts, {"n": 6})


if __name__ == "__main__":
    unittest.main()
