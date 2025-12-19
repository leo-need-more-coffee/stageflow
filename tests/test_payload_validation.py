import unittest
from typing import Optional, Any

from stageflow.core.utils import validate_schema
from stageflow.core.stage import BaseStage
from stageflow.core.event import EventSpec, InputSpec


class DummySession:
    def __init__(self, input_payload=None):
        self.emitted = []
        self._input_payload = input_payload or {"user": {"id": 1}}
        self.id = "dummy"

    def emit(self, event):
        self.emitted.append(event)

    async def wait_input(self, type_, timeout=None):
        return {"type": type_, "payload": self._input_payload}


class PayloadSchemaTests(unittest.TestCase):
    def test_validate_schema_primitives_and_list(self):
        validate_schema(5, int)
        validate_schema(["a", "b"], list[str])
        with self.assertRaises(ValueError):
            validate_schema(["a", 1], list[str])

    def test_validate_schema_dict_nested(self):
        schema = {"user": {"id": int, "tags": list[str]}}
        validate_schema({"user": {"id": 1, "tags": ["x"]}}, schema)
        with self.assertRaises(ValueError):
            validate_schema({"user": {"id": "oops", "tags": ["x"]}}, schema)

    def test_validate_schema_optional_union(self):
        validate_schema(None, Optional[int])
        validate_schema(3, Optional[int])
        with self.assertRaises(ValueError):
            validate_schema("bad", Optional[int])

    def test_validate_schema_typing_dict_and_extra(self):
        validate_schema({"a": 1, "b": 2}, dict[str, int])
        with self.assertRaises(ValueError):
            validate_schema({"a": "str"}, dict[str, int])

    def test_validate_schema_list_literal(self):
        validate_schema([{"id": 1}], [{"id": int}])
        with self.assertRaises(ValueError):
            validate_schema([{"id": "x"}], [{"id": int}])

    def test_validate_schema_any_and_object_skip(self):
        validate_schema({"whatever": 1}, object)
        validate_schema(["anything"], Any)

    def test_validate_schema_missing_key(self):
        with self.assertRaises(ValueError):
            validate_schema({"user": {}}, {"user": {"id": int}})


class StageValidationTests(unittest.IsolatedAsyncioTestCase):
    async def test_emit_validates_payload(self):
        session = DummySession()

        class MyStage(BaseStage):
            allowed_events = [EventSpec(type="progress", payload_schema={"step": int, "tags": list[str]})]

            async def run(self):
                return None

        stage = MyStage(stage_id="s1", config={}, arguments={}, outputs={}, session=session)
        stage.emit("progress", {"step": 1, "tags": ["a", "b"]})
        self.assertEqual(len(session.emitted), 1)
        with self.assertRaises(ValueError):
            stage.emit("progress", {"step": "bad", "tags": ["a"]})

    async def test_wait_input_validates_payload(self):
        session = DummySession(input_payload={"user": {"id": 42}})

        class MyStage(BaseStage):
            allowed_inputs = [InputSpec(type="user", payload_schema={"user": {"id": int}})]

            async def run(self):
                return None

        stage = MyStage(stage_id="s1", config={}, arguments={}, outputs={}, session=session)
        res = await stage.wait_input("user")
        self.assertEqual(res["payload"]["user"]["id"], 42)

        session_bad = DummySession(input_payload={"user": {"id": "oops"}})
        stage_bad = MyStage(stage_id="s1", config={}, arguments={}, outputs={}, session=session_bad)
        with self.assertRaises(ValueError):
            await stage_bad.wait_input("user")

    async def test_allowed_events_inputs_rejection(self):
        session = DummySession()

        class LimitedStage(BaseStage):
            allowed_events = [EventSpec(type="ping")]
            allowed_inputs = [InputSpec(type="only")]

            async def run(self):
                return None

        stage = LimitedStage(stage_id="s1", config={}, arguments={}, outputs={}, session=session)
        stage.emit("ping", {})
        with self.assertRaises(ValueError):
            stage.emit("other", {})

        with self.assertRaises(ValueError):
            await stage.wait_input("other")


if __name__ == "__main__":
    unittest.main()
