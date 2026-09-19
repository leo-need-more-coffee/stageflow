import unittest

from stageflow import Context, Pipeline, Session, TerminalNode


def _make_pipeline():
    return Pipeline(entry="end", nodes=[TerminalNode(id="end")])


class SessionWaitInputTests(unittest.IsolatedAsyncioTestCase):
    async def test_start_and_finish_wait_input_success(self):
        session = Session(id="s1", pipeline=_make_pipeline(), context=Context())
        waiter = session.start_wait_input("user")

        await session.input("user", {"id": 1})
        result = await session.finish_wait_input("user", waiter, timeout=1)

        self.assertEqual(result["payload"]["id"], 1)
        self.assertIn("waiting_for_input", [e.type for e in session.event_history])

    async def test_start_finish_timeout_and_pending_consumption(self):
        session = Session(id="s2", pipeline=_make_pipeline(), context=Context())
        waiter = session.start_wait_input("later")
        result = await session.finish_wait_input("later", waiter, timeout=0.01)

        self.assertIsNone(result)
        self.assertIn("input_timeout", [e.type for e in session.event_history])

        await session.input("later", {"value": 2})
        waiter2 = session.start_wait_input("later")
        result2 = await session.finish_wait_input("later", waiter2, timeout=1)
        self.assertEqual(result2["payload"]["value"], 2)

    async def test_second_input_is_buffered_when_future_already_resolved(self):
        """Уже разрешённая, но не снятая футура — не живой получатель:
        второе сообщение обязано попасть в буфер, а не потеряться."""
        session = Session(id="s3", pipeline=_make_pipeline(), context=Context())
        waiter = session.start_wait_input("msg")

        await session.input("msg", {"n": 1})
        await session.input("msg", {"n": 2})

        first = await session.finish_wait_input("msg", waiter, timeout=1)
        self.assertEqual(first["payload"]["n"], 1)

        second = await session.wait_input("msg", timeout=0.1)
        self.assertIsNotNone(second)
        self.assertEqual(second["payload"]["n"], 2)


if __name__ == "__main__":
    unittest.main()
