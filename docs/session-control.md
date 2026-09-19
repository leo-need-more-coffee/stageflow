# Session control

```python
session.stop(); session.pause(); session.resume()
await session.input("command", {"name": "skip"})
```

User input: a stage declares `allowed_inputs` and awaits
`await self.wait_input("user_input", timeout=...)`; input is delivered from
outside with `await session.input("user_input", {...})`. The payload is
validated against the `payload_schema` from the declaration.

Snapshots: `session.snapshot()` returns a dict, `Session.from_snapshot(snap)`
restores the session, and `run()` resumes from the saved node.
