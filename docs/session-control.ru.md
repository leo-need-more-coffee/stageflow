# Управление сессией

```python
session.stop(); session.pause(); session.resume()
await session.input("command", {"name": "skip"})
```

Пользовательский ввод: стадия объявляет `allowed_inputs` и ждёт
`await self.wait_input("user_input", timeout=...)`; снаружи ввод подаётся через
`await session.input("user_input", {...})`. Payload проверяется по
`payload_schema` из объявления.

Снапшоты: `session.snapshot()` возвращает dict, `Session.from_snapshot(snap)`
восстанавливает сессию, `run()` продолжает с сохранённого узла.
