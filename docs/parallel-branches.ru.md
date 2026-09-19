# Параллельные ветки

```json
{
  "id": "fan_out",
  "type": "parallel",
  "branches": [{ "id": "hash", "entry": "hash_step" },
               { "id": "thumb", "entry": "thumb_step" }],
  "next": "merge"
}
```

Наружу из ветки выходят только имена, которых во фрейме не было до `parallel`;
запись в имя, жившее до входа, остаётся branch-local. Такие имена
перечисляются в событии `parallel_completed`:
`{"merged": ["fresh"], "dropped": ["left.n"]}`. Две ветки, записавшие одно
имя, — `BranchError` с именами обеих.

`cancel_on_error` (по умолчанию `true`) определяет судьбу соседних веток при
падении одной: `true` — отменяются немедленно (событие `parallel_cancelled`),
`false` — доигрывают до конца. В обоих случаях узел завершается ошибкой первой
упавшей ветки.
