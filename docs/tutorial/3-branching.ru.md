# 3. Выбор дороги

Пока что граф — линия. Теперь ему надо решать: если в базе знаний есть ответ,
писать его; если нет, отдавать тикет человеку.

## Поиск ответа

`SearchKnowledgeStage` ищет статью по теме и словам и сообщает, нашлось ли
что-нибудь:

```json
{"id": "search", "type": "stage", "stage": "SearchKnowledgeStage",
 "arguments": {"vars": {"text": "text", "topic": "topic"}},
 "outputs": {"found": "found", "answer": "answer", "article_id": "article_id"},
 "next": "has_answer"}
```

## Развилка

Узел `condition` — это одно выражение и две дороги:

```json
{"id": "has_answer", "type": "condition", "condition": "vars.found",
 "then": "render", "else": "escalate"}
```

Выражение — это [CEL](../expressions.ru.md). Переменные фрейма живут в
пространстве имён `vars`, так что `vars.found` — та самая переменная, которую
только что записал поиск. Писать можно всё, что позволяет CEL:
`vars.urgency == 'high'`, `size(vars.tags) > 0`,
`vars.plan in ['pro', 'enterprise']`.

![Узел поиска, условие и две дороги из него](../img/tut-condition.png){ width="426" }

У панели узла `condition` одно поле под выражение и два — куда идти:

![Узел condition в инспекторе](../img/ref-expression.png){ width="372" }

## Два финала

У каждой дороги свой терминал, и то, до какого терминала дошли, и есть
результат запуска:

```json
{"id": "answered", "type": "terminal", "result": {"status": "answered"},
 "artifacts": ["reply", "article_id", "topic"]},

{"id": "handed_over", "type": "terminal", "result": {"status": "escalated"},
 "artifacts": ["task_id", "topic"]}
```

Между развилкой и финалами стоят ещё три стадии: `render` подставляет значения
в шаблон статьи, `send` отправляет ответ, `escalate` заводит задачу человеку.

## Что теперь происходит

Два тикета — две дороги:

```python
# ticket_id = "T-1001", двойное списание; в базе знаний есть статья
{'status': 'answered'}    artifacts: reply, article_id, topic

# ticket_id = "T-1005", просьба добавить фичу; ничего не подходит
{'status': 'escalated'}   artifacts: task_id, topic
```

Каждое решение запуск публикует событием, так что гадать, какой дорогой он
пошёл, не приходится:

```
condition_evaluated  {"result": true,  "next": "render"}
condition_evaluated  {"result": false, "next": "escalate"}
```

Дальше: [два дела сразу](4-parallel.ru.md).
