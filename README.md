# StageFlow

[![tests](https://github.com/leo-need-more-coffee/stageflow/actions/workflows/tests.yml/badge.svg)](https://github.com/leo-need-more-coffee/stageflow/actions/workflows/tests.yml)

Фреймворк описания и исполнения пайплайнов, заданных JSON: граф узлов,
пользовательские стадии, иммутабельный фрейм данных, CEL-выражения, retry и
блочный `try/except`, параллельные ветки, вложенные пайплайны.

Требуется Python 3.11+ (его требует CEL-байндинг `common-expression-language`).

## Установка

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Быстрый старт

Регистрация стадии:

```python
from stageflow import BaseStage, register_stage

@register_stage("HelloStage")
class HelloStage(BaseStage):
    """
    description: "Custom stage example"
    icon: "👋"
    arguments:
      name: string
    outputs:
      greeting: string
    """
    async def run(self):
        name = self.get_arguments().get("name", "world")
        self.set_outputs({"greeting": f"Hello, {name}!"})
```

Описание пайплайна:

```python
pipeline_dict = {
    "nodes": [
        {
            "id": "start",
            "type": "entry",
            "variables": {"user_name": "Alice"},
            "next": "hello",
        },
        {
            "id": "hello",
            "type": "stage",
            "stage": "HelloStage",
            "arguments": {"vars": {"name": "user_name"}},
            "outputs": {"greeting": "greeting"},
            "next": "finish",
        },
        {
            "id": "finish",
            "type": "terminal",
            "result": {"status": "ok"},
            "artifacts": ["greeting"],
        },
    ],
}
```

Запуск:

```python
import asyncio
from stageflow import Pipeline, Session

async def main():
    session = Session(id="demo", pipeline=Pipeline.from_dict(pipeline_dict))
    result = await session.run()
    print(result.result)     # {'status': 'ok'}
    print(result.artifacts)  # {'greeting': 'Hello, Alice!'}

asyncio.run(main())
```

Значения узла `entry` — значения по умолчанию: пришедшее снаружи их
перекрывает, поэтому один пайплайн параметризуется без правки JSON.

```python
from stageflow import Context
session = Session(id="demo", pipeline=pipeline, context=Context({"user_name": "Боб"}))
```

## Типы узлов

| `type` | Назначение | Ключевые поля |
|---|---|---|
| `entry` | начало графа и переменные старта | `variables`, `next` |
| `stage` | исполнение зарегистрированной стадии | `stage`, `arguments`, `outputs`, `consume`, `next` |
| `condition` | бинарное ветвление по CEL | `condition`, `then`, `else` |
| `switch` | n-way ветвление, первый истинный case | `cases: [{when, next}]`, `default` |
| `parallel` | конкурентные ветки с независимыми фреймами | `branches`, `cancel_on_error`, `next` |
| `try` | блочная обработка ошибок области графа | `body`, `except`, `next` |
| `subpipeline` | вложенный пайплайн со свежим фреймом | `subpipeline_id`, `inputs`, `artifact_outputs`, `result_output`, `next` |
| `terminal` | конец исполнения | `result`, `artifacts` |

Любому узлу дополнительно доступны `retry`, `consume` (убрать имена из фрейма
после шага) и `expose` (копирование или переименование переменной без стадии).

## Модель данных

Данные пайплайна — один фрейм `vars`, текущий вдоль пути исполнения.
Фрейм иммутабелен: каждая запись порождает новый, поэтому ветки `parallel`
расходятся независимо и один объект безопасно отдать нескольким конкурентным
потребителям. Единственная граница видимости — `subpipeline`: дочерний
пайплайн стартует со свежим фреймом, получая данные через `inputs`.

`arguments` узла делится на бакеты: `vars` — ссылки на переменные фрейма,
`const` — литералы. Бакеты смешиваются в одном узле; при совпадении имён
ссылка на переменную перекрывает литерал. Отдельного поля настроек у узла
`stage` нет: литеральная настройка — это `const`-аргумент.

```json
{
  "id": "auth",
  "type": "stage",
  "stage": "AuthStage",
  "arguments": { "vars": { "creds": "creds" }, "const": { "timeout_s": 30 } },
  "outputs":   { "token": "token" },
  "consume": ["creds"],
  "next": "fetch"
}
```

В `outputs` ключ — имя поля результата стадии, значение — переменная, куда его
записать. Поле, которого стадия не возвращает, отвергается
`Pipeline.validate()` по спецификации стадии. Если стадия не объявила выходов,
контракт считается необъявленным и проверка не применяется.

Выходы узла применяются как одновременное присваивание: значения вычисляются
против фрейма на входе в узел и только потом записываются, поэтому порядок
ключей на результат не влияет.

## Выражения

Язык выражений — [CEL](https://github.com/google/cel-spec). Выражение
допускается в `condition` узла `condition`, в `when` узла `switch` и в любом
значении с суффиксом `.$` в `arguments`, `outputs` и `variables`.

```json
"outputs": {
  "value": "n",
  "attempts.$": "0",
  "greeting.$": "'привет ' + string(vars.user_name)"
}
```

Переменные фрейма адресуются через namespace `vars` (`vars.n`). Имя, не
являющееся ASCII-идентификатором, адресуется индексом: `vars['итог']`.

Ключ с суффиксом `.$` в `outputs` — это имя переменной, а не поля стадии, так
что один узел заводит произвольное число переменных пайплайна.

Реализация: `common-expression-language` (нативная), с `cel-python` как
запасным вариантом.

## Узел `entry`

Начало графа — узел, а не поле в шапке JSON; он же объявляет переменные старта.

```json
{
  "id": "start",
  "type": "entry",
  "variables": {
    "n": 5,
    "items": [1, 2, 3],
    "total.$": "vars.n * 2"
  },
  "next": "check"
}
```

- Имя, уже пришедшее во фрейме (посев сессии, `inputs` родителя), узел не
  перезаписывает и не вычисляет.
- Переменные связываются в порядке зависимостей: выражение может сослаться на
  соседнюю переменную того же узла, порядок ключей в JSON значения не имеет,
  цикл — ошибка `validate()`.
- Узел `entry` в графе один, и переход в него запрещён; поле `entry` пайплайна
  при его наличии необязательно, а если задано — обязано указывать на него.
- Типы значений проверяются статически, при валидации.

## Параллельные ветки

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

## Ошибки: `retry` на узле, `try`/`except` на области

Повторы — свойство операции, поэтому `retry` является полем узла:

```json
{ "retry": [{ "error_equals": ["TimeoutError"], "max_attempts": 3, "backoff_rate": 2.0 }] }
```

`max_attempts` — все запуски узла вместе с первым: `3` означает один запуск и
два повтора. Счётчик у каждой политики в списке свой.

Обработка ошибок — блочная: узел `try` накрывает область графа, ошибка любого
узла внутри уходит в подходящий `except`.

```json
{
  "id": "safe_fetch",
  "type": "try",
  "body": "fetch",
  "except": [
    { "error_equals": ["TimeoutError"], "next": "on_timeout", "result_var": "error" },
    { "error_equals": ["*"], "next": "on_any" }
  ],
  "next": "after"
}
```

- Область блока — узлы, достижимые из `body`, но не достижимые из `next`;
  состав выводится из графа и не перечисляется вручную.
- Сначала исчерпываются `retry` упавшего узла, затем ошибка всплывает к
  ближайшему объемлющему `try`; не подошедшая по типу идёт дальше наружу.
- Вложенные `try` поддерживаются: внутренний лежит в области внешнего.
- Обработчик видит фрейм таким, каким его оставил последний успешно
  отработавший узел тела.
- `result_var` кладёт во фрейм объект ошибки с полями `type`, `full_type`,
  `message`, `node`.
- `error_equals` принимает короткое имя класса исключения, полный путь или `*`.

## Типизация переменных

Типизация постепенная: необъявленная переменная не проверяется, секции типов
необязательны.

```json
{
  "types": {
    "UserId": "int",
    "User": {
      "id": "UserId",
      "name": "string",
      "email?": "string",
      "settings": { "theme": "string" }
    },
    "Point": { "fields": { "x": "int", "y": "int" }, "strict": true },
    "Tree":  { "value": "int", "children": "list<Tree>" }
  },
  "variables": { "user": "User", "attempts": "int", "tags": "list<string>" }
}
```

Язык типов: примитивы `string`, `int`, `float`, `number` (int|float), `bool`,
`any`, `null`; контейнеры `list<T>` и `map<T>` (ключи строковые); объединения
`T|U`; сокращение `T?` = `T|null`; имена из секции `types`. Структуры
поддерживают опциональные поля (суффикс `?` у имени), вложенные анонимные
структуры, рекурсию и строгий режим (`strict` запрещает лишние поля).

Проверки двухслойные:

- статически, при валидации графа: объявленный тип переменной сверяется с
  хинтами из спецификации стадии, `expose` требует совместимости источника и
  назначения; несовместимость — ошибка `Pipeline.validate()` до запуска;
- динамически, при исполнении: каждая запись в объявленную переменную
  (`entry`, `outputs`, `expose`, `except.result_var`, артефакты субпайплайна) и
  входной контекст сессии проверяются по полной структуре значения; ошибка —
  `TypeCheckError` с узлом и путём до несовпадения.

Субпайплайн наследует именованные типы родителя и может объявить свои; типы
переменных у него собственные.

## Управление сессией

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

## Пошаговая отладка

`Session` принимает отладчик, получающий управление перед каждым узлом и после
него. Реализация в ядре — `StepDebugger`.

```python
from stageflow import Pipeline, Session, StepDebugger

debugger = StepDebugger(mode="step", delay=0.0, on_event=print)
session = Session("s1", Pipeline.from_dict(data), debugger=debugger)
task = asyncio.create_task(session.run())   # остановка перед первым узлом

debugger.step()                  # пустить один узел и снова встать
debugger.set_vars({"n": 42})     # применится перед следующим узлом
debugger.set_delay(0.5)          # идти самостоятельно с паузой между узлами
debugger.resume()                # дальше без остановок
result = await task
```

Снаружи доступны точка остановки (`debugger.node`), фрейм в ней
(`debugger.vars`) и поток событий `on_event`: `node_enter`, `node_exit`,
`paused`, `var_set`, `var_rejected`. Правка фрейма проверяется объявленными
типами — расхождение отвергается событием, а не падением сессии.

Отладчик работает и в теле `try`, и в ветках `parallel`, и в субпайплайне:
все узлы проходят через `Session.execute_node`, дочерняя сессия наследует
отладчик. Команды потокобезопасны.

## Встроенные стадии

| Категория | Стадии |
|---|---|
| vars | `SetValueStage`, `CopyValueStage`, `IncrementStage`, `MergeDictStage` |
| lists | `AppendListStage`, `ExtendListStage`, `FilterListStage`, `UniqueListStage`, `PopListStage` |
| dicts | `PickKeysStage`, `DropKeysStage` |
| strings | `ConcatStage`, `TemplateStage` |
| logic | `AssertStage`, `FailStage`, `LogStage`, `SleepStage` |

Все возвращают новые значения и не мутируют входные данные.

## Спецификация стадии

Спецификация стадии — YAML в её docstring: `description`, `arguments`,
`outputs` и визуальные подсказки для редактора.

```yaml
description: "Increment numeric value by delta"
icon: "＋"          # глиф, ссылка на SVG, data-URI или <svg>-разметка
icon_mono: false    # перекрасить SVG в цвет узла (монохромные наборы)
color: "#ff8800"    # акцент карточки (по умолчанию — цвет категории)
```

`icon` принимает четыре формы:

| Значение | Что рисуется |
|---|---|
| `"＋"`, `"👋"` | глиф или эмодзи |
| `"/icons/globe.svg"`, `"https://…/x.svg"` | SVG по ссылке |
| `"data:image/svg+xml;utf8,…"` | data-URI |
| `"<svg …>…</svg>"` | разметка из docstring |

`icon_mono: true` рисует SVG маской в цвет узла — для монохромных наборов
(lucide, feather, tabler), использующих `currentColor`. Без `icon` редактор
рисует монограмму из имени стадии (`IncrementStage` → `IS`), без `color` —
детерминированный цвет категории.

Стадия может объявить `allowed_events` и `allowed_inputs` (`EventSpec` /
`InputSpec` с `payload_schema`), `category` и `timeout`. Всё это попадает в
`get_specs()`.

## Схема и спецификации стадий

JSON Schema пайплайна и спецификации зарегистрированных стадий:

```python
from stageflow.docs import generate_pipeline_schema, generate_stages_json, load_pipeline_schema
from stageflow import get_stages

schema = generate_pipeline_schema(get_stages())   # схема с enum имён стадий
stages = generate_stages_json(get_stages())       # спеки стадий для редактора
```

`load_pipeline_schema()` отдаёт схему без подстановки enum — её же использует
`Pipeline.validate()`. Из этих двух функций и собирается всё, что нужно
внешнему инструменту: редактору, валидатору в CI, генератору документации.

## Тесты

```bash
python -m unittest discover -s tests
```

Для декларативного тестирования пайплайнов есть `stageflow.testing`:

```python
from stageflow.testing import PipelineTestSpec, run_pipeline_test
```

## Структура пакета

```
stageflow/
  core/          ядро: pipeline, session, nodes/, context, cel, stage, typesys, inputs, debug
  builtins/      встроенные стадии
  docs/          JSON Schema пайплайна и спецификации стадий
  exceptions.py  иерархия исключений
  testing.py     хелпер тестирования пайплайнов
tests/           unit-тесты
```
