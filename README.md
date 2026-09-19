# StageFlow

StageFlow — лёгкий фреймворк для описания и исполнения JSON-пайплайнов: граф узлов,
пользовательские стадии, иммутабельный фрейм данных, CEL-выражения,
retry и блочный try/except, параллельные ветки и вложенные пайплайны.

Дизайн модели памяти и роутинга описан в [MEMORY_MODEL.md](MEMORY_MODEL.md),
список изменений относительно исходной версии — в [REFACTORING.md](REFACTORING.md).

## Установка

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

Требуется Python 3.10+.

## Быстрый старт

1) Регистрируем стадию:

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

2) Описываем пайплайн JSON'ом:

```python
pipeline_dict = {
    # поле "entry" не нужно: начало графа задаёт узел entry, он же объявляет
    # переменные, с которыми пайплайн стартует
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

3) Запускаем:

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

Значения из `entry` — **по умолчанию**: то, что пришло снаружи, их перекрывает,
поэтому тот же пайплайн параметризуется без правки JSON:

```python
from stageflow import Context
session = Session(id="demo", pipeline=pipeline, context=Context({"user_name": "Боб"}))
```

## Модель данных: один иммутабельный фрейм

Вместо плоского контекста — один **фрейм** (`vars`), текущий вдоль пути исполнения
(подробно в MEMORY_MODEL.md §1). Он иммутабелен: каждая запись порождает новый
фрейм, поэтому ветки `parallel` расходятся независимо, а один и тот же объект
безопасно отдать нескольким конкурентным потребителям.

Второго скоупа нет. Session-wide `global` был удалён в 0.6.0 (разбор —
REFACTORING.md §10): всё, что он умел сверх фрейма, сводилось к разделяемому
мутабельному состоянию поперёк ветвей с «кто последний, тот и прав» — то есть к
отказу от той самой гарантии, которую даёт `parallel`, сообщая о конфликте
записи явной ошибкой. Единственная настоящая граница видимости данных —
`subpipeline`: ребёнок стартует со свежим фреймом.

Вслед за вторым скоупом в 0.7.0 ушло и слово `local` (REFACTORING.md §12):
уровень `{"local": {...}}` в `outputs` и `variables` был единственным возможным,
а префикс `local.` в `expose` схема требовала с обеих сторон — ни то, ни другое
ничего не различало. В выражениях namespace остался, но называется честно:
`vars.n`. Он там не для скоупа, а для адресуемости: без него переменная с именем
функции CEL (`size`, `type`, `has`) схлопнулась бы с этой функцией. Имя, которое
не является ASCII-идентификатором, адресуется индексом — `vars['итог']`:
точечный доступ к такому имени CEL не разбирает, а имена по-русски законны.

`arguments` узла делится на бакеты `vars` (ссылки на фрейм) и `const`
(литералы); `outputs` — плоский словарь. Суффикс `.$` на ключе означает
«значение — CEL-выражение». Это единственный канал входов стадии: отдельного
поля «настроек узла» у `stage` нет, литеральная настройка — это `const`-аргумент
(бакеты можно смешивать в одном узле, при совпадении имён ссылка на переменную
перекрывает литерал). Дополнительно у любого узла есть `consume`
(физически убрать имена из фрейма после шага — например, секреты) и `expose`
(копирование/переименование во фрейме без стадии).

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

Ключ в `outputs` — это имя **поля результата стадии**, значение — переменная,
куда его положить (`{"token": "next"}` = взять `output.token`, записать в
`next`). Поэтому поле, которого стадия не возвращает, — ошибка: её ловит
`Pipeline.validate()` по спеке стадии, не дожидаясь `StageOutputError` на
исполнении. Если стадия не объявила выходов вовсе, контракт считается
необъявленным и проверка не применяется.

Чтобы завести переменную, которой у стадии нет, есть ключ с `.$`: его имя —
это уже **переменная**, а значение — выражение. Так один узел заводит сколько
угодно переменных пайплайна, не притворяясь, что стадия их возвращает:

```json
"outputs": {
  "value": "n",
  "attempts.$": "0",
  "greeting.$": "'привет ' + string(vars.user_name)"
}
```

Все выходы узла применяются как одновременное присваивание: значения считаются
против фрейма на входе в узел и только потом пишутся, поэтому порядок ключей
на результат не влияет.

## Точка входа: узел `entry`

Начало графа — это узел, а не строка в шапке JSON. Он же объявляет переменные,
с которыми пайплайн стартует, поэтому описание пайплайна самодостаточно:
`Session(id=..., pipeline=...)` запускается без сборки контекста в коде.

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

- **Значения по умолчанию.** Имя, уже пришедшее во фрейме (посев сессии,
  `inputs` родителя для субпайплайна), узел не перезаписывает и не вычисляет.
  Так один пайплайн запускается и сам по себе, и параметризованно.
- **Связывание по зависимостям.** Выражение может сослаться на соседнюю
  переменную того же узла (`total.$` видит `n`), при этом порядок ключей в JSON
  на результат не влияет; цикл — ошибка `validate()`. Это отличается от
  `outputs` у `stage`, где записи одновременны: там голый ключ — имя поля
  результата стадии, и `vars.x` в соседней строке однозначно значит «x до
  узла»; здесь стадии нет, и ссылаться попросту не на что, кроме переменных
  этого же узла.
- **Одна на граф, и в неё нельзя вернуться.** Оба правила проверяет
  `validate()`; поле `entry` пайплайна при наличии такого узла необязательно, а
  если задано — обязано указывать на него.
- **Типы проверяются статически.** Значение посева известно уже в описании,
  поэтому расхождение с `variables` — ошибка валидации, а не рантайма.

## Типы узлов

| type | Назначение | Ключевые поля |
|---|---|---|
| `entry` | начало графа: переменные, с которыми пайплайн стартует | `variables`, `next` |
| `stage` | исполнение зарегистрированной стадии | `stage`, `arguments`, `outputs`, `consume`, `next` |
| `condition` | бинарное ветвление по CEL | `condition`, `then`, `else` |
| `switch` | n-way ветвление, первый истинный case | `cases: [{when, next}]`, `default` |
| `parallel` | конкурентные ветки с независимыми фреймами | `branches`, `cancel_on_error`, `next` |
| `subpipeline` | вложенный пайплайн, свежий контекст | `subpipeline_id`, `inputs`, `artifact_outputs`, `result_output`, `next` |
| `try` | блочная обработка ошибок области графа | `body`, `except`, `next` |
| `terminal` | конец исполнения | `result`, `artifacts` |

У `parallel` поле `cancel_on_error` (default `true`) определяет судьбу
соседних веток при падении одной: `true` — остальные отменяются немедленно
(эмитится событие `parallel_cancelled`), `false` — доигрывают до конца;
в обоих случаях узел завершается ошибкой первой упавшей ветки.

Наружу из ветки выходят только имена, которых во фрейме не было до `parallel`.
Запись в имя, жившее до входа, — branch-local scratch: ветка пишет `n = 101`,
после узла по-прежнему `n = 1`. Молчать об этом нельзя, поэтому такие имена
перечислены в `parallel_completed` рядом со слитыми:
`{"merged": ["fresh"], "dropped": ["left.n"]}`.

## Ошибки: retry на узле, try/except на области

Повторы — свойство операции, поэтому `retry` остаётся полем узла:

```json
{ "retry": [{ "error_equals": ["TimeoutError"], "max_attempts": 3, "backoff_rate": 2.0 }] }
```

`max_attempts` читается буквально — это все запуски узла вместе с первым:
три попытки — это один запуск и два повтора, то есть ровно три обращения к
внешнему сервису. Счётчик у каждой политики свой: `TimeoutError`, случившийся
дважды, не отнимает повторы у политики для `ValueError`.

Обработка ошибок — блочная, как `try/except` в Python: узел `try` накрывает
область графа, и ошибка любого узла внутри уходит в подходящий `except`.

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

- **Область блока** — узлы, достижимые из `body`, но не достижимые из `next`.
  Состав выводится из графа, руками не перечисляется.
- Сначала исчерпываются `retry` упавшего узла, потом ошибка всплывает к
  ближайшему объемлющему `try`; не подошедшая по типу идёт дальше наружу.
- Вложенные `try` работают сами собой — внутренний просто лежит в области
  внешнего.
- Обработчик видит фрейм таким, каким его оставил последний успешно
  отработавший узел тела (как `except` в Python видит присвоенное до ошибки).
- `result_var` кладёт во фрейм объект ошибки: `type`, `full_type`,
  `message`, `node`.

## Типизация переменных

Переменные фрейма, которыми обмениваются узлы, можно типизировать.
Типизация **постепенная**: необъявленная переменная не проверяется вовсе, поэтому
секции типов необязательны и старые пайплайны работают без изменений.

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

**Язык типов:** примитивы `string`, `int`, `float`, `number` (int|float), `bool`,
`any`, `null`; контейнеры `list<T>` и `map<T>` (строковые ключи); союзы `T|U`;
сокращение `T?` = `T|null`; имена структур и алиасов из секции `types`.
Структуры поддерживают опциональные поля (суффикс `?` у имени), вложенные
анонимные структуры, рекурсию (`Tree`) и строгий режим (`strict` запрещает
лишние поля).

**Проверки двухслойные:**

- *Статически*, при валидации графа: объявленный тип переменной сверяется с
  типами-хинтами из спеки стадии (docstring) — аргумент, читающий переменную,
  и выход, пишущий в неё, должны быть совместимы по роду значения; `expose`
  требует совпадения типов источника и назначения. Несовместимость — ошибка
  `Pipeline.validate()` до запуска.
- *Динамически*, во время исполнения: каждая запись в объявленную переменную
  (`entry`, `outputs`, `expose`, `except.result_var`, артефакты субпайплайна) и входной
  контекст сессии проверяются по полной структуре значения. Ошибка —
  `TypeCheckError` с узлом-виновником и путём до несовпадения:
  `produce: vars.user.settings.theme: ожидался string, получен int`.

Субпайплайны наследуют именованные типы родителя (свою секцию `types` можно
объявить и переопределить); типы их переменных — собственные.

## Управление сессией и ввод

- Команды: `await session.input("command", {"name": "stop" | "pause" | "resume" | "skip"})`
  либо напрямую `session.stop()` / `session.pause()` / `session.resume()`.
- Пользовательский ввод: стадия декларирует `allowed_inputs` и ждёт
  `await self.wait_input("user_input", timeout=...)`; снаружи ввод подаётся через
  `await session.input("user_input", {...})`. Payload проверяется по
  `payload_schema` из декларации.
- Снапшоты: `session.snapshot()` -> dict; `Session.from_snapshot(snap)`
  восстанавливает сессию, `run()` продолжает с сохранённого узла.

## Пошаговая отладка

`Session` принимает отладчик — объект, которому она отдаёт управление перед
каждым узлом и после него. Реализация в ядре — `StepDebugger`
(REFACTORING.md §14):

```python
from stageflow import Pipeline, Session, StepDebugger

debugger = StepDebugger(mode="step", delay=0.0, on_event=print)
session = Session("s1", Pipeline.from_dict(data), debugger=debugger)
task = asyncio.create_task(session.run())   # встанет перед первым узлом

debugger.step()                  # пустить ровно один узел и снова встать
debugger.set_vars({"n": 42})     # применится перед следующим узлом
debugger.set_delay(0.5)          # «смотреть, как идёт», без ручного шага
debugger.resume()                # дальше без остановок
result = await task
```

Что доступно снаружи: точка остановки (`debugger.node`), фрейм в ней
(`debugger.vars`) и поток событий `on_event` — `node_enter` / `node_exit` с
фреймом, `paused`, `var_set` / `var_rejected`. Правка фрейма проверяется
объявленными типами: расхождение отвергается событием, а не падением сессии.

Отладчик видит узлы везде, где они исполняются: тело `try`, ветки `parallel`
и субпайплайн проходят через ту же точку (`Session.execute_node`), а дочерняя
сессия наследует отладчик. Команды потокобезопасны — их можно подавать из
другого потока (в визуальном редакторе они приходят из HTTP-обработчика).

## Встроенные стадии

- vars: `SetValueStage`, `CopyValueStage`, `IncrementStage`, `MergeDictStage`
- lists: `AppendListStage`, `ExtendListStage`, `FilterListStage`, `UniqueListStage`, `PopListStage`
- dicts: `PickKeysStage`, `DropKeysStage`
- strings: `ConcatStage`, `TemplateStage`
- logic: `AssertStage`, `FailStage`, `LogStage`, `SleepStage`

Все они возвращают новые значения и не мутируют входные данные.

### Визуальные подсказки в docstring

Помимо `description`/`arguments`/`outputs`, стадия может объявить,
как её рисовать в визуальном редакторе:

```yaml
description: "Increment numeric value by delta"
icon: "＋"          # глиф, ссылка на SVG, data-URI или <svg>-разметка
icon_mono: false    # перекрасить SVG в цвет узла (монохромные наборы)
color: "#ff8800"    # акцент карточки (по умолчанию — цвет категории)
```

`icon` принимает четыре формы:

| Значение | Что рисуется |
|---|---|
| `"＋"`, `"👋"` | глиф или эмодзи как есть |
| `"/icons/globe.svg"`, `"https://…/x.svg"` | SVG по ссылке |
| `"data:image/svg+xml;utf8,…"` | data-URI |
| `"<svg …>…</svg>"` | разметка прямо в docstring |

`icon_mono: true` рисует SVG маской в цвет узла — так ложатся монохромные
наборы (lucide, feather, tabler), рисующие через `currentColor`; без флага
картинка показывается со своими цветами (логотипы).

Все поля необязательны: без `icon` редактор рисует монограмму из имени стадии
(`IncrementStage` → `IS`), без `color` — берёт детерминированный цвет
категории. Попадают в `get_specs()` и, соответственно, в `stages.json`.

Редактор никогда не вставляет чужой SVG разметкой — только как `<img>` или
CSS-маску, поэтому иконка из стороннего реестра стадий не может исполнить
скрипт; недоступная ссылка деградирует в монограмму.

## Документация и схема

```python
from stageflow.docs.html import generate_docs_assets

html_page, pipeline_schema, stages_json = generate_docs_assets()
```

Визуализация пайплайна (Mermaid + таблица потока данных):

```bash
python -m stageflow.docs.graph pipeline.json -o graph.html
```

## Тесты

```bash
python -m unittest discover -s tests
```

Для декларативного тестирования пайплайнов есть `stageflow.testing`:

```python
from stageflow.testing import PipelineTestSpec, run_pipeline_test
```

## Структура пакета

- `stageflow/core` — ядро: `pipeline`, `session`, `nodes/`, `context`, `stage`, `cel`, `inputs`
- `stageflow/builtins` — встроенные стадии
- `stageflow/docs` — JSON Schema, спеки стадий, HTML-шпаргалка, визуализатор графа
- `stageflow/exceptions.py` — иерархия исключений фреймворка
- `stageflow/testing.py` — хелпер тестирования пайплайнов
- `tests/` — unit-тесты
