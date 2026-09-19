# Архитектура StageFlow

> Снимок состояния ДО рефакторинга (v0.1.6). Документ оставлен как точка
> отсчёта: на него ссылается `MEMORY_MODEL.md`, а что и почему изменилось —
> в `REFACTORING.md`. Текущее устройство описано в `README.md`.

Документ описывает текущее устройство фреймворка `stageflow-framework` (v0.1.6) по состоянию кода в репозитории. Цель — дать опору для планирования рефакторинга: что где лежит, как компоненты связаны, какие есть неявные контракты и слабые места.

## 1. Общая идея

StageFlow — движок выполнения пайплайнов, описанных JSON/dict-структурой: граф узлов (`nodes`), между которыми пайплайн перемещается через поля `next`/`then`/`else`/`children`. Каждый узел определённого типа обрабатывается своим обработчиком в `Session`. Переменные между узлами передаются не напрямую, а через общий `Context` — плоское key-path хранилище, куда stage кладёт выходы, а следующий узел их читает.

Ключевая идея разделения ответственности:
- **Pipeline** — статическое описание графа (после валидации по JSON Schema).
- **Node** — единица графа, полиморфный класс по `type`.
- **Stage** — пользовательский юнит бизнес-логики, исполняемый `StageNode`, регистрируется в глобальном реестре.
- **Session** — выполняющийся экземпляр пайплайна: держит `Context`, историю событий, состояние ожидания ввода, интерпретирует граф.
- **Context** — данные текущего запуска (dot-path get/set поверх `dict`).
- **Event** — единица телеметрии выполнения (эмитится в `event_handler`, копится в истории).

## 2. Структура пакета

```
stageflow/
  core/
    context.py    — Context, DotDict
    event.py      — Event, EventSpec, InputSpec
    jsonlogic.py   — мини-интерпретатор JsonLogic-условий
    node.py       — Node и подклассы (Stage/Condition/Parallel/Terminal/SubPipeline)
    pipeline.py   — Pipeline: сборка из dict, валидация по JSON Schema и структурная
    session.py    — Session: движок выполнения графа, ожидание ввода, снапшоты
    stage.py      — BaseStage, реестр стадий (register_stage/get_stage)
    utils.py      — validate_schema / schema_to_jsonable (валидация payload по type-хинтам)
  builtins/       — набор готовых Stage (vars, dicts, lists, lists_extra, strings, logic)
  docs/
    schema.py     — генерация pipeline JSON Schema (+ enum стадий) и stages.json/yaml
    html.py       — рендер HTML "шпаргалки" по узлам и стадиям (без файловой записи)
    schemas/pipeline.json — источник истины по форме pipeline JSON
  testing.py      — PipelineTestSpec / run_pipeline_test — хелпер для тестирования пайплайнов
tests/            — unittest, ~880 строк, покрывает core flow, jsonlogic, snapshot, subpipeline, wait_input и т.д.
```

`stageflow/__init__.py` — публичный API пакета, реэкспортирует основные классы из `core` + `docs` + модуль `builtins`.

## 3. Основные сущности

### 3.1 Pipeline (`core/pipeline.py`)
- `Pipeline.from_dict(data)` — точка входа: если доступен `jsonschema`, валидирует `data` (и рекурсивно каждый `subpipelines[...]`) по схеме `docs/schemas/pipeline.json`, затем строит `Node`-объекты через `Node.from_dict`.
- Хранит `entry`, `nodes` (список) + `_nodes_map` (id → Node) для быстрого доступа, `raw_json` (для снапшотов/пересборки), `subpipelines` (сырые dict-описания вложенных пайплайнов, не превращённые в `Pipeline` заранее).
- `Pipeline.validate()` — вторая, структурная проверка поверх схемной: существование entry, существование целевых узлов у `next/fallback/then/else/children`, наличие класса стадии в реестре, наличие `subpipeline_id` в `subpipelines`. Вызывается автоматически в конструкторе `Session`.
- Валидация двухслойная: JSON Schema (форма) + `Pipeline.validate()` (перекрёстные ссылки, реестр стадий). Если `jsonschema` не установлен — первый слой тихо пропускается (`try/except ImportError`, `validate = None`).

### 3.2 Node (`core/node.py`)
Общий класс `Node(id, type, metadata)` + статическая фабрика `Node.from_dict`, которая по `type` создаёт один из подклассов:

| type | Класс | Ключевые поля |
|---|---|---|
| `stage` | `StageNode` | `stage`, `config`, `arguments`, `outputs`, `next`, `fallback` |
| `condition` | `ConditionNode` | `conditions: list[Condition]` (`if`+JsonLogic → `then`), `else_goto` |
| `parallel` | `ParallelNode` | `children: list[str]`, `policy: all\|any`, `cancel_on_error`, `next` |
| `subpipeline` | `SubPipelineNode` | `subpipeline_id`, `inputs`, `artifact_outputs`, `result_output`, `next` |
| `terminal` | `TerminalNode` | `artifact_paths`, `result` |

Валидация конкретной стадии (`get_stage(stage)`) происходит уже в конструкторе `StageNode` — то есть построение узла может упасть с `ValueError`, если нужный Stage не зарегистрирован **до** парсинга пайплайна (важно для порядка импортов).

### 3.3 Stage (`core/stage.py`)
- Глобальный `STAGE_REGISTRY: dict[str, type[BaseStage]]`, наполняется декоратором `@register_stage("Name")`. Повторная регистрация одного имени — `ValueError`.
- `BaseStage` — базовый класс пользовательской логики:
  - `arguments_paths` / `outputs_paths` — маппинги "имя аргумента/выхода" → "context path"; `get_arguments()` читает из `Context` (с `deepcopy`), `set_outputs()` пишет обратно.
  - `config` — статическая конфигурация узла (не завязана на Context).
  - Классовые атрибуты `skipable`, `allowed_events`, `allowed_inputs`, `timeout` (сек, по умолчанию 30), `retries` (по умолчанию 0) — управляют поведением в `Session._handle_stage`.
  - `emit()` / `wait_input()` / `start_wait_input()` / `finish_wait_input()` — прокси к `Session` с доп. проверкой белого списка (`allowed_events`/`allowed_inputs`) и опциональной валидацией `payload_schema` через `validate_schema` (`core/utils.py`, свой мини-валидатор поверх `typing`-хинтов, не JSON Schema).
  - `get_specs()` — парсит YAML-докстринг стадии (`description/arguments/config/outputs`) для нужд `docs/`.
- Docstring как источник метаданных — жёсткая связка документации с YAML-синтаксисом внутри `"""..."""`; ошибка отступов/синтаксиса YAML в докстринге стадии тихо даёт пустые specs (`yaml.safe_load` без обработки исключений в `get_specs`, кроме общего `if cls.__doc__`).

### 3.4 Session (`core/session.py`)
Главный движок:
- Конструктор сразу вызывает `pipeline.validate()`.
- `run()` — цикл `while node:`, на каждой итерации:
  1. busy-wait на паузу (`asyncio.sleep(0.1)`, пока `_paused`);
  2. проверка `_stopped`;
  3. обработка команды `skip` (только для `StageNode`, если `stage_class.skipable`);
  4. диспетчеризация в `_get_handler(node)` → одна из `_handle_stage/_handle_condition/_handle_parallel/_handle_subpipeline/_handle_terminal`;
  5. хэндлер оборачивается в `asyncio.create_task` и опрашивается **busy-wait поллингом каждые 0.05с** (`while not handler_task.done(): await asyncio.sleep(0.05)`) — это единственный способ прервать выполнение по внешней команде `stop`. Не `await handler_task` напрямую — сделано ради возможности отмены, но ценой постоянного polling'а вместо event-driven ожидания.
- **Ввод/ожидание (`wait_input`)**: `_waiting: dict[type -> list[Future]]` и `_pending_inputs: dict[type -> list[entry]]`. `input()` кладёт значение либо в первый ожидающий `Future`, либо в `_pending_inputs`, если никто не ждёт (входы, пришедшие "заранее", не теряются). `start_wait_input`/`finish_wait_input` разделены на два метода специально для сценария "снапшот между стартом ожидания и его завершением" (см. `_handle_stage`/тесты снапшотов).
- **Команды** приходят через тот же канал `input(type_="command", payload={"name": "stop"|"skip"|"pause"|"resume"})` — то есть команды управления сессией и бизнес-инпуты идут одним и тем же методом с разной семантикой `type_`.
- **Стадии** (`_handle_stage`): создаёт экземпляр `stage_class(...)`, выполняет `stage.run()` под `asyncio.wait_for(timeout=...)`, с ретраями (`range(retries + 1)`); при исчерпании ретраев — либо `fallback`-узел, либо `RuntimeError`. Ошибки таймаута/исключения эмитятся как события, но само сообщение об ошибке теряется после эмита (накапливается в локальном `errors` только для финального `RuntimeError`).
- **Condition** (`_handle_condition`): первый совпавший `if` (через `JsonLogic.evaluate(context)`) побеждает; иначе `else_goto`; если оба отсутствуют — `next_node = None`, и `run()` кинет `RuntimeError("No next node...")`, т.к. `ConditionNode` не является `TerminalNode`.
- **Parallel** (`_handle_parallel`): каждая ветка — независимый подграф, исполняемый `_run_branch_graph` (свой `while node:`, но **без** доступа к `_stopped`/`_paused`/skip-логике верхнего цикла — ветки параллельного узла не отвечают на стоп/паузу/skip сессии). Ветка обязана заканчиваться `TerminalNode` (метод просто `return`); значит внутри `parallel.children` нельзя завершиться через `next=None` у `StageNode` — упадёт `RuntimeError` в `_run_branch_graph`? Нет — там нет проверки `next_node is None`, просто `node = next_node`, и цикл `while node` завершится, если `next_node` — `None`, без ошибки (в отличие от `run()`). То есть поведение "нет следующего узла" отличается между главным циклом и параллельными ветками — несогласованность контракта.
  - `policy="all"`: `asyncio.gather(..., return_exceptions=True)`, при первой ошибке и `cancel_on_error=True` — прерывает сбор ошибок (но не отменяет уже запущенные таски explicitly в этой ветке — отмена не вызывается для policy="all", только заявленная ошибка).
  - `policy="any"`: ручной `asyncio.wait(FIRST_COMPLETED)` цикл, при успехе — отмена остальных; при ошибке и `cancel_on_error` — тоже отмена и `raise`.
- **SubPipeline** (`_handle_subpipeline` → `_run_subpipeline`): дочерний пайплайн собирается заново из `pipeline.subpipelines[id]` (dict) через `Pipeline.from_dict` **на каждый вызов узла** (то есть при повторном входе в тот же subpipeline-узел, например внутри `map`-подобной логики или ретраев, пайплайн парсится и валидируется заново — нет кэширования). Создаётся новый дочерний `Context`, в который копируются (`deepcopy`) выбранные пути из родителя (`node.inputs`), затем новый вложенный `Session` со своим id (`f"{parent_id}:{node.id}"`) и `event_handler`-прокси, который дописывает `subpipeline_node` в payload события и пробрасывает наверх в родительский `emit`. После завершения — артефакты и результат копируются обратно в родительский `Context` по `artifact_outputs`/`result_output`.
- **Snapshot/Resume** (`snapshot()` / `Session.from_snapshot()`): сериализует `context`, `event_history`, `input_history`, флаги `stopped/paused/skip_requested`, `current_node_id` и **весь `pipeline.raw_json`** (т.е. снапшот самодостаточен и не требует внешнего хранения схемы пайплайна). Рестор создаёт новую `Session` и продолжает `run()` с `_current_node_id`. Ожидающие `Future` в `_waiting` снапшотом не сохраняются — это ожидаемо, т.к. `Future` не сериализуем; при resume `wait_input` перезапускается с нуля внутри стадии (стадия должна сама быть идемпотентной/повторно вызвать `wait_input`).

### 3.5 Context (`core/context.py`)
- `DotDict` — `dict` с доступом через атрибуты (`__getattr__`/`__setattr__`), лениво оборачивает вложенные dict в `DotDict` при первом обращении.
- `Context.get/set(path)` — путь через `.`, с опциональным префиксом `payload.` (срезается). Поддерживает индексацию списков по числовым сегментам пути, включая автоудлинение списка при `set` (`while len(cur) <= idx: cur.append(...)`). Смешение форматов путей ("payload.x" и "x" эквивалентны) — не задокументированный, но используемый нюанс (см. `JsonLogic`, где `var` резолвится без сброса префикса `payload`).

### 3.6 JsonLogic (`core/jsonlogic.py`)
- Не полноценная библиотека JsonLogic — самостоятельная мини-реализация, поддерживает только `var`, `<`, `>`, `==`, `and`, `or`. Любой другой оператор — `NotImplementedError`. Название вводит в заблуждение: это подмножество спецификации JsonLogic, без `!`, `!=`, `<=`, `>=`, `in`, `merge`, `if` и т.д.
- `evaluate(context)` строит "плоскую" data-структуру `{"payload": context.payload, **context.payload}` — то есть верхнеуровневые ключи контекста доступны в условиях и как `var: "payload.x"`, и как `var: "x"` одновременно.

### 3.7 Event (`core/event.py`)
- `Event(type, session_id, node_id, stage_id, action_id, payload, timestamp=utcnow)`. `node_id` в коде почти нигде не заполняется (везде используется `stage_id` для узла и явного `node.id`), похоже на недоиспользуемое поле.
- `EventSpec`/`InputSpec` — декларативные описания разрешённых событий/входов стадии, с необязательной `payload_schema` (typing-хинт, не JSON Schema), используются и для рантайм-валидации (`BaseStage.emit/wait_input`), и для генерации документации (`get_specs`).

## 4. Поток данных пайплайна

```
Context (payload)
   │  arguments: {stage_arg: "context.path"}   ──▶ get_arguments() ──▶ Stage.run()
   │
   └── outputs: {stage_out_key: "context.path"} ◀── set_outputs() ◀──┘
```

- Данные между узлами **не** передаются напрямую — только через `Context`, адресуемый строковыми путями. Это даёт слабую связанность узлов, но означает отсутствие статической типизации потока данных: ошибка в path — тихий `None` при `get`, и `ValueError` при попытке `set` в несовместимую структуру.
- `ConditionNode`/`JsonLogic` читают тот же `Context` напрямую (без deepcopy).
- `SubPipelineNode` — единственное место, где происходит явный "мостик" данных между независимыми `Context` (родитель ↔ дочерний), с `deepcopy` на входе.

## 5. Валидация

Два независимых, слабо связанных валидационных слоя:
1. **JSON Schema** (`docs/schemas/pipeline.json`, через `jsonschema`, опционально) — про форму данных на входе (`Pipeline.from_dict`). Схема статична и **не согласована с рантаймом на 100%** — см. раздел 7.
2. **`validate_schema`** (`core/utils.py`) — собственный интерпретатор typing-хинтов (`int`, `list[str]`, `dict[str, int]`, `Optional[...]`, вложенные dict/list-литералы) для проверки payload событий/инпутов стадий. Это третья, отдельная от JSON Schema, схемная система — используется только внутри `BaseStage.emit/wait_input`, но не для `arguments`/`config`/`outputs` самой стадии (те не типизированы рантаймом, только описательно в докстринге для документации).
3. **`Pipeline.validate()`** — структурные перекрёстные проверки графа (см. 3.1).

## 6. Документация (`stageflow/docs`)

- `generate_pipeline_schema()` берёт статичный `pipeline.json` и инжектит `enum` списка зарегистрированных стадий в `$defs.stage_node.properties.stage` — то есть автогенерация "динамической" части схемы происходит только для этого одного поля.
- `generate_stages_yaml/json()` сериализует `BaseStage.get_specs()` каждой зарегистрированной стадии.
- `html.py` рендерит статичную самодостаточную HTML-страницу (инлайн CSS/JS, поиск по стадиям, сворачивание секций) — вызывается программно (`generate_docs_assets()`), скрипт `scripts/generate_docs_html.py` из README в репозитории отсутствует (упомянут в README, но не найден в дереве файлов — либо утерян, либо README устарел).

## 7. Известные несогласованности (важно для рефакторинга)

Это не баги в смысле "падает", а расхождения между слоями/документацией и кодом, которые стоит устранить или явно решить при рефакторинге:

1. **`map`-узел документирован, но не реализован.** README.md подробно описывает `type: "map"` (items/body/mode/output_map/item_path/index_path), но:
   - в `Node.from_dict` (`core/node.py`) такого типа нет — будет `ValueError: Unknown node type: map`;
   - в JSON Schema (`docs/schemas/pipeline.json`) в discriminator.mapping нет `map`;
   - `docs/html.py::build_nodes_section` всё ещё пытается отрендерить `map_node` из `schema["$defs"]` (просто тихо пропускается, т.к. ключа нет).
   - git-история подтверждает: `map` был добавлен (`ecb17a6 map node`) и затем сознательно удалён (`a960f23 rm map node`), но README и обрывок в `html.py` не подчистили. **Нужно решить**: либо вернуть `map` полноценно (Node + Session handler + Schema), либо вычистить все следы из README/`html.py`.
2. **`SubPipelineNode` не экспортируется из публичного API.** Есть в `core/node.py`, используется в `pipeline.py`/`session.py`, но отсутствует и в `core/__init__.py`, и в `stageflow/__init__.py`. Пользователь пакета не может сделать `from stageflow import SubPipelineNode`.
3. **Мёртвый атрибут `StageNode.inputs`.** Класс объявляет `inputs: dict = {}` на уровне класса, но конструктор оперирует `arguments`/`self.arguments` — `inputs` нигде не читается и не пишется, похоже на остаток переименования.
4. **Разное поведение "нет следующего узла" в главном цикле и в параллельных ветках.** `Session.run()` кидает `RuntimeError`, если `next_node is None` и узел не терминальный. `_run_branch_graph` (используется `ParallelNode`) просто завершает ветку молча при `next_node is None`, не требуя `TerminalNode`. Контракт "любая ветка графа обязана заканчиваться terminal-узлом" не унифицирован.
5. **Событийный поллинг вместо ожидания.** И основной цикл `run()` (0.05с), и пауза (0.1с) реализованы через busy-wait `asyncio.sleep`, а не через `asyncio.Event`/отмену задачи. Это работает, но при большом числе одновременных сессий/пайплайнов даёт лишнюю нагрузку и задержку до 50-100мс на реакцию на команды.
6. **Три независимые схемные системы**: JSON Schema (форма пайплайна), typing-хинты через `validate_schema` (payload событий/инпутов), и текстовые type-хинты в YAML-докстрингах стадий (`arguments/config/outputs`, используются только для документации, не для рантайм-валидации). Это создаёт дублирование понятий "тип" и разное поведение при несовпадении.
7. **`JsonLogic` — не совместим со спецификацией JsonLogic**, несмотря на название и создаваемое им ожидание (нет `!=`, `<=`, `>=`, `!`, `in`, вложенных операторов типа `if`/`merge`). Стоит либо переименовать (чтобы не создавать ложных ожиданий совместимости), либо расширить набор операторов, либо заменить на стороннюю библиотеку.
8. **Ошибки стадии теряются после эмита.** `_handle_stage` эмитит `stage_failed`/`stage_timeout` события с текстом ошибки, но финальный `RuntimeError` при исчерпании ретраев склеивает список строк без структуры (не exception chaining/`raise ... from`), что усложняет отладку и машинную обработку ошибок выше по стеку.
9. **`Event.node_id` практически не используется** — везде заполняется `stage_id`, хотя логически `node_id` подошёл бы для condition/parallel/subpipeline узлов (сейчас они тоже пишут в `stage_id`). Стоит определиться с единым полем или чётко развести семантику "узел графа" vs "исполняемая стадия".
10. **`scripts/generate_docs_html.py`**, упомянутый в README, отсутствует в дереве репозитория — либо файл предстоит добавить, либо описание в README нужно убрать/актуализировать.
11. **Повторный парсинг subpipeline на каждый вызов узла.** `_run_subpipeline` каждый раз заново строит `Pipeline.from_dict` (включая JSON Schema валидацию) для одного и того же `subpipeline_id` — при частых заходах в один subpipeline (например, из будущего `map`, если он вернётся) это лишняя работа; кандидат на кэширование на уровне `Pipeline`.

## 8. Тестовое покрытие

`tests/` (unittest, ~880 строк) — хорошее покрытие сценариев: линейный поток (`test_core_flow`), генерация схемы/докстрингов (`test_docs_schema`), полный пайплайн (`test_full_pipeline`), `JsonLogic` (`test_jsonlogic`), typing-валидация payload (`test_payload_validation`), хелпер `run_pipeline_test` (`test_pipeline_tester`), ожидание ввода (`test_session_wait_input`), снапшот/резюм (`test_snapshot`), встроенные стадии (`test_std_stages`), subpipeline (`test_subpipeline`). Явных тестов на `parallel`-политику `cancel_on_error=False` и на команды `stop/pause/skip` в живом `run()` не просмотрено отдельно — стоит проверить при рефакторинге логики `Session.run()`, т.к. это самая "императивная" и наименее тестируемая через unit-тесты часть (много `asyncio.sleep`-поллинга, гонки состояний).

## 9. Заметки для планирования рефакторинга

Судя по структуре, узкие места для рефакторинга концентрируются в `core/session.py` (самый большой и самый "процедурный" файл — там же весь диспетчинг типов узлов через `isinstance`, что при добавлении нового типа узла требует правки в нескольких местах: `Node.from_dict`, `Pipeline.validate`, `Session._get_handler`, JSON Schema, `docs/html.py`). Возможные направления:
- Вынести диспетчинг "тип узла → обработчик" в сам класс `Node` (метод `execute(session)` на подклассах) вместо `isinstance`-цепочек в `Session._get_handler`/`Pipeline.validate` — сейчас логика одного node-типа размазана по 4+ файлам.
- Заменить busy-wait поллинг (`stop`/`pause`) на `asyncio.Event`/`asyncio.wait` с отменяемой задачей.
- Решить судьбу `map`-узла и синхронизировать README/JSON Schema/`docs/html.py` с фактическим набором типов узлов.
- Свести три схемные системы (JSON Schema / typing-валидация payload / докстринг-спеки) к единому источнику истины, либо явно разделить зоны ответственности в документации.
