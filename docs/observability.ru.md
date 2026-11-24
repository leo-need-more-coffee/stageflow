# Наблюдаемость

Сессия принимает необязательный **инструмент**, который видит каждый прогон,
каждый узел и каждое событие. Без него ничего не измеряется; с ним появляются
трейсы, метрики или логи — без единой правки в стадиях.

```python
from stageflow import Instruments, Session
from stageflow.observability import LoggingInstrument, Recorder
from stageflow.observability.otel import OpenTelemetryInstrument

session = Session(
    id="t-1001", pipeline=pipeline, policy=policy,
    instrument=Instruments(OpenTelemetryInstrument(), LoggingInstrument()),
)
```

Дочерний прогон подпайплайна использует инструмент родителя, передавать его
вниз не нужно.

## OpenTelemetry

```bash
pip install "stageflow-framework[otel]"
```

`OpenTelemetryInstrument()` берёт глобальные tracer и meter providers, поэтому
данные уходят туда же, куда хост уже шлёт телеметрию: OTLP-коллектор, Jaeger,
Tempo, Prometheus. Другие можно передать через `tracer_provider=` /
`meter_provider=`.

**Трейсы.** Один спан на прогон, `stageflow.session`, и по одному на узел с
именем `<тип> <id>` (`stage classify`, `map each`). Спаны вложены так же, как
граф: узлы ветки `parallel` или элемента `map` — дети этого узла, тело `try`
внутри `try`, а прогон подпайплайна — ребёнок своего узла `subpipeline`. Весь
прогон — один трейс.

| Где | Атрибуты и события |
|---|---|
| спан сессии | `stageflow.session.id`, `stageflow.session.nested`, `stageflow.outcome` (finished, stopped, budget_exceeded, failed, …), `stageflow.meter.<имя>` для каждого счётчика |
| спан узла | `stageflow.node.id`, `stageflow.node.type`, `stageflow.stage`; статус ERROR и записанное исключение, если узел упал; `stageflow.cancelled`, если его отменил упавший сосед |
| события спана | каждое событие сессии внутри узла: `stage_started`, `node_retry`, `try_caught`, `stage_charged`, `map_item_failed`, … с payload в атрибутах `stageflow.*` |

**Метрики.**

| Имя | Вид | Атрибуты |
|---|---|---|
| `stageflow.sessions` | counter | `outcome`, `nested` |
| `stageflow.session.duration` | histogram, секунды | `outcome`, `nested` |
| `stageflow.nodes` | counter | `type`, `stage`, `outcome` (`ok`, `cancelled` или класс исключения) |
| `stageflow.node.duration` | histogram, секунды | `type`, `stage`, `outcome` |
| `stageflow.usage` | counter | `meter` — что потратили прогоны верхнего уровня: `steps`, `tokens`, ваши счётчики |

`stageflow.usage` — это счёт: те же счётчики, что в `SessionResult.meters`,
сложенные по всем прогонам.

!!! note "Время блока включает его содержимое"
    Длительность узла `parallel`, `try`, `map` или `subpipeline` — это время
    всего, что внутри. Время только стадий — `stageflow.node.duration` с
    фильтром `type="stage"`.

## Логи

`LoggingInstrument(logger="stageflow")` пишет через стандартный `logging`:

| Уровень | Что |
|---|---|
| INFO | прогон начался; прогон закончился — исход, длительность, результат, счётчики |
| ERROR | прогон закончился исключением |
| DEBUG | каждый узел с исходом и длительностью; каждое событие |

У каждой записи есть атрибут `stageflow` — словарь с `session` и, где
применимо, `node`, `type`, `stage`, `outcome`, `seconds`, `meters`, — чтобы
JSON-форматтер вывел их полями.

## В памяти

`Recorder()` копит счётчики и тайминги в процессе: по виду узла (`map`,
`parallel`, `stage:LoadTicketStage`), по прогонам, потраченные счётчики и
число событий каждого типа.

```python
recorder = Recorder()
await Session(id="s", pipeline=pipeline, instrument=recorder).run()
print(recorder.table())
```

`recorder.summary()` — то же самое словарём. Пригодится в тестах, бенчмарках и
чтобы быстро посмотреть, на что уходит время.

## Свой инструмент

Унаследуйтесь от `Instrument` и переопределите нужное. Каждый крючок
вызывается в той задаче, которая делает работу, поэтому контекстные
переменные — спан, id запроса — вкладываются так же, как граф.

```python
from contextlib import contextmanager
from stageflow import Instrument

class SlowNodes(Instrument):
    @contextmanager
    def node(self, session, node):
        started = time.perf_counter()
        yield
        if time.perf_counter() - started > 1:
            print(f"{node.id} дольше секунды")
```

`session()` оборачивает весь прогон, `node()` — каждый узел, `event()` видит
каждое событие. `Instruments(a, b, ...)` объединяет несколько: входят по
порядку, выходят в обратном.
