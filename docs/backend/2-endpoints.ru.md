# 2. Эндпоинты

Их семь, и чтобы войти, редактору нужен ровно один.

```
GET    /api/stages             спеки стадий, доступных этому вызывающему
GET    /api/meta               что этот бэкенд умеет исполнять  (необязателен)
GET    /api/secrets            ИМЕНА секретов из окружения
POST   /api/run                {pipeline, vars, mode, delay, secrets} -> {id, state}
GET    /api/run/<id>           состояние прогона
GET    /api/run/<id>/events    поток событий (SSE), ?from=N
POST   /api/run/<id>/control   {action: "step"|"resume"|"pause"|"stop"|"delay"}
POST   /api/run/<id>/vars      {set: {...}, drop: [...]}
```

Ничего специфичного для StageFlow в них нет, кроме форм; пример использует
FastAPI, потому что так короче, а сам протокол описан в
[руководстве по бэкенду](https://github.com/leo-need-more-coffee/stageflow-ui/blob/main/docs/backend.md)
редактора.

## Реестр, отданный наружу

```python
@router.get("/stages")
async def stages() -> dict:
    from stageflow import get_stages
    return {"stages": {name: cls.get_specs() for name, cls in get_stages().items()}}
```

`get_specs()` — это разобранный докстринг из [шага 1](1-stages.ru.md).
Единственный эндпоинт, без которого редактор не откроется, — и он же тот,
которым проверяется адрес на экране подключения: «подключено» значит, что
штука действительно отвечает и действительно разрешает этот origin, а не что
строка похожа на URL.

## Прогон — это настоящая Session

```python
pipeline = Pipeline.from_dict(payload["pipeline"])
pipeline.validate()          # плохой граф отвечает из запроса, а не из потока

debugger = StepDebugger(mode=mode, delay=delay, on_event=bus.push)
session = Session(
    id=run_id,
    pipeline=pipeline,
    context=Context(vars=start_vars),
    event_handler=lambda event: bus.push(telemetry_to_dict(event)),
    debugger=debugger,
)
```

**Настоящая** `Session`, и в этом весь смысл: отладчик показывает то, что
произойдёт, потому что это и происходит. `StepDebugger` — собственный
отладчик ядра ([Пошаговая отладка](../step-debugging.ru.md)): он открывает
точку останова, фрейм и возможность принять правку в него, а эндпоинты —
тонкая обёртка над его потокобезопасными методами.

Каждому прогону — свой поток со своим event loop, потому что HTTP-обработчики
живут в серверном. Команды заходят через отладчик, события выходят через лог.

## Лог событий, и почему именно лог

```python
@router.get("/run/{run_id}/events")
async def run_events(run: CurrentRun, start: FromEvent = 0) -> StreamingResponse:
    return StreamingResponse(
        sse_lines(run.bus, start),
        media_type="text/event-stream; charset=utf-8",
        headers={"X-Accel-Buffering": "no"},   # буферизующий прокси придержит токены
    )
```

Лог, а не очередь, и каждое событие несёт свой `index`. Подписчик приходит
отдельным HTTP-запросом уже **после** старта, и без истории он потерял бы
начало — а в отладке начало и есть самое интересное. `?from=N` — тот же
механизм, использованный для другого: редактор читает поток через `fetch`, а
не `EventSource` (чтобы можно было послать учётные данные), и при обрыве
возобновляется с события, следующего за последним увиденным.

## Секреты: имена, никогда значения

```python
@router.get("/secrets")
async def secrets() -> dict:
    return {"names": env_secret_names(), "source": "env"}
```

Браузер узнаёт, что `OPENAI_API_KEY` существует. Значение подставляется в
стартовый фрейм на сервере и вычищается из каждого события по дороге на
страницу, чтобы ключ не вернулся через отладочный лог. Подробности — в
[Секретах](https://github.com/leo-need-more-coffee/stageflow-ui/blob/main/docs/secrets.md).

## Сказать, что умеешь

```python
@router.get("/meta")
async def meta() -> dict:
    from stageflow import capabilities
    return {"api": 1, **capabilities()}
```

Необязателен, две строки, и убирает целый класс путаницы. Редактор
зеркалит реестр типов узлов ядра таким, каким он был на момент сборки
редактора, так что редактор новее вашего бэкенда предложил бы узел, который
ваши прогоны отвергнут, — в виде `Unknown node type: 'map'`, на середине, не
называя ни причины, ни лекарства. `capabilities()` отвечает самим реестром,
поэтому отсутствующее в нём имя — ровно то имя, которое прогон и отверг бы, и
редактор его гасит, а не угадывает.

Бэкенд, который `/meta` не отдаёт, не додумывают: ничего не помечается, всё
работает, редактор пишет, что версия неизвестна.

## CORS, дважды

Редактор на другом origin, поэтому каждому ответу нужен
`Access-Control-Allow-Origin`, а preflight'у JSON-POST'а — ответ. Ещё один
случай выглядит со страницы точно так же, но это не то же самое: хостящийся
редактор отдаётся по **https**, а ваш бэкенд отвечает на `127.0.0.1`, что
Chrome называет private-network-запросом и отвергает, если в ответе на
preflight нет `Access-Control-Allow-Private-Network: true`.

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", AUTH_HEADER],
    allow_private_network=True,
)
```

`AUTH_HEADER` — это [шаг 5](5-tenants.ru.md), заглянувший пораньше: заголовок,
который браузеру не разрешили, — это запрос, который со страницы не уйдёт.

---

На этом бэкенд уже работает. Следующие три шага — о том, чтобы им мог
пользоваться кто-то ещё.

Дальше: [что можно собирать](3-policy.ru.md).
