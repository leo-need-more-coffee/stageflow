# 2. Эндпоинты

Их семь, и чтобы редактор вообще открылся, нужен ровно один.

```
GET    /api/stages             спецификации стадий, доступных этому вызову
GET    /api/meta               что этот бэкенд умеет исполнять  (необязателен)
GET    /api/secrets            ИМЕНА секретов из окружения
POST   /api/run                {pipeline, vars, mode, delay, secrets} -> {id, state}
GET    /api/run/<id>           состояние прогона
GET    /api/run/<id>/events    поток событий (SSE), ?from=N
POST   /api/run/<id>/control   {action: "step"|"resume"|"pause"|"stop"|"delay"}
POST   /api/run/<id>/vars      {set: {...}, drop: [...]}
```

Ничего специфичного для StageFlow в них нет, кроме формы ответов; пример
написан на FastAPI просто потому, что так короче. Сам протокол подробно
расписан в [руководстве по
бэкенду](https://github.com/leo-need-more-coffee/stageflow-ui/blob/main/docs/backend.md)
на стороне редактора.

## Отдаём реестр

```python
@router.get("/stages")
async def stages() -> dict:
    from stageflow import get_stages
    return {"stages": {name: cls.get_specs() for name, cls in get_stages().items()}}
```

`get_specs()` возвращает разобранный докстринг из [шага 1](1-stages.ru.md).
Это единственный эндпоинт, без которого редактор не откроется, — им же он и
проверяет адрес на экране подключения. Поэтому «подключено» означает, что
сервис действительно отвечает и действительно разрешает этот origin, а не
что введённая строка похожа на URL.

## Прогон — это настоящая Session

```python
pipeline = Pipeline.from_dict(payload["pipeline"])
pipeline.validate()          # про кривой граф отвечаем сразу, а не из фонового потока

debugger = StepDebugger(mode=mode, delay=delay, on_event=bus.push)
session = Session(
    id=run_id,
    pipeline=pipeline,
    context=Context(vars=start_vars),
    event_handler=lambda event: bus.push(telemetry_to_dict(event)),
    debugger=debugger,
)
```

Именно **настоящая** `Session` — в этом весь смысл: отладчик показывает то,
что произойдёт, потому что это ровно то, что происходит. `StepDebugger` —
собственный отладчик ядра ([Пошаговая отладка](../step-debugging.ru.md)): он
даёт точку останова, доступ к фрейму и возможность этот фрейм поправить, а
эндпоинты — тонкая обёртка над его потокобезопасными методами.

Каждому прогону достаётся свой поток со своим циклом событий, потому что
HTTP-обработчики живут в потоке сервера. Команды заходят внутрь через
отладчик, события выходят наружу через лог.

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

Именно лог, а не очередь, и каждое событие несёт свой номер `index`.
Подписчик приходит отдельным HTTP-запросом уже **после** старта прогона, и
без истории он потерял бы начало — а в отладке начало обычно и есть самое
интересное. `?from=N` пригодился и второй раз: редактор читает поток через
`fetch`, а не через `EventSource` (иначе не отправить ключ доступа), и при
обрыве связи продолжает с события, следующего за последним полученным.

## Секреты: только имена, никогда значения

```python
@router.get("/secrets")
async def secrets() -> dict:
    return {"names": env_secret_names(), "source": "env"}
```

Браузер узнаёт лишь то, что `OPENAI_API_KEY` существует. Значение
подставляется в стартовый фрейм на сервере и вычищается из каждого события по
дороге обратно, чтобы ключ не вернулся на страницу через отладочный лог.
Подробнее — в
[Секретах](https://github.com/leo-need-more-coffee/stageflow-ui/blob/main/docs/secrets.md).

## Рассказать, что умеешь

```python
@router.get("/meta")
async def meta() -> dict:
    from stageflow import capabilities
    return {"api": 1, **capabilities()}
```

Необязателен, занимает две строки и снимает целый класс недоразумений.
Редактор носит в себе копию реестра типов узлов — такую, какой она была на
момент его сборки. Значит, редактор новее бэкенда предложит узел, который
прогон отвергнет, причём в виде `Unknown node type: 'map'` где-то на середине
и без единого намёка на причину и лечение. `capabilities()` отвечает самим
реестром, поэтому отсутствующее в ответе имя — ровно то имя, которое прогон и
отклонил бы, и редактор гасит такой узел вместо того, чтобы гадать.

За бэкенд, который `/meta` не отдаёт, редактор ничего не додумывает: ничего
не помечается, всё работает как раньше, а в строке состояния сказано, что
версия неизвестна.

## CORS, и два разных случая

Редактор живёт на другом origin, поэтому каждому ответу нужен
`Access-Control-Allow-Origin`, а предварительному запросу к POST с JSON — свой
ответ. Второй случай со страницы выглядит точно так же, но природа у него
другая: редактор на GitHub Pages отдаётся по **https**, а ваш бэкенд отвечает
на `127.0.0.1`. Chrome считает это обращением в приватную сеть и блокирует,
если в ответе на preflight нет `Access-Control-Allow-Private-Network: true`.

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", AUTH_HEADER],
    allow_private_network=True,
)
```

`AUTH_HEADER` тут — это [шаг 5](5-tenants.ru.md), заглянувший пораньше:
заголовок, который браузеру не разрешили, — это запрос, который со страницы
даже не уйдёт.

---

На этом бэкенд уже работает. Три следующих шага — про то, как пустить на него
кого-то ещё.

Дальше: [что разрешено собирать](3-policy.ru.md).
