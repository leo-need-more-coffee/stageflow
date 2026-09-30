# 2. The endpoints

Seven, and the editor needs exactly one of them to get in.

```
GET    /api/stages             the specs of the stages this caller may use
GET    /api/meta               what this backend can run  (optional)
GET    /api/secrets            the NAMES of the secrets in the environment
POST   /api/run                {pipeline, vars, mode, delay, secrets} -> {id, state}
GET    /api/run/<id>           the state of the run
GET    /api/run/<id>/events    the event stream (SSE), ?from=N
POST   /api/run/<id>/control   {action: "step"|"resume"|"pause"|"stop"|"delay"}
POST   /api/run/<id>/vars      {set: {...}, drop: [...]}
```

Nothing about them is StageFlow-specific except the shapes; the example uses
FastAPI because it is short, and the protocol is written up in the editor's
own [backend guide](https://github.com/leo-need-more-coffee/stageflow-ui/blob/main/docs/backend.md).

## The registry, served

```python
@router.get("/stages")
async def stages() -> dict:
    from stageflow import get_stages
    return {"stages": {name: cls.get_specs() for name, cls in get_stages().items()}}
```

`get_specs()` is the parsed docstring from [step 1](1-stages.md), and it needs
no argument here on purpose: the prose comes back in **every** language the
build has, as a `{locale: text}` mapping, and the editor picks one for its
reader. There is nothing to negotiate at this endpoint — the specs are fetched
once and the language is chosen afterwards, so choosing here would only mean
being asked again ([Localization](../localization.md)).

This is the
one endpoint the editor cannot open without — it is also what the connection
screen probes, so "connected" means the thing really answered and really
allows this origin, not that the address looked like a URL.

## A run is a real Session

```python
pipeline = Pipeline.from_dict(payload["pipeline"])
pipeline.validate()          # answer a bad graph from the request, not from a thread

debugger = StepDebugger(mode=mode, delay=delay, on_event=bus.push)
session = Session(
    id=run_id,
    pipeline=pipeline,
    context=Context(vars=start_vars),
    event_handler=lambda event: bus.push(telemetry_to_dict(event)),
    debugger=debugger,
)
```

A **real** `Session`, which is the whole point: the debugger shows what will
happen because it is what happens. `StepDebugger` is the core's own
([Step debugging](../step-debugging.md)) — it exposes the stop point, the
frame and the ability to accept an edit to it, and the endpoints are a thin
wrapper over its thread-safe methods.

Each run gets a thread with an event loop of its own, because the HTTP
handlers live in the server's. Commands go in through the debugger; events
come back out through a log.

## The event log, and why it is a log

```python
@router.get("/run/{run_id}/events")
async def run_events(run: CurrentRun, start: FromEvent = 0) -> StreamingResponse:
    return StreamingResponse(
        sse_lines(run.bus, start),
        media_type="text/event-stream; charset=utf-8",
        headers={"X-Accel-Buffering": "no"},   # a buffering proxy holds the tokens back
    )
```

A log rather than a queue, and every event carries its own `index`. A
subscriber arrives over a separate HTTP request *after* the run started, and
without history it would miss the beginning — which in debugging is the
interesting part. `?from=N` is the same mechanism used for something else: the
editor reads the stream with `fetch` rather than `EventSource` (so it can send
a credential), and resumes at the event after the last one it saw when a
connection drops.

## Secrets: names, never values

```python
@router.get("/secrets")
async def secrets() -> dict:
    return {"names": env_secret_names(), "source": "env"}
```

The browser learns that `OPENAI_API_KEY` exists. The value is substituted into
the starting frame on the server and scrubbed back out of every event on the
way to the page, so a key does not return through the debug log. The full
story is on [Secrets](https://github.com/leo-need-more-coffee/stageflow-ui/blob/main/docs/secrets.md).

## Saying what you can run

```python
@router.get("/meta")
async def meta() -> dict:
    from stageflow import capabilities
    return {"api": 1, **capabilities()}
```

Optional, and two lines, and it removes a whole class of confusion. The editor
mirrors the core's node registry as it stood when the editor was built, so an
editor newer than your backend would offer a node your runs reject — as
`Unknown node type: 'map'`, halfway through, naming neither cause nor cure.
`capabilities()` answers with the registry itself, so a name absent from it is
exactly a name a run would refuse, and the editor greys it out instead of
guessing.

A backend that does not serve `/meta` is not second-guessed: nothing is
marked, everything works, and the editor says the version is unknown.

## CORS, twice

The editor is on another origin, so every answer needs
`Access-Control-Allow-Origin` and the preflight of a JSON POST needs
answering. One more case looks identical from the page and is not: the hosted
editor is served over **https** and your backend answers on `127.0.0.1`, which
Chrome calls a private-network request and refuses unless the preflight is
answered with `Access-Control-Allow-Private-Network: true`.

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", AUTH_HEADER],
    allow_private_network=True,
)
```

`AUTH_HEADER` is [step 5](5-tenants.md) arriving early: a header the browser
has not been told to allow is a request that never leaves the page.

---

At this point you have a working backend. The next three steps are about
letting somebody else use it.

Next: [what may be composed](3-policy.md).
