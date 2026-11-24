# 6. Runs that wait

Every run so far lived as long as one HTTP conversation: start it, follow the
stream, done. The seventh pipeline of the example does not. It drafts a reply
and then stops until a support agent has read it — which may be in a minute,
after lunch, or tomorrow.

```json
{"id": "review", "type": "wait", "input": "review",
 "outputs": {"verdict": "verdict",
             "reply.$": "has(output.text) && output.text != '' ? output.text : vars.reply"},
 "timeout": "1d", "on_timeout": "escalate_unreviewed", "next": "decide"}
```

The [`wait` node](../wait-node.md) is the core's part. This page is the
backend's: where the run lives while nobody is looking at it, how the answer
gets to it, and what happens to it when the server restarts.

## A store for runs that wait

A thread blocked for a day on one run is a thread, a slot and a promise that
the process will not restart. Instead, a run of a pipeline with a `wait` gets a
checkpoint store, and when it reaches the wait it is saved and `run()` returns:

```python
durable = any(node.type == "wait" for node in pipeline.nodes)
session = Session(..., store=store if durable else None)
```

```python
result = await session.run()
if result.waiting:          # {"node": "review", "input": "review", "deadline": ...}
    run.status = "waiting"  # the thread ends here
```

Only these pipelines touch the store; the other six run in memory as they
always did. A waiting run is not counted against the run limits from
[chapter 5](5-tenants.md) — it holds nothing.

## The answer

One endpoint:

```
POST /api/run/<id>/input   {"type": "review", "payload": {"verdict": "approve"}}
```

It continues the run from the store, in a new thread:

```python
session = await Session.restore(store, run.id, policy=policy_for(run.plan))
await session.input("review", payload)
result = await session.run()     # to the end, or to the next wait
```

Three refusals, all `409`: the run is not waiting, it waits for another input
type, or it was already answered. The last one matters most — two agents
clicking "Approve" at once must not send the reply twice. Inside one process a
lock settles it; between processes the store does, because `run()` refuses a
run someone else has already continued (`CheckpointConflict`), before running
anything.

The editor follows the run as before. The event stream of a waiting run ends
with a `waiting` event; after the answer the same log goes on, and the client
reads it from where it stopped with `?from=N`.

## What the host keeps

`restore` needs a policy, and the checkpoint deliberately does not contain one:
it is data, and data could widen its own allowance. So the backend keeps,
next to each saved run, who owns it and which plan it runs on — a three-column
table in the same SQLite file. Ownership is the platform's business, as in
[chapter 5](5-tenants.md); the core only stores the run.

## After a restart

On start the server looks through that table and brings back every run whose
checkpoint is still waiting. To the agent nothing happened: the run has the
same id and the same status, and the answer continues it.

The example's system test does exactly this over HTTP: start a run, read the
stream up to `waiting`, kill the server, start a new one on the same database,
send the answer, read the result.

## Deadlines

Nobody is sleeping on a run that waits, so nobody notices its day is up. The
example has a loop in `main.py`:

```python
while True:
    await asyncio.sleep(config.DUE_CHECK_SECONDS)
    for run_id in await store.due():
        continue_without_input(run_id)      # the run takes `on_timeout`
```

In a bigger deployment that is a cron job or whatever scheduler the platform
already runs. The core does not start timers of its own: it stores the
deadline and answers "which ones are due".

## Secrets

The frame of a waiting run is written to disk, so anything in it is too. The
example refuses a pipeline with a `wait` node if the run would carry secrets —
an API key substituted from the environment, a variable the editor marked as
secret. A platform that needs both would store a reference to the secret and
resolve it in the stage, never the value in the frame.

---

That is the whole backend: [stages](1-stages.md), [endpoints](2-endpoints.md),
a [policy](3-policy.md), [meters](4-metering.md), a credential, and runs that
outlive the request that started them. The finished thing is
[stageflow-example](https://github.com/leo-need-more-coffee/stageflow-example).
