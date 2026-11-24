# The wait node

Some pipelines finish in a second. Others stop halfway for a person: a
manager approves an expense, a customer answers a question, a reviewer signs
off — today, or next Thursday. The `wait` node is that stop.

```json
{
  "id": "ask",
  "type": "wait",
  "input": "approval",
  "outputs": { "decision": "decision", "comment": "comment" },
  "timeout": "7d",
  "on_timeout": "remind",
  "next": "decided"
}
```

- `input` — the type of input to wait for, as sent with `session.input(type, payload)`.
- `outputs` — payload fields to variables, exactly as for a
  [stage](stage-node.md), `.$` expressions included.
- `timeout` — seconds, or `"90s"`, `"15m"`, `"2h"`, `"7d"`. Optional; with it,
  `on_timeout` names where to go when nothing came. One without the other is
  a validation error.
- `next` — where to go with the answer.

## Two ways to wait

The same graph runs either way; the host decides by giving the session a
store or not.

**In memory.** Without a store the session waits like any coroutine: `run()`
returns when the input arrives or the timeout passes. Good for minutes —
a chat, a debugging session, a test. Waiting is idle time: it does not count
towards the `seconds` limit or a stage's timeout.

```python
session = Session(id="r1", pipeline=pipeline)
task = asyncio.create_task(session.run())
...
await session.input("approval", {"decision": "approve"})   # from an HTTP handler
result = await task
```

**Saved.** With a store, a run that reaches a `wait` with nothing to read is
saved, and `run()` returns at once:

```python
from stageflow import Session, SQLiteStore

store = SQLiteStore("runs.db")
result = await Session(id="r1", pipeline=pipeline, policy=policy, store=store).run()
result.waiting   # {"status": "waiting", "node": "ask", "input": "approval", "deadline": 1767...}
```

The process is free; the run is a row in the store. When the answer comes —
in an hour, in another process, after a deploy — the host restores the run and
gives it the input:

```python
session = await Session.restore(store, "r1", policy=policy)
await session.input("approval", {"decision": "approve"})
result = await session.run()   # on from `ask` to the end, or to the next wait
```

A pipeline with no `wait` node never touches the store beyond marking the run
as started and finished, so instant pipelines pay nothing for this.

The example backend does all of this over HTTP — an answer endpoint, runs
brought back after a restart, a deadline check — in
[Building a backend, 6. Runs that wait](backend/6-waiting.md).

## What is saved

The frame, the node the run stopped at, the meters spent so far, the inputs
received, the deadlines of the waits — and **the pipeline itself**. A run
continues with the graph it started with: editing a pipeline changes new runs,
not the ones already waiting.

The **policy** is not saved. A checkpoint is data and could be edited, so the
host gives the policy again to `restore`, exactly as to
[`from_snapshot`](session-control.md). A run restored under a narrower policy
than it started with is refused at validation.

## Timeouts

A deadline is an absolute time, fixed when the run first reaches the `wait`
and stored with the run. Nobody is sleeping on it, so somebody has to look:
the host asks the store which runs are past their deadline, and restores each
without an input — the run sees the time is up and takes `on_timeout`.

```python
for run_id in await store.due():
    session = await Session.restore(store, run_id, policy=policy_of(run_id))
    await session.run()
```

Once a minute from cron, a scheduler the application already has, or a loop
in the backend. StageFlow does not ship a scheduler: where and how often to
look is the host's decision, as is where the store lives.

## Two processes, one run

Every save says which version of the run it replaces. `run()` on a restored
session first marks the run as running; if another process got there first,
it raises `CheckpointConflict` **before running anything**. A double click on
"Approve" continues the run once.

A run whose process died mid-run stays marked as running. Restoring it needs
`takeover=True`, which is the host saying "that process is gone" — StageFlow
cannot tell a dead process from a slow one.

## Crashes between waits

With `checkpoint="every_node"` the run is also saved after every node of the
main graph, so a crash loses at most the node that was running. That node
runs again after `restore(takeover=True)` — at least once, not exactly once.
A stage with side effects should pass its `idempotency_key` to whatever it
calls (a payment provider, a mail service): the key is the same for the run
that crashed and the run that repeated it.

```python
class Charge(BaseStage):
    async def run(self):
        await payments.charge(amount, idempotency_key=self.idempotency_key)
```

Saving after every node costs a write per node, so it is for long pipelines
of slow stages, not for a chain of quick ones.

## Where a wait may stand

A saved run comes back at one node of the main graph. So a `wait` cannot be
inside a `try` block, a `map` body, a `parallel` branch or a subpipeline:
validation refuses it, naming the block. Wait before or after the block
instead — a `wait` right after a `parallel` gets everything its branches
produced.

## Stores

| | |
|---|---|
| `MemoryStore()` | a dict: tests, and a single process that wants waits to free it |
| `SQLiteStore(path)` | a file; several processes on one machine; standard library only |

Anything else — Postgres, Redis — is a class with four coroutines: `load`,
`save(checkpoint, expected)`, `due(now)` and `delete`. `save` must refuse
when the stored version is not `expected`; that single check is what keeps
two processes from both continuing a run.

## Events

| Event | When |
|---|---|
| `waiting_started` | the run reached the wait; payload `input`, `deadline` |
| `wait_completed` | the input came |
| `wait_timed_out` | the deadline passed first |
| `session_suspended` | the run was saved instead of waiting |
| `session_restored` | the run was loaded from the store |

Each time a run is restored and walks into its `wait` again, the node counts
as a step once more.
