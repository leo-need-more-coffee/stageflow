# The terminal node

The end of the run, and the only node that says what the run was for: what it
returns, and which variables leave the frame as artifacts.

```json
{
  "id": "answered",
  "type": "terminal",
  "result": { "status": "answered", "channel": "email" },
  "artifacts": ["reply", "topic", "urgency"]
}
```

![A terminal node on the canvas](img/ref-terminal-card.png){ width="282" }

A session that reaches it returns exactly that:

```python
result = await session.run()
result.result             # {"status": "answered", "channel": "email"}
result.artifacts["reply"] # the value of vars.reply at that moment
```

- `result` is handed back as written — a plain dict of your own shape, not
  interpreted by the core. It is what a caller switches on, so it is worth
  making it say *which* end this was, not just "done".
- `artifacts` names variables to read out of the frame. A name that is not in
  the frame yields `null` rather than an error — a terminal is not the place
  to discover a missing value, and is easy to misread as "the stage returned
  nothing". Write the names the graph really produces.
- A terminal has no `next`: it is the end of the road, and the graph may have
  as many of them as it has outcomes.
- `expose` is applied before the result is taken, so a variable renamed there
  can be exported in the same node.

## Ending from inside a block

A terminal ends **the whole run**, wherever it stands — inside the body of a
[`try`](errors.md) block, a branch of a [`parallel`](parallel-branches.md)
node, the body of a [`map`](map-node.md) loop. The enclosing node does not get
to continue: the block sees that the session has finished and stops with it,
so a `map` halfway through its third element stops there.

That is what makes an early exit expressible — a guard that finds a closed
ticket and ends the run on the spot:

```json
{ "id": "closed", "type": "condition", "condition": "vars.status == 'closed'",
  "then": "nothing_to_do", "else": "work" },
{ "id": "nothing_to_do", "type": "terminal", "result": { "status": "skipped" } }
```

## Running off the end

Reaching no terminal at all is legal but silent: a `condition` without `else`,
a `switch` with no matching case, a stage whose `next` is absent. The session
ends with `result = null` and no artifacts. If a road is meant to end, give it
a terminal — the result is then the difference between "finished this way" and
"fell off the graph".
