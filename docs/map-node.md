# The map node

```json
{
  "id": "per_ticket",
  "type": "map",
  "items": "vars.tickets",
  "body": "classify",
  "item_var": "ticket",
  "index_var": "i",
  "collect": { "reply": "replies" },
  "mode": "sequential",
  "next": "send_all"
}
```

`map` runs one region of the graph once per element of a list. The region is
the body, derived exactly like the body of a [`try` block](errors.md):
everything reachable from `body` but not from `next`. The editor frames it and
shows on the card what goes in and what comes out:

![A map node and the body it frames](img/ref-map-node.png){ width="340" }

## What an iteration sees

Every iteration starts from its own copy of the frame as it was when the loop
began, with the element bound to `item_var` (default `item`) and, if asked
for, the zero-based position bound to `index_var`. Iterations therefore never
see each other's writes — the same rule as the branches of a
[`parallel` node](parallel-branches.md), and the reason the two modes behave
alike.

## What leaves

Only `collect`. A name written inside the body becomes a list outside, one
entry per element, **in the order of the items** — not the order in which the
iterations finished, which matters in `parallel` mode:

```json
"collect": { "reply": "replies" }
```

reads `vars.reply` at the end of every iteration and leaves
`vars.replies == [reply₀, reply₁, …]` behind. A list is shorthand for keeping
the name: `"collect": ["reply"]`. An iteration that never writes the name
fails the node with `StageOutputError` — a hole in the middle of a collected
list is never what was meant. An empty `items` list collects an empty list and
goes straight to `next`.

Declared types are checked on the way out: if `replies` is declared, the
collected list is checked against it before it enters the frame.

The panel is the same JSON in words — the list, the pace, the names the
element and the index arrive under, and the table of what to collect:

![The map node in the inspector](img/ref-map-inspector.png){ width="372" }

## Sequential and parallel

`mode` is `sequential` (default) or `parallel`. `cancel_on_error` (default
`true`) decides what happens to the remaining elements when one fails: `true`
stops at the first failure — in parallel mode cancelling the iterations still
running, with a `map_cancelled` event — and `false` lets every element run
before the node fails with the error of the earliest failed element.

The error is raised **unchanged**, not wrapped: a `try` block around a `map`
matching on `ValueError` catches the `ValueError` the body raised. The index is
in the `map_item_failed` event.

`retry` on the map node itself repeats the whole loop, not the element that
failed — it is a property of the node, as everywhere else.

## The body must be closed

Every road inside the body has to end inside it. A road out of the region is a
validation error:

```
per_ticket: 'classify' leads to 'send_all', outside the loop body;
a road inside the body must end inside it
```

A `try` block tolerates a road out — it simply stops owning it. A loop cannot:
leaving in the middle of the third element would abandon the elements still to
come, and in parallel mode there would be no single frame to continue with. A
`terminal` inside the body is still allowed and ends the whole run, as it does
everywhere.

## Events

| Event | Payload |
|---|---|
| `map_started` | `items`, `mode`, `body` |
| `map_item_started`, `map_item_completed` | `index` |
| `map_item_failed` | `index`, `error`, `type` |
| `map_cancelled` | `items` — the indices cancelled |
| `map_completed` | `items`, `collected` |
