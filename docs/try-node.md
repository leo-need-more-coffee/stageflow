# The try node

Error handling is block-scoped: a `try` node covers a region of the graph, and
an error raised by any node inside it goes to the matching `except`.

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

![A try node, its body and its handler](img/tut-try.png){ width="418" }

## The region is derived, not listed

The block covers every node reachable from `body` but **not** reachable from
`next` — the same rule the [`map` loop](map-node.md) uses for its body. So the
block is defined by the shape of the graph: move an edge and the protected
region changes with it, and there is no list of members to keep in sync.

Nested blocks work as expected: an inner `try` simply lies inside the outer
one's region.

## Choosing a handler

- `error_equals` accepts a bare exception class name (`TimeoutError`), a fully
  qualified path (`stageflow.exceptions.BranchError`), or `*`.
- Handlers are tried in order and the first match wins, so the specific ones
  go above `*`.
- An error no handler matches keeps propagating outwards, to the nearest
  enclosing block or out of the session.
- `result_var` puts the error into the frame as an object with the fields
  `type`, `full_type`, `message` and `node` — the last one names the node that
  failed, which is not the block itself. A failure deeper than one scope is
  named by the node of *this* block's body that contained it: an error from
  inside a nested `try` or a `map` body names that nested node, because that is
  the node this handler's own graph has.

A handler sees the frame as the **last successfully completed node of the
body** left it: the writes made before the failure are there, the failing
node's are not.

## Where a road inside the block leads

A road that simply ends (`"next": null`) hands control back to the block,
which continues at its own `next`. That is true of the body and of a handler
alike — a handler is a part of the block, not an exit from it.

A road may also leave the block outright, by pointing at a node the block does
not own; the block then stops owning that road. (This is the one place where
`try` and `map` differ: a [loop](map-node.md) refuses such a road, because
leaving mid-iteration would abandon the elements still to come.)

A [`terminal`](terminal-node.md) inside the block ends the whole run, and
nothing after the block is executed.

## Order of recovery

The failing node's own [`retry`](errors.md) policies are exhausted first, and
only then does the error propagate to the nearest enclosing `try`. Retrying is
a property of an operation; catching is a property of a region — they compose
rather than compete.
