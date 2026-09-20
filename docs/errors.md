# Errors: retry and try/except

Retrying is a property of an operation, so `retry` is a node field:

```json
{ "retry": [{ "error_equals": ["TimeoutError"], "max_attempts": 3, "backoff_rate": 2.0 }] }
```

`max_attempts` counts every run of the node including the first: `3` means one
run and two retries. Each policy in the list keeps its own counter.

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

- The region is every node reachable from `body` but not reachable from
  `next`; it is derived from the graph rather than listed by hand.
- The failing node's `retry` policies are exhausted first, then the error
  propagates to the nearest enclosing `try`; an error no handler matches keeps
  propagating outwards.
- Nested `try` nodes work as expected: the inner one simply lies inside the
  outer one's region.
- A handler sees the frame as the last successfully completed node of the body
  left it.
- `result_var` puts the error object into the frame with the fields `type`,
  `full_type`, `message` and `node`.
- `error_equals` accepts a bare exception class name, a fully qualified path,
  or `*`.
- A road that simply ends (`"next": null`) hands control back to the block,
  which continues at its own `next`. This is the same for the body and for a
  handler: a handler is a part of the block, not an exit from it.
- A `terminal` inside the block ends the whole run, and nothing after the
  block is executed.

![A try node, its body and its handler](img/tut-try.png){ width="418" }

The two regions are framed on the canvas: the body around the nodes whose
errors are caught, the handler around the road out of them.

