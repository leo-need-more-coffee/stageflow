# Errors: retry and try/except

Two mechanisms, deliberately separate. Retrying is a property of an
**operation**, so `retry` is a field of a node. Catching is a property of a
**region**, so it is a node of its own — the [`try` block](try-node.md).

## retry

```json
{
  "id": "fetch",
  "type": "stage",
  "stage": "HttpGetStage",
  "retry": [
    { "error_equals": ["TimeoutError"], "max_attempts": 3, "interval_seconds": 1.0,
      "backoff_rate": 2.0, "max_delay_seconds": 30 }
  ],
  "next": "parse"
}
```

| Field | Default | Means |
|---|---|---|
| `error_equals` | `["*"]` | which errors this policy answers for |
| `max_attempts` | `3` | runs **including the first**: `3` is one run and two retries |
| `interval_seconds` | `1.0` | the pause before the first retry |
| `backoff_rate` | `2.0` | each further pause is multiplied by this |
| `max_delay_seconds` | none | a ceiling for the pause |

`retry` is a list, and each policy keeps its own counter: a node can be
patient with `TimeoutError` and give up at once on everything else. The first
policy whose `error_equals` matches is the one that answers.

Every node type accepts `retry`, and it always repeats **the whole node** —
for a [`subpipeline`](subpipeline-node.md) the entire child run, for a
[`map`](map-node.md) the entire loop rather than the element that failed.

## How an error travels

1. The node's own `retry` policies are exhausted.
2. The error propagates to the nearest enclosing [`try`](try-node.md) block,
   and the first handler whose `error_equals` matches takes it.
3. An error nobody matches keeps going outwards, and out of the session — the
   exception surfaces from `session.run()`.

Inside a [`parallel`](parallel-branches.md) node the first failing branch
raises `BranchError`; inside a [`map`](map-node.md) loop the error of the
element is raised unchanged.

## The exceptions of the core

Every error raised by StageFlow inherits `StageFlowError` **and** a builtin
type, so `except ValueError` keeps working on code that never heard of this
package:

| Exception | Also a | Raised when |
|---|---|---|
| `PipelineDefinitionError` | `ValueError` | the JSON does not describe a pipeline |
| `PipelineValidationError` | `ValueError` | `validate()` collected problems |
| `StageContractError` | `ValueError` | a stage broke its own contract |
| `StageOutputError` | `KeyError` | an output field asked for was not returned |
| `ArtifactNotFoundError` | `KeyError` | a subpipeline did not return an artifact |
| `ExpressionError` | `RuntimeError` | a CEL expression failed to evaluate |
| `BranchError` | `RuntimeError` | a parallel branch failed, or two wrote the same name |
| `TypeCheckError` | `TypeError` | a value did not match a declared type |
| `TypeDeclarationError` | `ValueError` | the type declarations themselves are broken |
| `PayloadValidationError` | `ValueError` | an event or input payload failed its schema |
| `RegistryError` | `ValueError` | an unknown stage or node type |
