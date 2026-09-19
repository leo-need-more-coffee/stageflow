# StageFlow

A framework for describing and running JSON-defined pipelines: a graph of
nodes, user-defined stages, an immutable data frame, CEL expressions, retry
and block-scoped `try/except`, parallel branches, nested pipelines.

```bash
pip install stageflow-framework
```

Requires Python 3.11+ (the `common-expression-language` CEL binding does).

## Where to start

| Page | What it covers |
|---|---|
| [Quick start](quick-start.md) | installing, writing a stage, running a pipeline |
| [Node types](node-types.md) | the eight node types and their fields |
| [Data model](data-model.md) | the frame, argument buckets, outputs |
| [Expressions](expressions.md) | CEL, the `.$` suffix, the `vars` namespace |

## Reference

| Page | What it covers |
|---|---|
| [The entry node](entry-node.md) | graph start and initial variables |
| [Parallel branches](parallel-branches.md) | concurrency, merge rules, cancellation |
| [Errors](errors.md) | `retry` per node, `try`/`except` per region |
| [Variable typing](variable-typing.md) | gradual typing, static and runtime checks |
| [Session control](session-control.md) | stop/pause/resume, user input, snapshots |
| [Step debugging](step-debugging.md) | stopping between nodes, editing the frame |
| [Built-in stages](built-in-stages.md) | what ships with the package |
| [Stage specification](stage-specification.md) | the YAML docstring contract |
| [Schema and stage specs](schema-and-stage-specs.md) | JSON Schema and editor metadata |

## Development

| Page | What it covers |
|---|---|
| [Releasing](releasing.md) | tag-driven publishing to PyPI |

Source and issues: [github.com/leo-need-more-coffee/stageflow](https://github.com/leo-need-more-coffee/stageflow)
