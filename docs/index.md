# StageFlow

A framework for describing and running JSON-defined pipelines: a graph of
nodes, user-defined stages, an immutable data frame, CEL expressions, retry
and block-scoped `try/except`, parallel branches, loops over a list,
nested pipelines, and per-tenant limits on all of it.

```bash
pip install stageflow-framework
```

Requires Python 3.11+ (the `common-expression-language` CEL binding does).

![A pipeline in the StageFlow editor](img/tut-final.png)

The picture is the [editor](https://github.com/leo-need-more-coffee/stageflow-ui):
a separate web page that draws and debugs a graph, while the core executes it.

## Where to start

New here? The [tutorial](tutorial/index.md) builds a working pipeline step by
step, with screenshots from the editor. Once the pipelines are somebody
else's, [Building a backend](backend/index.md) is the other half: your stages
behind an HTTP contract, with a policy deciding what may be composed on them
and how much it may spend.

| Page | What it covers |
|---|---|
| [Quick start](quick-start.md) | installing, writing a stage, running a pipeline |
| [Node types](node-types.md) | the nine node types and their fields |
| [Data model](data-model.md) | the frame, argument buckets, outputs |
| [Expressions](expressions.md) | CEL, the `.$` suffix, the `vars` namespace |

## Building a backend

The editor executes nothing: your stages live on a backend of your own. Five
steps, taking the [example backend](https://github.com/leo-need-more-coffee/stageflow-example)
apart in the order it was written.

| Page | What it covers |
|---|---|
| [What a backend is](backend/index.md) | the division of labour, and what the track builds |
| [1. Your stages](backend/1-stages.md) | `register_stage`, specs, arguments, events, streaming |
| [2. The endpoints](backend/2-endpoints.md) | the seven endpoints, a real `Session`, SSE, CORS |
| [3. What may be composed](backend/3-policy.md) | a `Policy` per caller, and telling the editor |
| [4. What a run costs](backend/4-metering.md) | `reserve:`, `charge()`, meters of your own |
| [5. Who is calling](backend/5-tenants.md) | plans, a credential, and where the check goes |

## Reference

| Page | What it covers |
|---|---|
| [The entry node](entry-node.md) | graph start and initial variables |
| [The stage node](stage-node.md) | arguments, outputs, `consume` |
| [The condition node](condition-node.md) | a fork on a CEL expression |
| [The switch node](switch-node.md) | many roads, first matching case |
| [Parallel branches](parallel-branches.md) | concurrency, merge rules, cancellation |
| [The try node](try-node.md) | a region of the graph under `except` |
| [The map node](map-node.md) | a region of the graph run once per element |
| [The subpipeline node](subpipeline-node.md) | a nested graph and its boundary |
| [The terminal node](terminal-node.md) | the end of a run, result and artifacts |
| [Errors](errors.md) | `retry`, how an error travels, the exceptions |
| [Policy](policy.md) | restricting the stages and node types a pipeline may use |
| [Limits](limits.md) | meters, budgets, what a stage spends |
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
