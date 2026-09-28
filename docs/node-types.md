# Node types

| `type` | Purpose | Key fields |
|---|---|---|
| [`entry`](entry-node.md) | graph start and the variables it begins with | `variables`, `next` |
| [`stage`](stage-node.md) | run a registered stage | `stage`, `arguments`, `outputs`, `consume`, `next` |
| [`condition`](condition-node.md) | binary branch on a CEL expression | `condition`, `then`, `else` |
| [`switch`](switch-node.md) | n-way branch, first matching case wins | `cases: [{when, next}]`, `default` |
| [`parallel`](parallel-branches.md) | concurrent branches with independent frames | `branches`, `cancel_on_error`, `next` |
| [`try`](try-node.md) | block-scoped error handling over a graph region | `body`, `except`, `next` |
| [`map`](map-node.md) | run a graph region once per element of a list | `items`, `body`, `item_var`, `collect`, `mode`, `next` |
| [`subpipeline`](subpipeline-node.md) | nested pipeline with a fresh frame | `subpipeline_id`, `inputs`, `artifact_outputs`, `result_output`, `next` |
| [`terminal`](terminal-node.md) | end of execution | `result`, `artifacts` |

Every node additionally accepts `retry`, `consume` (drop names from the frame
after the step) and `expose` (copy or rename a variable without a stage).

In the [editor](tutorial/7-debugger.md) the node types are the top of the
palette; the stages the backend knows about come below them.

![The node types in the palette](img/ref-palette-nodes.png){ width="240" }

