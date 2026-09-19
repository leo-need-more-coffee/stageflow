# Node types

| `type` | Purpose | Key fields |
|---|---|---|
| `entry` | graph start and the variables it begins with | `variables`, `next` |
| `stage` | run a registered stage | `stage`, `arguments`, `outputs`, `consume`, `next` |
| `condition` | binary branch on a CEL expression | `condition`, `then`, `else` |
| `switch` | n-way branch, first matching case wins | `cases: [{when, next}]`, `default` |
| `parallel` | concurrent branches with independent frames | `branches`, `cancel_on_error`, `next` |
| `try` | block-scoped error handling over a graph region | `body`, `except`, `next` |
| `subpipeline` | nested pipeline with a fresh frame | `subpipeline_id`, `inputs`, `artifact_outputs`, `result_output`, `next` |
| `terminal` | end of execution | `result`, `artifacts` |

Every node additionally accepts `retry`, `consume` (drop names from the frame
after the step) and `expose` (copy or rename a variable without a stage).
