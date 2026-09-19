# StageFlow

[![tests](https://github.com/leo-need-more-coffee/stageflow/actions/workflows/tests.yml/badge.svg)](https://github.com/leo-need-more-coffee/stageflow/actions/workflows/tests.yml)

A framework for describing and running JSON-defined pipelines: a graph of
nodes, user-defined stages, an immutable data frame, CEL expressions, retry
and block-scoped `try/except`, parallel branches, nested pipelines.

Requires Python 3.11+ (the `common-expression-language` CEL binding does).

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Quick start

Register a stage:

```python
from stageflow import BaseStage, register_stage

@register_stage("HelloStage")
class HelloStage(BaseStage):
    """
    description: "Custom stage example"
    icon: "👋"
    arguments:
      name: string
    outputs:
      greeting: string
    """
    async def run(self):
        name = self.get_arguments().get("name", "world")
        self.set_outputs({"greeting": f"Hello, {name}!"})
```

Describe a pipeline:

```python
pipeline_dict = {
    "nodes": [
        {
            "id": "start",
            "type": "entry",
            "variables": {"user_name": "Alice"},
            "next": "hello",
        },
        {
            "id": "hello",
            "type": "stage",
            "stage": "HelloStage",
            "arguments": {"vars": {"name": "user_name"}},
            "outputs": {"greeting": "greeting"},
            "next": "finish",
        },
        {
            "id": "finish",
            "type": "terminal",
            "result": {"status": "ok"},
            "artifacts": ["greeting"],
        },
    ],
}
```

Run it:

```python
import asyncio
from stageflow import Pipeline, Session

async def main():
    session = Session(id="demo", pipeline=Pipeline.from_dict(pipeline_dict))
    result = await session.run()
    print(result.result)     # {'status': 'ok'}
    print(result.artifacts)  # {'greeting': 'Hello, Alice!'}

asyncio.run(main())
```

Values declared by the `entry` node are defaults: anything supplied from the
outside overrides them, so the same pipeline can be parameterised without
editing its JSON.

```python
from stageflow import Context
session = Session(id="demo", pipeline=pipeline, context=Context({"user_name": "Bob"}))
```

## Node types

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

## Data model

Pipeline data is a single frame, `vars`, carried along the execution path.
The frame is immutable: every write produces a new one, so `parallel` branches
diverge independently and the same object can safely be handed to several
concurrent consumers. The only visibility boundary is `subpipeline`: a child
starts with a fresh frame and receives data through `inputs`.

A node's `arguments` are split into buckets: `vars` holds references to frame
variables, `const` holds literals. Buckets can be mixed in one node; on a name
collision the variable reference wins over the literal. A `stage` node has no
separate settings field — a literal setting is a `const` argument.

```json
{
  "id": "auth",
  "type": "stage",
  "stage": "AuthStage",
  "arguments": { "vars": { "creds": "creds" }, "const": { "timeout_s": 30 } },
  "outputs":   { "token": "token" },
  "consume": ["creds"],
  "next": "fetch"
}
```

In `outputs` the key is the name of a field in the stage result and the value
is the variable to store it in. A field the stage does not return is rejected
by `Pipeline.validate()` against the stage specification. If a stage declares
no outputs at all, the contract is considered undeclared and the check is
skipped.

A node's outputs are applied as a simultaneous assignment: values are computed
against the frame as it was on entry and only then written, so key order does
not affect the result.

## Expressions

The expression language is [CEL](https://github.com/google/cel-spec). An
expression is allowed in `condition` of a `condition` node, in `when` of a
`switch` node, and in any value whose key carries the `.$` suffix in
`arguments`, `outputs` and `variables`.

```json
"outputs": {
  "value": "n",
  "attempts.$": "0",
  "greeting.$": "'hello ' + string(vars.user_name)"
}
```

Frame variables are addressed through the `vars` namespace (`vars.n`). A name
that is not an ASCII identifier is addressed by index: `vars['итог']`.

A key with the `.$` suffix in `outputs` names a variable rather than a stage
field, so a single node can introduce any number of pipeline variables.

Backend: `common-expression-language` (native), falling back to `cel-python`.

## The `entry` node

The graph starts at a node, not at a field in the JSON header, and that node
declares the pipeline's initial variables.

```json
{
  "id": "start",
  "type": "entry",
  "variables": {
    "n": 5,
    "items": [1, 2, 3],
    "total.$": "vars.n * 2"
  },
  "next": "check"
}
```

- A name already present in the frame (session seed, a parent's `inputs`) is
  neither overwritten nor evaluated.
- Variables are bound in dependency order: an expression may reference a
  sibling variable of the same node, key order in the JSON is irrelevant, and
  a cycle is a `validate()` error.
- There is exactly one `entry` node per graph and jumping back into it is
  forbidden. The pipeline-level `entry` field is optional when such a node
  exists, and must point at it when given.
- Value types are checked statically, during validation.

## Parallel branches

```json
{
  "id": "fan_out",
  "type": "parallel",
  "branches": [{ "id": "hash", "entry": "hash_step" },
               { "id": "thumb", "entry": "thumb_step" }],
  "next": "merge"
}
```

Only names that were not in the frame before `parallel` leave a branch; a
write to a name that existed on entry stays branch-local. Such names are
listed in the `parallel_completed` event:
`{"merged": ["fresh"], "dropped": ["left.n"]}`. Two branches writing the same
name raise `BranchError` naming both.

`cancel_on_error` (default `true`) decides the fate of sibling branches when
one fails: `true` cancels them immediately (the `parallel_cancelled` event),
`false` lets them finish. Either way the node fails with the error of the
first branch that failed.

## Errors: `retry` per node, `try`/`except` per region

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

## Variable typing

Typing is gradual: an undeclared variable is not checked, and the type
sections are optional.

```json
{
  "types": {
    "UserId": "int",
    "User": {
      "id": "UserId",
      "name": "string",
      "email?": "string",
      "settings": { "theme": "string" }
    },
    "Point": { "fields": { "x": "int", "y": "int" }, "strict": true },
    "Tree":  { "value": "int", "children": "list<Tree>" }
  },
  "variables": { "user": "User", "attempts": "int", "tags": "list<string>" }
}
```

The type language: primitives `string`, `int`, `float`, `number` (int|float),
`bool`, `any`, `null`; containers `list<T>` and `map<T>` (string keys); unions
`T|U`; the shorthand `T?` for `T|null`; names from the `types` section.
Structures support optional fields (a `?` suffix on the name), nested
anonymous structures, recursion, and a strict mode (`strict` forbids extra
fields).

Checks come in two layers:

- statically, during graph validation: a variable's declared type is matched
  against the type hints in the stage specification, and `expose` requires the
  source and destination to be compatible; a mismatch is a
  `Pipeline.validate()` error raised before the run starts;
- dynamically, during execution: every write to a declared variable (`entry`,
  `outputs`, `expose`, `except.result_var`, subpipeline artifacts) and the
  session's input context are checked against the full structure of the value;
  a mismatch raises `TypeCheckError` carrying the node and the path to it.

A subpipeline inherits its parent's named types and may declare its own;
variable types are its own.

## Session control

```python
session.stop(); session.pause(); session.resume()
await session.input("command", {"name": "skip"})
```

User input: a stage declares `allowed_inputs` and awaits
`await self.wait_input("user_input", timeout=...)`; input is delivered from
outside with `await session.input("user_input", {...})`. The payload is
validated against the `payload_schema` from the declaration.

Snapshots: `session.snapshot()` returns a dict, `Session.from_snapshot(snap)`
restores the session, and `run()` resumes from the saved node.

## Step debugging

`Session` accepts a debugger that is given control before and after every
node. The in-core implementation is `StepDebugger`.

```python
from stageflow import Pipeline, Session, StepDebugger

debugger = StepDebugger(mode="step", delay=0.0, on_event=print)
session = Session("s1", Pipeline.from_dict(data), debugger=debugger)
task = asyncio.create_task(session.run())   # stops before the first node

debugger.step()                  # let one node run, then stop again
debugger.set_vars({"n": 42})     # applied before the next node
debugger.set_delay(0.5)          # run on its own, pausing between nodes
debugger.resume()                # continue without stopping
result = await task
```

Available from outside: the stop point (`debugger.node`), the frame at that
point (`debugger.vars`), and the `on_event` stream: `node_enter`, `node_exit`,
`paused`, `var_set`, `var_rejected`. Frame edits are checked against declared
types — a mismatch is rejected with an event rather than crashing the session.

The debugger also applies inside a `try` body, inside `parallel` branches and
inside a subpipeline: every node goes through `Session.execute_node`, and a
child session inherits the debugger. Commands are thread-safe.

## Built-in stages

| Category | Stages |
|---|---|
| vars | `SetValueStage`, `CopyValueStage`, `IncrementStage`, `MergeDictStage` |
| lists | `AppendListStage`, `ExtendListStage`, `FilterListStage`, `UniqueListStage`, `PopListStage` |
| dicts | `PickKeysStage`, `DropKeysStage` |
| strings | `ConcatStage`, `TemplateStage` |
| logic | `AssertStage`, `FailStage`, `LogStage`, `SleepStage` |

All of them return new values and never mutate their input.

## Stage specification

A stage is specified by YAML in its docstring: `description`, `arguments`,
`outputs`, plus visual hints for the editor.

```yaml
description: "Increment numeric value by delta"
icon: "＋"          # glyph, SVG link, data URI or inline <svg> markup
icon_mono: false    # recolour the SVG to the node colour (monochrome sets)
color: "#ff8800"    # card accent (defaults to the category colour)
```

`icon` accepts four forms:

| Value | What gets drawn |
|---|---|
| `"＋"`, `"👋"` | the glyph or emoji itself |
| `"/icons/globe.svg"`, `"https://…/x.svg"` | an SVG by link |
| `"data:image/svg+xml;utf8,…"` | a data URI |
| `"<svg …>…</svg>"` | markup straight from the docstring |

`icon_mono: true` draws the SVG as a mask in the node colour, which suits
monochrome sets (lucide, feather, tabler) that paint via `currentColor`.
Without `icon` the editor draws a monogram of the stage name
(`IncrementStage` → `IS`); without `color` it picks a deterministic colour for
the category.

A stage may also declare `allowed_events` and `allowed_inputs` (`EventSpec` /
`InputSpec` with a `payload_schema`), a `category` and a `timeout`. All of it
ends up in `get_specs()`.

## Schema and stage specifications

The pipeline JSON Schema and the specifications of registered stages:

```python
from stageflow.docs import generate_pipeline_schema, generate_stages_json, load_pipeline_schema
from stageflow import get_stages

schema = generate_pipeline_schema(get_stages())   # schema with the stage-name enum
stages = generate_stages_json(get_stages())       # stage specs for an editor
```

`load_pipeline_schema()` returns the schema without the injected enum; it is
the one `Pipeline.validate()` uses. These two functions supply everything an
external tool needs: an editor, a CI validator, a documentation generator.

## Tests

```bash
python -m unittest discover -s tests
```

For declarative pipeline testing there is `stageflow.testing`:

```python
from stageflow.testing import PipelineTestSpec, run_pipeline_test
```

## Package layout

```
stageflow/
  core/          the engine: pipeline, session, nodes/, context, cel, stage, typesys, inputs, debug
  builtins/      built-in stages
  docs/          pipeline JSON Schema and stage specifications
  exceptions.py  exception hierarchy
  testing.py     pipeline testing helper
tests/           unit tests
```
