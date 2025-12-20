# StageFlow

StageFlow is a lightweight framework for describing and running JSON pipelines with nodes, stages, and schema validation.

## Features
- Node types: `stage`, `condition`, `parallel`, `map`, `subpipeline`, `terminal`.
- Custom stages (async `run`) with documented arguments/config/outputs.
- Input waiting (`wait_input`), event history, session snapshots.
- Built-in stages for dict/list/string/logic utilities.
- Pipeline JSON Schema (stage enum injected on the fly) and HTML doc generator.

## Installation
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install stageflow-framework
# or for local dev:
# pip install -e .
```
Requires Python 3.10+.

## Quickstart
1) Register your stage:
```python
from stageflow.core.stage import BaseStage, register_stage

@register_stage("HelloStage")
class HelloStage(BaseStage):
    """
    description: "Custom stage example"
    arguments:
      name: string
    outputs:
      greeting: string
    """
    async def run(self):
        name = self.get_arguments().get("name", "world")
        self.set_outputs({"greeting": f"Hello, {name}!"})
```

2) Describe a pipeline in JSON:
```python
pipeline_dict = {
    "entry": "start",
    "nodes": [
        {
            "id": "start",
            "type": "stage",
            "stage": "HelloStage",
            "arguments": {"name": "user_name"},
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

3) Run:
```python
import asyncio
from stageflow.core.pipeline import Pipeline
from stageflow.core.session import Session
from stageflow.core.context import Context

async def main():
    pipe = Pipeline.from_dict(pipeline_dict)
    session = Session(id="demo", pipeline=pipe, context=Context(payload={"user_name": "Alice"}))
    result = await session.run()
    print(result.result)        # {'status': 'ok'}
    print(result.artifacts)     # {'greeting': 'Hello, Alice!'}

asyncio.run(main())
```

## Docs and schema generation
- Programmatic:
```python
from stageflow.docs.html import generate_docs_assets

html_page, pipeline_schema, stages_json = generate_docs_assets()
# html_page — ready-to-serve HTML
# pipeline_schema — dict with JSON Schema (stage enum injected)
# stages_json — raw JSON with stage specs
```
- CLI (prints JSON payload with `html`, `pipeline_schema`, `stages_json` to stdout):
```bash
python scripts/generate_docs_html.py > docs_assets.json
```
If needed, persist the `html` from the JSON payload yourself.

## Tests
```bash
python -m unittest
```

## Node types overview
- `stage`: Executes a registered Stage. Fields: `stage` (name), `config`/`arguments`/`outputs` mappings, `next`, optional `fallback`.
- `condition`: List of `{if, then}` branches (JsonLogic conditions), optional `else`.
- `parallel`: Runs several children; `policy` = `all|any`, `cancel_on_error` flag, optional `next`.
- `map`: Iterates over items path, runs body node for each item (sequential/parallel), collects mapped outputs.
- `subpipeline`: Invokes nested pipeline by id, with `inputs` mapping, `artifact_outputs`, optional `result_output`, `next`.
- `terminal`: Ends execution, returns `result` and selects `artifacts`.

### Node types in detail
- **Stage node**
  - Required: `id`, `type="stage"`, `stage` (registered stage name).
  - Data flow: `arguments` map context paths -> stage inputs; `outputs` map stage return keys -> context paths.
  - Control flow: `next` points to following node; optional `fallback` node is used on stage exceptions.
  - Stage class declares `skipable`, `allowed_events`, `allowed_inputs`, `timeout`, `retries`.
- **Condition node**
  - Required: `id`, `type="condition"`, non-empty `conditions` list.
  - Each condition: `{ "if": <JsonLogic>, "then": "<node id>" }`; first matched branch wins.
  - Optional `else` target if no branch matches.
- **Parallel node**
  - Required: `id`, `type="parallel"`, non-empty `children` (node ids).
  - `policy`: `all` (default) waits all children; `any` completes on first success.
  - `cancel_on_error`: whether to cancel siblings on failure.
  - `next`: node id to continue after parallel finishes.
- **Map node**
  - Required: `id`, `type="map"`, `items` (context path with iterable), `body` (node id).
  - `mode`: `sequential` (default) or `parallel` for body executions.
  - `output_map`: map of body outputs -> target context paths for aggregated results.
  - `item_path`/`index_path`: where to inject current item/index into context for body runs.
  - `cancel_on_error`: abort remaining iterations on error.
  - `next`: node id after all iterations finish.
- **Subpipeline node**
  - Required: `id`, `type="subpipeline"`, `subpipeline_id` (key in `subpipelines` dict of pipeline).
  - `inputs`: context mapping passed into subpipeline root context.
  - `artifact_outputs`: map subpipeline artifacts -> parent context paths.
  - `result_output`: path in parent context to store subpipeline `result`.
  - `next`: next node in parent pipeline.
- **Terminal node**
  - Required: `id`, `type="terminal"`.
  - `result`: arbitrary object returned as final result.
  - `artifacts`: list of context paths that will be returned as artifacts map.

## Built-in stages
- Vars: `SetValueStage`, `CopyValueStage`, `IncrementStage`, `MergeDictStage`.
- Dicts: `PickKeysStage`, `DropKeysStage`.
- Lists: `AppendListStage`, `ExtendListStage`, `FilterListStage`, `UniqueListStage`, `PopListStage`.
- Strings: `ConcatStage`, `TemplateStage`.
- Logic: `AssertStage`, `FailStage`, `LogStage`, `SleepStage`.

Each stage docstring describes arguments/config/outputs; they are also available via `generate_docs_assets()` / `stages_json`.

## Useful refs
- `stageflow/core` — core (pipeline, session, nodes, context).
- `stageflow/builtins` — built-in stages.
- `stageflow/docs` — schema/HTML generation helpers.
- `scripts/` — helpers (docs generator wrapper).
- `tests/` — unit tests.
