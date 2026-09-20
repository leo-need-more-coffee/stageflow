<div align="center">

# StageFlow

**Pipelines described in JSON, executed in Python — and debuggable node by node.**

[![tests](https://github.com/leo-need-more-coffee/stageflow/actions/workflows/tests.yml/badge.svg)](https://github.com/leo-need-more-coffee/stageflow/actions/workflows/tests.yml)
[![PyPI](https://img.shields.io/pypi/v/stageflow-framework)](https://pypi.org/project/stageflow-framework/)
[![Python](https://img.shields.io/pypi/pyversions/stageflow-framework)](https://pypi.org/project/stageflow-framework/)
[![docs](https://img.shields.io/badge/docs-stageflow-blue)](https://leo-need-more-coffee.github.io/stageflow/)
[![license](https://img.shields.io/badge/license-MIT-green)](LICENSE)

[Documentation](https://leo-need-more-coffee.github.io/stageflow/) ·
[Tutorial](https://leo-need-more-coffee.github.io/stageflow/tutorial/) ·
[Документация на русском](https://leo-need-more-coffee.github.io/stageflow/ru/)

</div>

A pipeline is a graph of nodes in JSON. The work happens in stages — Python
classes you write. Between them travels one immutable frame of variables, and
everything the graph does with that frame is visible in the JSON: branching,
retries, error handling, concurrency, nested graphs.

Because the pipeline is data, it can be stored, diffed, generated, validated
before it runs — and drawn:

![A pipeline open in the StageFlow editor](docs/img/tut-final.png)

That is the [editor](https://github.com/leo-need-more-coffee/stageflow-ui): a
separate static page that draws and debugs a graph while this core executes it.

## Install

```bash
pip install stageflow-framework
```

Python 3.11+.

## A pipeline in 30 seconds

```python
import asyncio

from stageflow import BaseStage, Pipeline, Session, register_stage


@register_stage("HelloStage")
class HelloStage(BaseStage):
    """
    description: "Greets whoever the pipeline points at"
    arguments:
      name: string
    outputs:
      greeting: string
    """

    async def run(self):
        name = self.get_arguments().get("name", "world")
        self.set_outputs({"greeting": f"Hello, {name}!"})


pipeline = Pipeline.from_dict({
    "nodes": [
        {"id": "start", "type": "entry",
         "variables": {"user_name": "Alice"}, "next": "hello"},
        {"id": "hello", "type": "stage", "stage": "HelloStage",
         "arguments": {"vars": {"name": "user_name"}},
         "outputs": {"greeting": "greeting"}, "next": "finish"},
        {"id": "finish", "type": "terminal",
         "result": {"status": "ok"}, "artifacts": ["greeting"]},
    ],
})
pipeline.validate()

result = asyncio.run(Session(id="demo", pipeline=pipeline).run())
print(result.result)     # {'status': 'ok'}
print(result.artifacts)  # {'greeting': 'Hello, Alice!'}
```

The docstring is the stage's specification. `validate()` checks the graph
against it, so a wrong argument name is an error before anything runs.

## What the graph can do

| | |
|---|---|
| **Eight node types** | `entry`, `stage`, `condition`, `switch`, `parallel`, `try`, `subpipeline`, `terminal` |
| **One immutable frame** | variables travel along the path; a write produces a new frame, so branches never collide |
| **CEL expressions** | in conditions, in switch cases, and in any argument or output through the `.$` suffix |
| **Errors as roads** | `retry` on a node, `try`/`except` over a region of the graph derived from its shape |
| **Real concurrency** | `parallel` branches with their own frames and explicit merge rules |
| **Nested graphs** | a `subpipeline` starts with a fresh frame and returns artifacts |
| **Gradual typing** | declare the variables that matter; checked at validation and on every write |
| **Step debugging** | stop between nodes, read and edit the frame, replay the event stream |

## Documentation

The [tutorial](https://leo-need-more-coffee.github.io/stageflow/tutorial/)
builds one working pipeline step by step, with screenshots from the editor.
The reference covers the rest:

[Quick start](https://leo-need-more-coffee.github.io/stageflow/quick-start/) ·
[Node types](https://leo-need-more-coffee.github.io/stageflow/node-types/) ·
[Data model](https://leo-need-more-coffee.github.io/stageflow/data-model/) ·
[Expressions](https://leo-need-more-coffee.github.io/stageflow/expressions/) ·
[Errors](https://leo-need-more-coffee.github.io/stageflow/errors/) ·
[Parallel branches](https://leo-need-more-coffee.github.io/stageflow/parallel-branches/) ·
[Variable typing](https://leo-need-more-coffee.github.io/stageflow/variable-typing/) ·
[Session control](https://leo-need-more-coffee.github.io/stageflow/session-control/) ·
[Step debugging](https://leo-need-more-coffee.github.io/stageflow/step-debugging/)

Sources are in [`docs/`](docs) (`*.md` English, `*.ru.md` Russian) and publish
themselves on every push to `main`.

## The rest of the project

| Repository | What it is |
|---|---|
| **stageflow** | this one: the core that runs the pipelines |
| [stageflow-ui](https://github.com/leo-need-more-coffee/stageflow-ui) | the editor: a static page that draws and debugs a graph |
| [stageflow-example](https://github.com/leo-need-more-coffee/stageflow-example) | a working backend for the editor: a support bot in four pipelines |

## Development

```bash
pip install -e ".[dev]"
python -m unittest discover -s tests
```

Pipelines can be tested declaratively:

```python
from stageflow.testing import PipelineTestSpec, run_pipeline_test
```

Releases are tag-driven: the tag has to match `project.version` in
`pyproject.toml`, and the workflow publishes to PyPI.

## License

MIT — see [LICENSE](LICENSE).
