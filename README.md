# StageFlow

[![tests](https://github.com/leo-need-more-coffee/stageflow/actions/workflows/tests.yml/badge.svg)](https://github.com/leo-need-more-coffee/stageflow/actions/workflows/tests.yml)
[![PyPI](https://img.shields.io/pypi/v/stageflow-framework)](https://pypi.org/project/stageflow-framework/)
[![Python](https://img.shields.io/pypi/pyversions/stageflow-framework)](https://pypi.org/project/stageflow-framework/)

A framework for describing and running JSON-defined pipelines: a graph of
nodes, user-defined stages, an immutable data frame, CEL expressions, retry
and block-scoped `try/except`, parallel branches, nested pipelines.

```bash
pip install stageflow-framework
```

Requires Python 3.11+ (the `common-expression-language` CEL binding does).

## Example

```python
import asyncio
from stageflow import BaseStage, Pipeline, Session, register_stage

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

result = asyncio.run(Session(id="demo", pipeline=pipeline).run())
print(result.result)     # {'status': 'ok'}
print(result.artifacts)  # {'greeting': 'Hello, Alice!'}
```

## Documentation

The full guide lives in the
[wiki](https://github.com/leo-need-more-coffee/stageflow/wiki):

- [Quick start](https://github.com/leo-need-more-coffee/stageflow/wiki/Quick-Start)
  — installing, writing a stage, running a pipeline
- [Node types](https://github.com/leo-need-more-coffee/stageflow/wiki/Node-Types)
  — `entry`, `stage`, `condition`, `switch`, `parallel`, `try`, `subpipeline`, `terminal`
- [Data model](https://github.com/leo-need-more-coffee/stageflow/wiki/Data-Model)
  — the frame, argument buckets, outputs
- [Expressions](https://github.com/leo-need-more-coffee/stageflow/wiki/Expressions)
  — CEL, the `.$` suffix, the `vars` namespace
- [Errors](https://github.com/leo-need-more-coffee/stageflow/wiki/Errors)
  — `retry` per node, `try`/`except` per graph region
- [Variable typing](https://github.com/leo-need-more-coffee/stageflow/wiki/Variable-Typing)
  — gradual typing, static and runtime checks
- [Step debugging](https://github.com/leo-need-more-coffee/stageflow/wiki/Step-Debugging)
  — stopping between nodes, inspecting and editing the frame

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

## License

MIT — see [LICENSE](LICENSE).
