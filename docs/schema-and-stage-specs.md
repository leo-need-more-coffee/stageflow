# Schema and stage specifications

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

The editor is one such tool. It holds no stages of its own: it asks a backend
for the specs and draws the palette, the cards and the argument forms from
them.

## What this build can do

An editor is written against one version of the core and then pointed at
whatever backend is running. The question it needs answered is not "which
release is this" but "may I offer a `map` node here", so the core answers that
one directly:

```python
from stageflow import capabilities, __version__

capabilities()
# {"stageflow": "0.12.0",
#  "node_types": ["condition", "entry", "map", "parallel", "stage",
#                 "subpipeline", "switch", "terminal", "try"],
#  "stages": 17}
```

With a [policy](policy.md) the answer narrows to what that caller may use,
which is what a backend should serve rather than a description of itself:

```python
capabilities(basic_plan)
# {"stageflow": "0.12.0",
#  "node_types": ["condition", "entry", "stage", "switch", "terminal"],
#  "stages": 5}
```

`node_types` is the registry itself, not a list written down beside it: a type
registered by a plugin appears here too, and a name missing from it is exactly
a name `Pipeline.from_dict` will reject with `Unknown node type`. That makes
it something a client can branch on, which a version range is not — a build
with a custom node type belongs to no range.

A backend is expected to hand this to its clients. The example backend serves
it at `GET /api/meta`, together with the version of its own HTTP contract:

```json
{ "api": 1, "stageflow": "0.12.0", "node_types": ["condition", "…"], "stages": 17 }
```

`__version__` is read from the installed distribution. In a source checkout
that was never installed it reads `0.0.0+unknown` — deliberately not a
plausible number, so that nobody compares it with one.

![The editor working off a backend's specs](img/tut-final.png)

