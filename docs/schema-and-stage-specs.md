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
