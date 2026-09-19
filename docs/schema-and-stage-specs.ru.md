# Схема и спецификации стадий

JSON Schema пайплайна и спецификации зарегистрированных стадий:

```python
from stageflow.docs import generate_pipeline_schema, generate_stages_json, load_pipeline_schema
from stageflow import get_stages

schema = generate_pipeline_schema(get_stages())   # схема с enum имён стадий
stages = generate_stages_json(get_stages())       # спеки стадий для редактора
```

`load_pipeline_schema()` отдаёт схему без подстановки enum — её же использует
`Pipeline.validate()`. Из этих двух функций и собирается всё, что нужно
внешнему инструменту: редактору, валидатору в CI, генератору документации.
