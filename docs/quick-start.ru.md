# Быстрый старт

## Установка

```bash
pip install stageflow-framework
```

Из клона репозитория, для разработки:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

Требуется Python 3.11+ (его требует CEL-байндинг `common-expression-language`).

## Стадия

Регистрация стадии:

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

## Пайплайн


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

## В редакторе

Тот же пайплайн, открытый в [редакторе](tutorial/7-debugger.ru.md) на бэкенде,
где зарегистрирована `HelloStage`:

![Пайплайн из быстрого старта](img/ref-quick-start.png){ width="294" }

## Запуск


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

Значения узла `entry` — значения по умолчанию: пришедшее снаружи их
перекрывает, поэтому один пайплайн параметризуется без правки JSON.

```python
from stageflow import Context
session = Session(id="demo", pipeline=pipeline, context=Context({"user_name": "Боб"}))
```
