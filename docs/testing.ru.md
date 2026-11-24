# Тестирование

Две половины: как тестировать **свои** стадии и пайплайны с помощью
`stageflow.testing` и как StageFlow тестирует сам себя.

## Тест стадии

`run_stage` запускает одну стадию так же, как это делает движок, — внутри
настоящего `Session`, под её таймаутом, с бюджетом, событиями и вводом — и
возвращает то, что она произвела:

```python
from stageflow.testing import run_stage

async def test_greeting():
    run = await run_stage("GreetStage", {"name": "Ann"})
    assert run.outputs == {"text": "Hello, Ann", "length": 10}
    assert run.emitted("greeted") == [{"to": "Ann"}]   # полезная нагрузка её событий
    assert run.charged == {"greetings": 1}            # что она списала через charge()
```

Заодно стадия проверяется на соответствие контракту из её докстринга — во
время работы этого не делает больше никто: аргумент или выход, которого нет в
спецификации, пропущенный обязательный или значение не того типа дают
`ContractViolation` с указанием, что именно не так.

```
ContractViolation: GreetStage: required output 'length' is missing
ContractViolation: GreetStage: output text: expected string, got int
```

Остальное в `run_stage`:

| | |
|---|---|
| `inputs=[{"type": "approval", "payload": {...}}]` | ставятся в очередь до старта стадии; стадия, вызвавшая `wait_input`, получит их сразу |
| `limits=` / `policy=` | настоящий бюджет: `reserve`, `charge()` и потолки работают как в бою, итог — в `run.meters` |
| `check_contract=False` | не проверять контракт |

Стадия, которой бюджет отказал, поднимает `BudgetExceeded`; исключение из
самой стадии проходит наружу без изменений.

## Тест пайплайна

`run_pipeline_test` запускает весь пайплайн, отправляет ввод, которого он
ждёт, и сверяет результат, артефакты и порядок узлов:

```python
from stageflow.testing import PipelineTestSpec, run_pipeline_test

result, events = await run_pipeline_test(PipelineTestSpec(
    pipeline=json.load(open("pipelines/approval.json")),
    payload={"amount": 120},
    inputs=[{"type": "approval", "payload": {"ok": True}}],
    expected_result={"status": "approved"},
    expected_history=["start", "check", "ask", "pay", "done"],
))
```

## Стадии, зарегистрированные тестом

Реестр стадий глобальный, поэтому тест, регистрирующий стадию в своём теле,
падает при втором запуске в том же процессе — под мутационным тестированием,
с плагином `--count` или в REPL. `isolated_stages()` забывает всё, что было
зарегистрировано внутри:

```python
from stageflow.testing import isolated_stages

class MyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(isolated_stages())
```

## Как StageFlow тестирует себя

```bash
pip install -e ".[test]"
python -m unittest discover -s tests          # ~615 тестов, ~20 с
```

| вид | где | что проверяет |
|---|---|---|
| модульные | `tests/test_unit_*.py` | по одному классу, с заглушкой вместо сессии (`tests/_support.py`) |
| интеграционные | `tests/test_integration_*.py` | обход графа, отладчик, ошибки и наблюдаемость вместе |
| системные | `tests/test_system_*.py` | целое приложение (`fixtures/approval/`) и корпус из 26 замысловатых пайплайнов (`fixtures/corpus/`) |
| property-based | `tests/test_property.py` | Hypothesis генерирует графы и документы; инварианты должны выполняться для всех |

### Покрытие

```bash
pip install coverage
coverage run --branch --source=stageflow -m unittest discover -s tests
coverage report
```

**95 %** ветвей. Меньше всего покрыты узел стадии (85 %, в основном ветки
ошибок при связывании аргументов) и проверка схемы полезной нагрузки (87 %).

### Мутационное тестирование

Покрытие говорит, какие строки выполнились, но не говорит, заметил бы тест,
что они неправильные. Это проверяет мутационное тестирование:
[mutmut](https://mutmut.readthedocs.io) вносит в код тысячи мелких изменений —
`<` в `<=`, `and` в `or`, одну константу в другую — и прогоняет тесты на
каждом. Изменение, которого тесты не заметили (*выживший* мутант), — это либо
недостающий тест, либо код, от которого ничего не зависит.

```bash
pip install mutmut pytest
mutmut run              # десятки минут на всех ядрах; настройки в pyproject.toml
mutmut results          # выжившие
mutmut show <имя>       # дифф одного из них
```

Итог по `stageflow/core` и `stageflow/builtins` (6 275 мутантов):

| | мутантов | |
|---|---|---|
| пойманы тестом (упал, завис или рухнул) | 4 548 | 72 % от всех |
| выжили, но изменился только текст сообщения | 1 065 | |
| выжили, поведение изменилось | 601 | |
| этот код не выполняет ни один тест | 61 | |

Сообщения проверяются по смыслу в нескольких местах и по точному тексту — ни в
одном, намеренно. Поэтому из мутантов, меняющих то, что код **делает**, тесты
ловят **87 %**. 601 выживший с изменённым поведением — в основном трёх видов:
эквивалентные мутанты, которые от оригинала не отличить (умолчание `getattr`,
которое никогда не используется, `round(x, 4)` против `round(x, 5)`), поля
событий, которые никто не проверяет (`session_id`, заменённый на `None`), и
настоящие пробелы.

Настоящие пробелы, найденные первым прогоном, закрыты: условие внутри `try`,
чья ветка `else` выходит из блока раньше, заставляло `reachable`
останавливаться слишком рано, и ничто не проверяло, что блок всё ещё
охватывает ветку `then` (`test_try_block`); и никто не проверял, что запрос на
пропуск расходуется ровно одной стадией (`test_session_control`).

Property-тесты в мутационный прогон не входят: их входы случайны, и убьют
они мутанта или нет — менялось бы от запуска к запуску.
