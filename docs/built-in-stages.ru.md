# Встроенные стадии

| Категория | Стадии |
|---|---|
| vars | `SetValueStage`, `CopyValueStage`, `IncrementStage`, `MergeDictStage` |
| lists | `AppendListStage`, `ExtendListStage`, `FilterListStage`, `UniqueListStage`, `PopListStage` |
| dicts | `PickKeysStage`, `DropKeysStage` |
| strings | `ConcatStage`, `TemplateStage` |
| logic | `AssertStage`, `FailStage`, `LogStage`, `SleepStage` |

Все возвращают новые значения и не мутируют входные данные.


В палитре редактора они лежат рядом с вашими стадиями, разложенные по
категориям:

![Встроенные стадии в палитре](img/ref-palette-stages.png){ width="240" }
