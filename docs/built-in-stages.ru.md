# Встроенные стадии

| Категория | Стадии |
|---|---|
| vars | `SetValueStage`, `CopyValueStage`, `IncrementStage`, `MergeDictStage` |
| lists | `AppendListStage`, `ExtendListStage`, `FilterListStage`, `UniqueListStage`, `PopListStage` |
| dicts | `PickKeysStage`, `DropKeysStage` |
| strings | `ConcatStage`, `TemplateStage` |
| logic | `AssertStage`, `FailStage`, `LogStage`, `SleepStage` |

Все возвращают новые значения и не мутируют входные данные.
