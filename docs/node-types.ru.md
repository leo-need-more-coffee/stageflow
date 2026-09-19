# Типы узлов

| `type` | Назначение | Ключевые поля |
|---|---|---|
| `entry` | начало графа и переменные старта | `variables`, `next` |
| `stage` | исполнение зарегистрированной стадии | `stage`, `arguments`, `outputs`, `consume`, `next` |
| `condition` | бинарное ветвление по CEL | `condition`, `then`, `else` |
| `switch` | n-way ветвление, первый истинный case | `cases: [{when, next}]`, `default` |
| `parallel` | конкурентные ветки с независимыми фреймами | `branches`, `cancel_on_error`, `next` |
| `try` | блочная обработка ошибок области графа | `body`, `except`, `next` |
| `subpipeline` | вложенный пайплайн со свежим фреймом | `subpipeline_id`, `inputs`, `artifact_outputs`, `result_output`, `next` |
| `terminal` | конец исполнения | `result`, `artifacts` |

Любому узлу дополнительно доступны `retry`, `consume` (убрать имена из фрейма
после шага) и `expose` (копирование или переименование переменной без стадии).
