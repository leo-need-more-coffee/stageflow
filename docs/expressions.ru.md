# Выражения

Язык выражений — [CEL](https://github.com/google/cel-spec). Выражение
допускается в `condition` узла `condition`, в `when` узла `switch` и в любом
значении с суффиксом `.$` в `arguments`, `outputs` и `variables`.

```json
"outputs": {
  "value": "n",
  "attempts.$": "0",
  "greeting.$": "'привет ' + string(vars.user_name)"
}
```

Переменные фрейма адресуются через namespace `vars` (`vars.n`). Имя, не
являющееся ASCII-идентификатором, адресуется индексом: `vars['итог']`.

Ключ с суффиксом `.$` в `outputs` — это имя переменной, а не поля стадии, так
что один узел заводит произвольное число переменных пайплайна.

Реализация: `common-expression-language` (нативная), с `cel-python` как
запасным вариантом.
