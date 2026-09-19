# Спецификация стадии

Спецификация стадии — YAML в её docstring: `description`, `arguments`,
`outputs` и визуальные подсказки для редактора.

```yaml
description: "Increment numeric value by delta"
icon: "＋"          # глиф, ссылка на SVG, data-URI или <svg>-разметка
icon_mono: false    # перекрасить SVG в цвет узла (монохромные наборы)
color: "#ff8800"    # акцент карточки (по умолчанию — цвет категории)
```

`icon` принимает четыре формы:

| Значение | Что рисуется |
|---|---|
| `"＋"`, `"👋"` | глиф или эмодзи |
| `"/icons/globe.svg"`, `"https://…/x.svg"` | SVG по ссылке |
| `"data:image/svg+xml;utf8,…"` | data-URI |
| `"<svg …>…</svg>"` | разметка из docstring |

`icon_mono: true` рисует SVG маской в цвет узла — для монохромных наборов
(lucide, feather, tabler), использующих `currentColor`. Без `icon` редактор
рисует монограмму из имени стадии (`IncrementStage` → `IS`), без `color` —
детерминированный цвет категории.

Стадия может объявить `allowed_events` и `allowed_inputs` (`EventSpec` /
`InputSpec` с `payload_schema`), `category` и `timeout`. Всё это попадает в
`get_specs()`.
