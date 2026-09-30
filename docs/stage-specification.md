# Stage specification

A stage is specified by YAML in its docstring: `description`, `arguments`,
`outputs`, plus visual hints for the editor.

```yaml
description: "Increment numeric value by delta"
icon: "＋"          # glyph, SVG link, data URI or inline <svg> markup
icon_mono: false    # recolour the SVG to the node colour (monochrome sets)
color: "#ff8800"    # card accent (defaults to the category colour)
```

`icon` accepts four forms:

| Value | What gets drawn |
|---|---|
| `"＋"`, `"👋"` | the glyph or emoji itself |
| `"/icons/globe.svg"`, `"https://…/x.svg"` | an SVG by link |
| `"data:image/svg+xml;utf8,…"` | a data URI |
| `"<svg …>…</svg>"` | markup straight from the docstring |

`icon_mono: true` draws the SVG as a mask in the node colour, which suits
monochrome sets (lucide, feather, tabler) that paint via `currentColor`.
Without `icon` the editor draws a monogram of the stage name
(`IncrementStage` → `IS`); without `color` it picks a deterministic colour for
the category.

Four more things end up in `get_specs()`, and those are **class attributes,
not docstring keys**: `category`, `timeout`, `allowed_events` and
`allowed_inputs` (`EventSpec` / `InputSpec` with a `payload_schema`).

| In the docstring | On the class |
|---|---|
| `description`, `icon`, `icon_mono`, `color` | `category` |
| `arguments`, `outputs`, `reserve` | `timeout` |
| | `allowed_events`, `allowed_inputs` |

A `category:` or `timeout:` written in the docstring is parsed and then
dropped, and nothing warns. `get_specs()` reports the class attribute, so the
spec and the deadline the run enforces agree with each other and there is
nothing to notice: a stage that asks for `timeout: 90` in its docstring alone
runs under the `BaseStage` default of 30 seconds, and the editor is told 30 as
well.

## Prose has languages, identifiers do not

Everything in a spec is either an identifier or prose. A name, a type, a
category, a meter, a colour — identifiers, the same in every language, never
translated. The stage's `description` and the `description` of every argument,
output, event and input — prose, and prose has a language.

So any of those may be a `{locale: text}` mapping instead of a string:

```yaml
description:
  en: "Increment numeric value by delta"
  ru: "Увеличивает число на delta"
arguments:
  delta:
    type: int
    description:
      en: "How much to add"
      ru: "Насколько увеличить"
```

And `get_specs()` hands back **every** language it has, rather than choosing
one:

```python
>>> IncrementStage.get_specs()["description"]
{'en': 'Increment numeric value by delta', 'ru': 'Увеличивает число на delta'}
```

That is the shape a client wants. An editor fetches the specs once and its
reader picks a language afterwards — and picks again whenever they change their
mind — so a spec that had already been narrowed to one language would have to be
fetched again on every change of mind. The spec carries the choice; whoever
draws it chooses.

Prose in one language stays a plain string, because a mapping of one entry is a
choice that carries no information:

```python
>>> LoadTicketStage.get_specs()["description"]
'Takes a prepared ticket out of data/tickets.json'
```

Naming a locale collapses the prose to it, for a caller that really is answering
one reader rather than serving a client:

```python
IncrementStage.get_specs(locale="ru")["description"]   # 'Увеличивает число на delta'
```

A mapping is the whole mechanism for a handful of stages. For a hundred, a
`gettext` catalog of your own is less repetitive, and the built-in stages use
one — both in [Localization](localization.md).

## What it asks to reserve

`reserve` declares what a run of the stage may consume, in the
[meters](limits.md) the host counts. Values are numbers or CEL over `args`,
the arguments as the stage will receive them:

```yaml
reserve:
  llm_calls: 1
  tokens: "args.max_tokens + size(args.text) / 3"
```

It is declared rather than computed because it is read **before** the stage
runs: a host refuses a graph it cannot pay for without executing it, and an
editor can show the figure on the card. What was actually spent is reported
by the stage itself with `self.charge(...)`, which is code, because the truth
is only known at the end.

This is what the specification above turns into on the canvas: the icon, the
description, the arguments it reads and the outputs it writes.

![A stage card drawn from its specification](img/ref-hello-card.png){ width="278" }

