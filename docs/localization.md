# Localization

Two audiences say text in StageFlow, and they get two mechanisms.

The framework's own strings — validation errors, refusals, the descriptions of
the built-in stages — are written in English in the source and translated
through a `gettext` catalog that ships inside the package. **Your** stages are
not in that catalog and should not have to be, so a spec string may be a
per-locale mapping instead of a string: no catalog, no extraction, no build
step.

Nothing has to be configured to run in English. A process that sets no locale
gets the source strings.

## Answering in a language

The locale is not a global. It lives in a `ContextVar`, which is what makes it
usable in a server: a handler sets the locale the request asked for, two
requests answered at the same time get two languages, and a task started from a
handler inherits the handler's locale.

```python
from stageflow import i18n

i18n.available_locales()        # ['en', 'ru'] — what this build can answer in
i18n.get_locale()               # the locale in force, or 'en'

with i18n.use_locale("ru"):     # for the duration of the block
    pipeline.validate()         # its errors come out in Russian

token = i18n.set_locale("ru")   # or by hand, in a handler
i18n.reset_locale(token)
```

`set_locale(None)` returns to the source strings.

### What the caller asked for

An HTTP caller states a preference in `Accept-Language`, which is a ranked list
of tags rather than a tag. `negotiate` takes it and answers with a locale this
build actually has:

```python
wanted = i18n.parse_accept_language("fr;q=0.9, ru-RU;q=0.8, en;q=0.5")
locale = i18n.negotiate(wanted)          # 'ru' — no French catalog, so the next best
```

`ru-RU` finds the `ru` catalog: a tag falls back to its less specific forms
(`pt_BR` → `pt`), and a tag nobody has a catalog for falls back to the source
language. So a regional tag needs no catalog of its own, and an unknown
language is answered rather than refused.

In a backend that is two lines in the handler — see
[2. The endpoints](backend/2-endpoints.md) for where they go:

```python
asked = request.headers.get("accept-language", "")
with i18n.use_locale(i18n.negotiate(i18n.parse_accept_language(asked))):
    ...
```

**Reading the header is the backend's job.** Nothing in the framework touches a
request: `parse_accept_language` is a parser handed a string, `negotiate` matches
tags against the catalogs on disk, and `use_locale` sets the locale for this
context. What language an answer is in is decided where the request is, which is
not here.

This covers the framework's own messages — what a validation error says, what a
refusal says. It does **not** cover the stage specs, which do not have a language
at all: see below.

## Specs have every language, not one

`get_specs()` puts every language the build can answer in into the prose, as a
`{locale: text}` mapping:

```python
>>> ConcatStage.get_specs()["description"]
{'en': 'Concatenate stringified parts with separator',
 'ru': 'Склеивает части через разделитель, приводя их к строкам'}
```

An editor fetches the specs once and its reader picks a language afterwards —
and picks again whenever they change their mind. A backend that had collapsed
the prose to one language would have to be asked again every time, which would
make choosing a language a network round trip. So the specs carry the choice and
whatever draws them chooses; the backend never decides.

Prose nobody has translated stays a plain string, because a mapping of one entry
is a choice that carries no information:

```python
>>> SomeUntranslatedStage.get_specs()["description"]
'Takes a ticket out of the queue'
```

The locale in force does not change this. `use_locale` says which language *this
answer* is in, and a spec is not an answer to anybody:

```python
with i18n.use_locale("ru"):
    ConcatStage.get_specs()["description"]      # still every language
```

Naming a locale outright collapses the prose to it, for a caller that really is
answering one reader — the docs generator, mostly:

```python
ConcatStage.get_specs(locale="ru")["description"]
# 'Склеивает части через разделитель, приводя их к строкам'
```

## Your stages, in every language

A stage describes itself in a [YAML docstring](stage-specification.md). Any
piece of prose in it — the stage's `description`, and the `description` of every
argument, output, event and input — may be a per-locale mapping instead of a
string:

```python
@register_stage("TakeTicketStage")
class TakeTicketStage(BaseStage):
    """
    description:
      en: Takes a ticket out of the queue
      ru: Берёт тикет из очереди
    category: support
    outputs:
      - name: ticket
        description:
          en: The ticket taken
          ru: Взятый тикет
    """
```

A mapping like that is already every language, so `get_specs()` hands it over as
written and the editor picks from it:

```python
TakeTicketStage.get_specs()["description"]
# {'en': 'Takes a ticket out of the queue', 'ru': 'Берёт тикет из очереди'}

TakeTicketStage.get_specs(locale="ru")["description"]    # 'Берёт тикет из очереди'
TakeTicketStage.get_specs(locale="pt-BR")["description"] # the English one: no pt entry
```

Wherever one language does have to be chosen — a `locale=` above, or the editor
choosing for its reader — the keys are negotiated exactly as a request's tags
are, with the source language as the fallback. A mapping written in one language
only is still an answer; a stage with plain strings reads the same in every
language, which is the correct answer for a stage nobody has translated.

### A catalog of your own

A mapping per string is right for a handful of stages and wrong for a hundred.
A host with that many registers a `gettext` domain of its own and points its
stages at it — the framework then looks their prose up there instead of using it
as written:

```python
i18n.register_domain("mystages", "/path/to/locale")

@register_stage("TakeTicketStage")
class TakeTicketStage(BaseStage):
    """
    description: Takes a ticket out of the queue
    """
    i18n_domain = "mystages"
```

The directory is the usual gettext layout — `<locale>/LC_MESSAGES/<domain>.mo`.
A separate domain rather than an addition to `stageflow`'s: your strings are
yours, and looking for them in the framework's catalog would be looking in
somebody else's.

## The catalogs that ship

`stageflow/locale/` holds the source catalog (`stageflow.pot`) and one directory
per language, with the `.po` a translator edits and the compiled `.mo` the
runtime reads. Both ship in the wheel, so a translator who has the package has
the source too.

A locale exists because a catalog for it exists on disk. No language is named
anywhere in the code except `SOURCE_LOCALE`, so **adding a language is adding a
file**, not editing a list — and `available_locales()` will report it.

`tools/i18n.py` does the catalog work (`pip install -e '.[i18n]'` for `babel`,
which is a dev dependency and never needed to run StageFlow):

```bash
python tools/i18n.py extract              # the sources -> locale/stageflow.pot
python tools/i18n.py update               # the .pot -> every locale's .po
python tools/i18n.py update --locale de   # or one, creating it if it is new
python tools/i18n.py compile              # the .po files -> the .mo that ships
python tools/i18n.py stats                # how much of each is done
```

Extraction reads two places, because the framework's translatable text lives in
two: `_("…")` in the source, and the prose of the built-in stages, which is
written in a YAML docstring that no extractor parses — so the stages are
imported and asked, which extracts exactly what `get_specs()` publishes.

One thing is deliberately left in English: the assertion messages of
`stageflow.testing`. They are read by a developer running a test suite, next to
a Python traceback, and translating them would help nobody.

## The editor

The [editor](https://github.com/leo-need-more-coffee/stageflow-ui) is a separate
project with a separate mechanism for its own text — a flat JSON catalog per
language, chosen in "View → Language".

The stage specs are not its own text and it does not try to translate them: it
takes the `{locale: text}` mappings a backend sent, resolves them once against
the language it is drawn in, and draws strings from there on. A regional tag
satisfies a request for the language, a missing language falls back to the one
the specs were written in, and a mapping with neither gives up its only entry.

Which means a backend serving an editor needs to do nothing at all about the
language of its stages — it serves `get_specs()` and the editor sorts it out.
