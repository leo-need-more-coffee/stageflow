# 1. Your stages

A stage is the one place in StageFlow where your code runs. Everything else —
the branching, the loops, the error handling, the frame — is the graph's
business, and the graph is JSON somebody else may well have drawn.

```python
from stageflow import BaseStage, register_stage

@register_stage("LoadTicketStage")
class LoadTicketStage(BaseStage):
    """
    description: "Takes a prepared ticket out of data/tickets.json"
    icon: "/icons/ticket.svg"
    arguments:
      ticket_id:
        type: string
        optional: true
        description: "Ticket id (T-1001); empty — the first one in the file"
    outputs:
      text:
        type: string
        description: "The customer's message"
      customer_id:
        type: string
        description: "Who wrote it"
    """

    category = "support.data"

    async def run(self):
        wanted = (self.get_arguments().get("ticket_id") or "").strip()
        ...
        self.set_outputs({"text": ticket["text"], "customer_id": ticket["customer_id"]})
```

The docstring is not documentation that happens to be parsed — it *is* the
spec, and the editor draws the card from it: the name, the icon, the colour,
which ports exist and what each one is for. Two of the card's fields sit
beside it as class attributes rather than docstring keys — `category` and
`timeout` — and [Stage specification](../stage-specification.md) has the full
grammar with that split. What matters here is that all of it lives in the file
it describes, a few lines from the code it is about.

![The palette, built from the specs the backend serves](../img/be-plan-palette.png){ width="240" }

## Small, and about one thing

The stages in the example are deliberately tiny: load a ticket, classify it,
search the knowledge base, render a reply, send it, escalate. That is not
neatness. **A node on the canvas is a stage**, and a stage that both looks
something up and decides what to do with it becomes a node whose meaning has
to be guessed from its name. The decisions belong in the graph, as a
`condition` or a `switch`, where they can be seen and argued with.

The test is easy to apply: if the docstring needs the word "and", it is two
stages.

## Arguments come from the frame

A stage never reads the frame. It is handed arguments, and the graph says
where they came from:

```json
{
  "id": "classify",
  "type": "stage",
  "stage": "ClassifyByRulesStage",
  "arguments": {"vars": {"text": "text", "subject": "subject"}},
  "outputs": {"topic": "topic", "urgency": "urgency"},
  "next": "search"
}
```

`arguments.vars` maps an argument name to a frame variable, `arguments.const`
to a literal, and a key ending in `.` makes the value a
[CEL expression](../expressions.md). `outputs` maps the other way. The stage
sees a plain dict and returns a plain dict, which is what makes it testable
without a pipeline at all — and what lets the same stage appear twice in one
graph reading different variables.

## Errors are roads

A stage that raises tells the graph what happened, and the graph decides. That
only works if the exceptions are distinguishable:

```python
class LlmAuthError(RuntimeError):      # no key — fall back to the rules
class LlmRateLimited(RuntimeError):    # 429 — a retry with a pause helps
class LlmUnavailable(RuntimeError):    # 5xx — a retry helps too
class LlmBadAnswer(RuntimeError):      # the model answered nonsense
```

Four classes rather than one, because in a graph they become four different
roads: `retry` on the two transient ones, an `except` down to the keyword
rules on the first, a different ending for the last. A single bare `Exception`
would leave the author of the pipeline nothing to branch on — see
[Errors and retries](../errors.md).

## Events, and text as it is written

A stage can emit events, and they reach the editor's log as the run goes:

```python
allowed_events = [
    EventSpec("reply_sent", "The reply left for the customer",
              payload_schema={"ticket_id": str, "channel": str, "chars": int}),
]

self.emit("reply_sent", {"ticket_id": ticket_id, "channel": "email", "chars": len(reply)})
```

Declaring them is worth the three lines: an undeclared type is refused, so a
typo in an event name is caught rather than silently producing an event nobody
listens for.

One payload shape is a contract rather than a convention. Anything with
`{"stream": true, "text": "…"}` is treated by the editor as a piece of text
the node is writing right now, and shown in a column of the debug panel as it
arrives:

```python
for word in reply.split(" "):
    self.emit("reply_chunk", {"stream": True, "text": word + " ", "label": "Reply"})
```

The editor knows nothing about support bots or language models here — it knows
the payload shape. A transcription stage or a build-log stage gets the same
treatment for free, and `label` is what the column is called.

## Registering them

`register_stage` puts the class in a **process-global** registry, which is
what makes `get_stages()` — and therefore `/api/stages` — possible at all:

```python
# app/stages/__init__.py
from . import support, llm  # noqa: F401 - importing registers them
```

Global is also the sentence with consequences mentioned on the
[index](index.md): every stage this process imports is a stage every pipeline
in it can name. That is fine while you write the pipelines; it stops being
fine the moment somebody else does, which is what [step 3](3-policy.md) is
for.

---

Next: [the endpoints](2-endpoints.md) — how the editor reaches any of this.
