# The stage node

The only node that does work. Everything else decides where to go; a `stage`
runs a registered stage class and writes what it returned into the frame.

```json
{
  "id": "greet",
  "type": "stage",
  "stage": "TemplateStage",
  "arguments": {
    "const": { "template": "Hello, {name}! You have {count} tickets." },
    "vars": { "name": "user", "count": "open_tickets" }
  },
  "outputs": { "value": "greeting" },
  "consume": ["user"],
  "next": "done"
}
```

![A stage node on the canvas](img/ref-node-card.png){ width="278" }

## Arguments: two buckets

A stage never reaches into the frame itself — it only receives arguments. Both
buckets end up as one flat set of keyword arguments:

| Bucket | Means | Example |
|---|---|---|
| `const` | a literal, written in the pipeline | `"template": "Hello, {name}!"` |
| `vars` | the value of a frame variable | `"name": "user"` → the argument `name` gets `vars.user` |

`vars` also accepts a list when the argument and the variable share a name:
`"vars": ["ticket_id"]` is the same as `"vars": {"ticket_id": "ticket_id"}`.

A key ending in `.$` is a [CEL expression](expressions.md) instead of a value,
and works in either bucket:

```json
"arguments": {
  "const": { "greeting.$": "'Hello, ' + vars.user" },
  "vars": { "total.$": "vars.a + vars.b" }
}
```

The argument is named without the suffix (`greeting`, `total`). Which bucket
holds the expression makes no difference — the suffix is what decides.

## Outputs: what goes back into the frame

What the stage returned becomes a namespace of fields:

| The stage returned | The namespace |
|---|---|
| a dict | itself |
| an object | its attributes (`vars(obj)`) |
| anything else | `{"value": …}` |
| `None` | empty |

`outputs` maps a field of that namespace to a variable — **field first,
variable second**:

```json
"outputs": { "value": "greeting" }
```

Nothing is written implicitly: a field the stage returned and `outputs` does
not mention simply stays behind. A field that `outputs` asks for and the stage
did not return is a `StageOutputError` naming what was available — fail-fast
at the cause, rather than a `KeyError` three nodes later.

A computed output is the mirror image of a computed argument — the key carries
the `.$` and the destination, the value is the expression, and the stage's
return is visible in it as `output`:

```json
"outputs": { "loud.$": "output.value + '!'" }
```

## consume: dropping what is no longer needed

```json
"consume": ["user", "raw_html"]
```

The names are removed from the frame after the step. Useful for a bulky value
that only mattered to this stage — a downloaded page, a decoded file — so that
it is not dragged along the rest of the graph and does not show up in the
snapshot of a session.

## What validation catches before the run

`Pipeline.validate()` compares the node against the
[spec of the stage](stage-specification.md):

- an unknown stage name;
- an output field the stage does not declare (`does not return field 'x'
  (available: [...])`) — unless the spec declares `*`;
- with [typing](variable-typing.md) on, an argument fed by a variable whose
  declared type does not match the spec, and an output written into a variable
  of the wrong type.

`retry` and `expose` work here as on any node — see
[Errors](errors.md) and [Node types](node-types.md).
