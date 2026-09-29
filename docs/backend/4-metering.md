# 4. What a run costs

[Step 3](3-policy.md) put ceilings in the policy. This step is about the
numbers that rise towards them — and the first thing to say is that most
stages should contribute none.

Reading a JSON file costs the platform nothing worth counting. A meter nobody
needs is a number in the way, and the core agrees: **what is not limited is
not measured**, so the whole mechanism costs a host with no limits a single
`if`. A stage that declares nothing is free, and in the example most of them
are.

Three of the bot's stages charge, and the two model ones both reserve and
charge. Between them they show every shape the mechanism has.

## Charging: what was actually used

```python
class SearchKnowledgeStage(BaseStage):
    async def run(self):
        ...
        self.charge(kb_lookups=1)
        self.set_outputs({...})


class SendReplyStage(BaseStage):
    async def run(self):
        ...
        self.charge(replies_sent=1, reply_chars=len(reply))
```

`charge()` reports **units, not money**. What a unit is worth is a price list;
price lists change without any code changing, and they belong to the host. A
stage knows how much it used, not what that came to.

The names are yours. `kb_lookups`, `replies_sent`, `escalations` are units of
*this* business, and the core fixes none of them — it fixes `steps`,
`seconds`, `iterations` and the gauges because it is the only thing that can
count those. Everything else is whatever is scarce for you: rows written,
messages sent, minutes of audio, a person's attention.

A meter nobody limits still accrues and still arrives in `result.meters`, so
you can meter first and decide on ceilings later.

## Reserving: what may not even be attempted

```python
class LlmReplyStage(BaseStage):
    """
    description: "Writes the reply from the knowledge base article — as a stream"
    reserve:
      llm_calls: 1
      tokens: "(size(args.text) + size(args.article)) / 4 + 600"
    """

    timeout = 120

    async def run(self):
        answer, usage = await self._chat(...)
        self.charge(llm_calls=1, tokens=usage.total_tokens)
```

Two mechanisms, and they do not overlap:

| | Where | Sees | Answers |
|---|---|---|---|
| `reserve` | the spec, declaratively | `args` | may this be attempted at all |
| `charge()` | the stage's code | everything the stage knows | what it actually came to |

`reserve` is declarative because it is read **before** the run: the editor can
show "up to 4000 tokens a call", and the host can refuse a graph without
executing it. Its values are numbers or [CEL](../expressions.md) over `args`
— the arguments as the stage will receive them.

The order is reserve → run → settle:

1. the reservation is computed and held. Does not fit in what is left, and the
   **stage is not constructed at all**: an expensive call is better not
   started than cut off once the money has gone;
2. the stage runs;
3. `charge()` **replaces** the reservation for the meters it names and adds
   the ones it does not, so an amount both reserved and charged is not counted
   twice. A stage that charges nothing settles at its reservation — including
   when it fails, because a call that went out and timed out still spent what
   it spent.

The example's model stages ask the provider for the real figure —
`stream_options={"include_usage": True}` on a stream, since usage arrives in a
final chunk that carries no choices — and fall back to four characters to the
token when a gateway sends none. That fallback is a judgement call worth
naming: charging nothing would quietly turn a ceiling on `tokens` into a
ceiling on nothing.

## What it looks like

The debug panel shows what the run has spent against what it was allowed,
during the run and after it:

![Meters after a run on the cheap plan](../img/be-meters.png)

`kb_lookups 1/20`, `steps 9/200`, `seconds 0.01/15` — and `replies_sent 1/1`
in red, because this plan allows exactly one reply per run and the run just
used it. `reply_chars 282` has no denominator: nothing limits it, so it is
being counted and reported and nothing more. The `stage_charged` event in the
log on the right is the settlement itself.

## The loop that cannot be priced in advance

A reservation is taken per stage, as each one starts. What a `map` will cost
is *not* computed before the loop is entered — an iteration's reservation
depends on its arguments, its arguments on the element, and the element on the
data.

What can be checked in advance is the **number of iterations**, because the
list is there: `iterations` is charged before the first pass, so a loop over a
thousand elements on a plan allowing fifty is refused having run none of them.

So a loop stops as soon as it cannot pay for the next stage, and the passes
already made are already paid for. A host that wants a hard ceiling on a whole
loop gets it from arithmetic rather than prediction: limit `iterations` so
that the worst case fits.

---

Next: [who is calling](5-tenants.md) — deciding which policy any of this is.
