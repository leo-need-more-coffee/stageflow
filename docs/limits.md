# Limits: how much a pipeline may consume

[A policy](policy.md) answers what a pipeline may be made of. This answers how
much of anything it may use, and the two are separate because a graph built
entirely of allowed stages can still loop, fan out into thousands of tasks, or
run until the process dies.

```python
from stageflow import Limits, Policy, Session

plan = Policy(
    stages={"LoadTicket", "ClassifyByRules", "LlmReply"},
    limits=Limits(
        counters={"seconds": 120, "steps": 5_000, "iterations": 2_000,
                  "tokens": 200_000, "llm_calls": 100},
        gauges={"concurrency": 8, "depth": 4, "frame_bytes": 4_000_000},
        max_retries=5,
        max_delay_seconds=30,
    ),
)

session = Session(id="run-1", pipeline=pipeline, context=ctx, policy=plan)
result = await session.run()
result.meters   # {"steps": 412, "seconds": 8.2, "tokens": 5120, "llm_calls": 9}
```

## One mechanism: meters

Not three systems for time, cost and size — one, with two kinds of meter:

| Kind | Behaviour | Examples | Checked |
|---|---|---|---|
| **counter** | grows, never falls | `seconds`, `steps`, `iterations`, `tokens` | on every charge |
| **gauge** | an instantaneous value | `concurrency`, `depth`, `frame_bytes` | when it is taken up |

The runtime charges `steps` and `iterations` and holds the gauges; stages
charge whatever they spent. `concurrency` is the one gauge that **waits**
rather than failing: a hundred items to process is a good pipeline that may
not have a hundred calls in the air at once, and refusing it for being wide
would be refusing the work rather than bounding it. Depth and size do fail —
too deep is too deep, and waiting would not help. **Meter names are not fixed by the core.** A host
counts what is scarce for it, and a meter nobody limits simply accumulates and
comes back in `result.meters`, which is what a bill is made of.

`seconds` is the one hybrid, deliberately. It is reported like a counter so
that it shows up in the same table as everything else, but it is *enforced* as
a deadline — a counter is only checked when something charges it, and a stage
that hangs charges nothing.

!!! note "Nothing is measured unless it is limited"
    Every check costs something and `frame_bytes` costs the most: measuring it
    means serialising the frame. A meter absent from the limits is not
    computed at all, so the price of all this to a host that sets no limits is
    one `if`.

## What a stage spends

A stage charges the **units it consumed** — `tokens`, `llm_calls`,
`http_calls`, `rows`. What a unit is worth is a price list; price lists change
without any code changing, and they belong to the host. A stage knows how much
it used, not what that costs.

```python
@register_stage("LlmTriageStage")
class LlmTriageStage(BaseStage):
    """
    description: "Classify a ticket with a model"
    timeout: 60
    reserve:
      llm_calls: 1
      tokens: "args.max_tokens + size(args.text) / 3"
    """

    async def run(self):
        answer = await self.client.chat(self.get_arguments()["text"])
        self.set_outputs({"topic": answer.topic})
        self.charge(tokens=answer.usage.total, llm_calls=1)
```

Two mechanisms, and they do not overlap:

| | Where | Sees | Answers |
|---|---|---|---|
| `reserve` | the spec, declarative | `args` | may this be attempted at all |
| `charge()` | the stage, in code | everything the stage knows | what it actually spent |

`reserve` is declarative because it is read **before** anything runs: an
editor can show "up to 4000 tokens a call", and a host can refuse a graph
without executing it. Its values are numbers or [CEL](expressions.md) over
`args`, the arguments as the stage will receive them.

The order is reserve → run → settle:

1. the reservation is evaluated and held. It does not fit what is left →
   **the stage never starts**. An expensive call is better not begun than cut
   off after the money is gone;
2. the stage runs;
3. `charge()` **replaces** the reservation for the meters it names and adds
   the ones it does not — an amount both reserved and charged is not counted
   twice. A stage that charges nothing is settled at what it reserved, and
   that is true of a failure too: a call that went out and then timed out
   still spent what it spent.

A stage that reserves and charges nothing is free.

!!! warning "A loop is not reserved as a whole"
    Reservations are per stage, taken as that stage is about to run — nothing
    sums up what a `map` will cost before entering it, because the
    reservation of an iteration depends on its arguments, which depend on the
    item, which comes from the data. What *is* checked up front is the
    iteration count: `iterations` is charged before the first pass, so a loop
    over a thousand items on a plan allowing fifty is refused without running
    any of them.

    A loop therefore stops as soon as it cannot afford the next stage, with
    the passes it did make already paid for. In `parallel` mode the overshoot
    is bounded by `concurrency` rather than by one stage: calls already in
    flight have already been made. A host that needs a hard ceiling for the
    whole loop gets it by arithmetic rather than prediction — cap
    `iterations` so that the count times the worst case fits the budget.

## What validation refuses before the run

`Pipeline.validate(policy)` checks what is **soundly knowable from the JSON**,
so that a tenant saving a graph is told what to change rather than finding out
on the tenth element:

| Checked | Because |
|---|---|
| the shortest way through the graph against `steps` | every run passes at least that many nodes, so a graph whose cheapest path does not fit cannot finish at all |
| how deep the declared subpipelines nest against `depth` | the nesting is written down, not computed |
| what each `retry` asks for against `max_retries` and `max_delay_seconds` | the numbers are in the JSON |

The bound on steps is a **lower** one, which is what makes it safe to refuse
by: a graph with a long road and a short one is judged by the short one.

What is *not* checked is anything that would have to be guessed. A loop's cost
is unknowable before the run — the number of passes comes from the data and
the reservation of a pass from its arguments — and multiplying something here
would refuse pipelines that fit. A false refusal on a working graph is worse
than the ceiling it was meant to save.

## Running out

The ceiling is hard. `BudgetExceeded` inherits **`BaseException`**,
deliberately outside the `StageFlowError` tree, because `try` blocks catch
`Exception` and so does the `retry` machinery — a tenant able to wrap a graph
in `try` and swallow this would make every limit decoration.

`Session.run` is the one place that catches it, and it ends the run the way a
stop does, rather than throwing an exception through work that did happen:

```python
result.result   # {"status": "budget_exceeded", "meter": "tokens",
                #  "limit": 200000, "spent": 200512}
result.meters   # everything spent, for the host to bill by
```

A child session of a `subpipeline` shares the parent's budget — a fresh one
per child would multiply the allowance by the depth of nesting — and lets the
stop travel up rather than swallowing it.

## The tenant's knobs are requests

`retry` and the pauses between attempts come from the pipeline, which is to
say from the less-trusted side. `max_retries` and `max_delay_seconds` clamp
them: a node asking for fifty attempts on a plan that allows three gets three,
and the event says three.

## Waiting for a person is not work

A pipeline that asks a question and waits is not consuming the host, so the
deadline does not tick while it waits. Without that, a deadline would be a
limit on how fast somebody reads.

This also fixes an older trap: a stage that waits for input used to die at its
own 30-second timeout. A stage timeout is now measured in busy time as well,
and clamped by what is left of the deadline — otherwise a stage allowed sixty
seconds and started with two left would run for sixty, and the budget would
leak by the length of its last stage.

## What limits do not do

`concurrency: 8` bounds **one run**. A hundred runs are eight hundred tasks:
the core cannot know how many runs exist, so **admission control is the
host's** — a queue and a pool per tenant. Likewise a monthly quota: the core
enforces the ceiling of one run and reports what it spent; subtracting that
from an allowance for the month, and turning units into money, is the
platform's accounting.
