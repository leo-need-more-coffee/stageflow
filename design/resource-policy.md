# Policy and budget: what a tenant may do, and how much

> A design note, not shipped behaviour. It records the decisions taken before
> the implementation so that the implementation can be argued with.

## The gap this closes

StageFlow answers **what** a less-trusted party may do: a pipeline may only
name stages that the platform registered. It answers nothing about **how
much**. Measured on the core as of 0.11:

| Lever | Today |
|---|---|
| A graph with an edge pointing back (`next` to itself) | passes `validate()`, spins at ~22 000 nodes/s |
| `Session.event_history` | a plain list, ~44 000 events/s, no ceiling |
| `map` with `mode: parallel` | one task per element, count comes from the data: 2000 elements → 2000 tasks |
| A subpipeline referencing itself | recursion until the stack runs out |
| A single stage | `BaseStage.timeout = 30` — the only thing bounded today |

And the registry is a module-level global, so one process cannot offer tenant
A twelve stages and tenant B twenty. That is the headline claim of the project
— the platform developer decides which blocks exist — implemented only at the
granularity of a whole process.

## The shape

Three parties, three kinds of decision, and each number is set by the party
that can know it:

| Party | Trusted? | Decides | Where it is written |
|---|---|---|---|
| Stage author | yes | what a stage costs, how long it may take | the stage's docstring spec |
| Platform | yes | what a tenant may use and how much | `Policy`, passed to `Session` |
| Tenant | no | the shape of the graph | the pipeline JSON |

The tenant's `retry`, `delay` and `mode: parallel` stop being settings and
become **requests** the platform clamps. Nothing in the JSON can raise a
limit; if it could, there would be no limit.

The core knows `Policy`. The core does not know the words "tenant",
"subscription" or "Pro Max" — the platform maps a plan onto a policy and hands
over the result. Billing does not leak into the framework.

## One mechanism: meters

Not three systems for cost, time and limits — one, with two kinds of meter:

| Kind | Behaviour | Examples | Checked |
|---|---|---|---|
| **counter** | grows, never falls | `seconds`, `steps`, `usd`, `tokens`, `llm_calls` | on every charge |
| **gauge** | an instantaneous value | `concurrency`, `depth`, `frame_bytes` | on acquire |

The runtime charges `seconds` and `steps` itself; stages charge the rest.
Meter names are **not** fixed by the core: a platform counts what is scarce
for it. One check, one line in the event stream, one row in the debug panel,
for `usd` as much as for `crm_writes`.

`seconds` is a hybrid, deliberately: it is reported as a counter so that it
shows up in the same table as everything else, but it is *enforced* as a
deadline. A counter is only checked when something charges it, and a stage
that hangs charges nothing — only a deadline plus `wait_for` notices that.

### Nothing is measured unless it is limited

Every check costs something, and `frame_bytes` costs the most. A meter absent
from the policy is not computed at all. The price of the whole mechanism for
a host that sets no limits is one `if`.

## The objects

```python
@dataclass(frozen=True)
class Limits:
    counters: dict[str, float] = field(default_factory=dict)
    gauges: dict[str, float] = field(default_factory=dict)
    max_iterations: int | None = None


@dataclass(frozen=True)
class Policy:
    #: None means "everything the process has registered"
    stages: frozenset[str] | None = None
    node_types: frozenset[str] | None = None
    limits: Limits = Limits()
    #: charging a meter the limits do not mention is an error, not free
    strict_meters: bool = False
```

`Budget` is the mutable half — one object for the whole session tree:

```python
budget.charge("usd", 0.002)        # counter; raises when it goes over
budget.settle("usd", 0.0031)       # replaces a reservation with the real figure
with budget.gauge("concurrency"):  # peak; raises on acquire
    ...
budget.spent()                     # {"steps": 412, "usd": 0.31, ...}
budget.remaining("seconds")        # what a stage timeout is clamped to
```

## Where it hooks

| Point | What happens | Why there |
|---|---|---|
| `Session.execute_node` | charge `steps`, check the deadline, gauge `depth` | 0.8.0 already made it the single path for every node, in `run`, `run_scope`, `run_subgraph` and `map` — the same choke point the debugger uses |
| `Session.run_stage` | stage allowed? price → reserve → run under `min(stage.timeout, remaining)` → settle | the arguments are resolved by here, which the price formula needs |
| `map` / `parallel` | gauge `concurrency` per iteration and branch, check `max_iterations` | the only places that fan out |
| `Pipeline.validate(policy=…)` | stages, node types, node count, depth, declared `retry` attempts, meter cross-check | so the tenant is told at save time, not at run time |

A child session of a `subpipeline` gets **the same** `Budget` object, not a
copy — otherwise nesting multiplies the allowance. The debugger is already
threaded down this way.

## Price: two slots, and the one that matters is the second

The cost of a model call is decided by the answer, not by the question. So a
price is not one formula evaluated up front; it is two optional slots, and
they differ only in what they can see:

| Slot | Evaluated | Sees | For |
|---|---|---|---|
| `reserve` | before the stage runs | `args` | may this be attempted at all |
| `actual` | after it returns | `args` **and** `output` | what it really cost |

`output` is the same namespace the `outputs` mapping already works over — the
dict a stage returned, or its attributes, exactly as `apply_outputs` sees it.
Nothing new to learn, and the number the vendor reports is right there:

```python
@register_stage("LlmTriageStage")
class LlmTriageStage(BaseStage):
    """
    description: "Classify a ticket with a model"
    timeout: 60
    outputs:
      topic: {type: string}
      tokens_in: {type: int}
      tokens_out: {type: int}
    price:
      usd:
        reserve: 0.05                      # the worst this call may cost
        actual: "0.000003 * output.tokens_in + 0.000015 * output.tokens_out"
      llm_calls: 1
    """

    async def run(self):
        answer = await self.client.chat(self.get_arguments()["text"])
        self.set_outputs({"topic": answer.topic,
                          "tokens_in": answer.usage.input,
                          "tokens_out": answer.usage.output})
```

The stage does not mention money at all. It returns what it used; the price
of those units is a property of the deployment, and it lives in the spec
where the platform can read and show it.

A fixed price is a bare number — it means both slots at once:

```yaml
price:
  usd: 0.001
  http_calls: 1
```

### When a formula is the wrong tool

Per-model rate tables, vendor-reported totals, a price that depends on which
branch the code took — these are not worth bending CEL around. The stage says
so directly, and an explicit charge wins over whatever the spec computed:

```python
self.charge(usd=answer.billed_usd, tokens=answer.usage.total)
```

So: `reserve` gates admission, `actual` prices the common linear case
declaratively, `charge()` is for everything a formula should not express.

### The order, and what happens when it goes wrong

1. `reserve` is evaluated and held. It does not fit the remaining budget →
   **the stage never starts** and the run stops. An expensive operation is not
   begun, rather than cut off halfway.
2. The stage runs.
3. `actual` (or `charge()`) replaces the reservation for the meters it names
   and adds the ones it does not. Without replacing, an amount both reserved
   and charged would be counted twice.

**If the stage fails, the reservation stands.** A call that went out and then
timed out still cost money; refunding it by default would be the optimistic
lie. A stage that knows better charges the truth from its own `except`.

A stage with no `price` is free. Forgetting it therefore gives work away, so
`validate()` warns when a pipeline uses a stage that declares no price while
the policy limits meters.

**The unavoidable overshoot.** With an after-the-fact price the ceiling can be
exceeded by one call: the model has already answered and the money is spent.
The most that can be overshot is the difference between the priciest
`reserve` in the plan and what it actually cost. A platform that cannot accept
even that sets `reserve` to a true upper bound — which is what `max_tokens`
on the call is for.

## Exceeding it

The ceiling is hard everywhere. `BudgetExceeded` inherits **`BaseException`**,
deliberately outside the `StageFlowError` tree, for one reason: `try` blocks
catch `except Exception`, and `run_with_retry` does too. A tenant who could
wrap the graph in `try` and catch the budget would make it decoration.
Uncatchability comes for free from the class hierarchy rather than from a
check that can be forgotten.

`Session.run` is the one place that catches it, and it ends the run the way a
stop does — a defined result, artifacts preserved, the event stream saying
why:

```python
result = await session.run()
result.result    # {"status": "budget_exceeded", "meter": "usd",
                 #  "limit": 1.0, "spent": 1.0031}
result.artifacts # whatever the run had already produced
result.meters    # {"steps": 412, "seconds": 8.2, "usd": 1.0031, "tokens": 5120}
```

## A platform wiring two plans

```python
PLANS = {
    "pro": Policy(
        stages=frozenset({"LoadTicket", "ClassifyByRules", "Template", "SetValue"}),
        node_types=frozenset({"entry", "stage", "condition", "switch", "terminal"}),
        limits=Limits(
            counters={"seconds": 30, "steps": 5_000, "usd": 0.0},
            gauges={"concurrency": 4, "depth": 3},
            max_iterations=200,
        ),
    ),
    "pro_max": Policy(
        stages=frozenset({"LoadTicket", "ClassifyByRules", "Template", "SetValue",
                          "LlmTriage", "LlmReply"}),
        node_types=None,                       # every type the core has
        limits=Limits(
            counters={"seconds": 300, "steps": 100_000, "usd": 5.0, "llm_calls": 200},
            gauges={"concurrency": 16, "depth": 8, "frame_bytes": 8_000_000},
            max_iterations=10_000,
        ),
    ),
}

async def run_for(tenant, pipeline_json, vars):
    policy = PLANS[tenant.plan]
    pipeline = Pipeline.from_dict(pipeline_json)
    pipeline.validate(policy=policy)          # refuses before anything runs

    session = Session(id=f"{tenant.id}:{uuid4()}", pipeline=pipeline,
                      context=Context(vars=vars), policy=policy)
    result = await session.run()
    await billing.record(tenant, result.meters)   # the period quota lives here
    return result
```

Note what the platform does and the core does not: the plan table, the
tenant id, and the period quota.

## Per run and per period are different problems

|  | Per run | Per period |
|---|---|---|
| Lives in | `Limits`, in the process | the platform's database |
| State | none | durable, shared across processes |
| The core's part | **stops** the run at the ceiling | **reports** what was actually spent |

"Pipelines up to 1000" is the first. "1000 a month" is the second, and the
core's only duty there is an honest final reading.

**The double spend.** Ten concurrent runs of one tenant each see "quota
available" and together overspend it. The only fix is reserve-then-settle: the
platform debits the run's *ceiling* before it starts and refunds the
difference after. That is why `Limits` carries an explicit ceiling and
`SessionResult` carries the final meters — without that pair, a reservation
cannot be built on top.

## Limits compose; concurrency especially

`concurrency: 16` bounds one run. A hundred runs are 1600 tasks. The core
cannot and should not know how many runs exist — **admission control is the
platform's** (a queue and a pool per tenant). This has to be said out loud in
the documentation, or somebody will read `concurrency` as protecting the
server.

## Predictability, and why `map` changed it

While a graph was a chain, a static cost estimate was a sum over reachable
nodes. With `map` the iteration count comes from the data, so **the graph has
no upper bound any more** — only a floor.

Hence: to sell "a pipeline up to 1000" and mean it *before* the run,
`max_iterations` must be part of the plan. Otherwise the figure is not a
promise but a place where the run gets cut. It is a required field of the
policy for that reason, not an option.

Also: an estimate can only be computed from literal arguments. An argument
read from the frame is known at execution time, so the editor shows a price
for constant nodes and "depends on the data" for the rest.

## What is deliberately not here

- **Per-process or container isolation** as the default. It protects more, and
  it contradicts the promise the whole project is built on — a library with no
  infrastructure beyond the application's own server. `Budget` is an interface;
  a host that needs hard isolation implements it.
- **Rate limiting** in calls per second — throughput, not a total; a concern of
  the stage that talks to the throttled service.
- **Monthly quotas and markup by plan** — the platform's accounting. It
  constructs `Limits` per run out of its own books and multiplies the totals
  afterwards.
- **Fairness between tenants** — a scheduler, not a budget. Worth naming in
  the docs all the same.

## Traps worth keeping in the tests

- a `try` block **cannot** swallow `BudgetExceeded`;
- a subpipeline gets the parent's budget, not a fresh one;
- `retry` × `map` × `parallel` multiply the price: three nodes on the canvas
  can be thirty thousand calls;
- a stage timeout is clamped by the remaining deadline, or the budget leaks by
  the length of the last stage;
- a meter name typo (`usd` / `USD`) silently means "unlimited" unless
  `strict_meters` is on;
- waiting for a human is not work: `wait_input` must not burn the deadline,
  and today it burns the stage's 30-second timeout instead.

## Order of work

1. **`Policy` with the stage and node-type sets** — the hole in the project's
   central claim, and the one a reader will poke at first.
2. **Meters and `Limits`**: `seconds`/`steps`, gauges, one budget per session
   tree, the hard stop, `BudgetExceeded` outside the catchable tree.
3. **Prices**: `price:` in the spec, `charge()`, reserve/settle,
   `max_iterations`.
4. **Outward**: `/api/meta` describing the *caller* rather than the backend,
   the remaining budget in the debug panel, a documentation page.

Bug fixes that need none of the above and should not wait for it: a ring
buffer for `event_history`, cycle detection in `validate()`, and waiting for
input not consuming the stage timeout.
