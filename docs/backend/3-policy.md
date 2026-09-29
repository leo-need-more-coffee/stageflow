# 3. What may be composed

Here is the sentence from [step 1](1-stages.md) with its consequence attached:
`register_stage` puts a class in a **process-global** registry, so every stage
this process imports is a stage every pipeline in it can name.

That is fine while the pipelines are yours. It stops being fine the moment
they are not — and "the developer defines the blocks, someone less trusted
composes them" is the arrangement StageFlow exists for. Without something
else, `ChargeCardStage` is available to any JSON anybody posts.

The something else is a [`Policy`](../policy.md): given by the host, never
readable from the pipeline, and never wideneable by it.

```python
from stageflow import Limits, Policy

BASIC = Policy(
    stages={"LoadTicketStage", "ClassifyByRulesStage", "SearchKnowledgeStage",
            "RenderReplyStage", "SendReplyStage", "EscalateStage",
            "SetValueStage", "ConcatStage", "TemplateStage"},
    node_types={"entry", "stage", "condition", "switch", "terminal"},
    limits=Limits(
        counters={"seconds": 15, "steps": 200, "kb_lookups": 20, "replies_sent": 1},
        gauges={"concurrency": 2, "depth": 2, "frame_bytes": 200_000},
        max_retries=2, max_delay_seconds=2,
    ),
)
```

Three things, and `None` in any of them means "no opinion", which is not the
same as an empty set: `Policy()` allows everything, `Policy(stages=set())`
allows no stage at all.

## Two layers, the same answer

```python
pipeline.validate(policy)                       # before: the whole graph, at once
Session(id=..., pipeline=..., policy=policy)    # during: every node, as it is reached
```

Validation is what a person acts on — it names every problem in one go, so a
graph can be fixed rather than discovered to be wrong one node at a time. The
runtime check is what actually holds, because a graph can reach a session
without ever being validated. Neither replaces the other, and the wording is
the same either way.

The example validates in the request handler, before a thread is even
started:

```python
def start(self, payload: dict, policy: Policy) -> Run:
    pipeline = Pipeline.from_dict(payload.get("pipeline") or {})
    pipeline.validate(policy)      # a refusal answers the POST, not a thread minutes later
    ...
    session = Session(..., policy=policy)
```

## Telling the editor, so it can tell the author

A policy the editor does not know about is a policy the author discovers by
being refused. So both read-only endpoints are narrowed by it:

```python
@router.get("/stages")
async def stages(caller: CallerPlan) -> dict:
    policy = policy_for(caller)
    return {"stages": {name: cls.get_specs()
                       for name, cls in get_stages().items()
                       if policy.allows_stage(name)}}


@router.get("/meta")
async def meta(caller: CallerPlan) -> dict:
    policy = policy_for(caller)
    return {"api": 1, "plan": caller, **capabilities(policy), "limits": _limits_of(policy)}
```

Sending the specs of a stage the policy forbids would have the editor draw it
in the palette and the run refuse it — exactly the mismatch the policy exists
to remove, reintroduced one endpoint later. `capabilities(policy)` does the
same for node types.

What that buys, on the cheap plan of the example, looking at a pipeline that
needs a `map`:

![A graph the plan cannot run, said before the run](../img/be-plan-refuses.png)

Four node types are dim in the palette, the offending node carries a marker,
and the status bar reads `per_ticket: plan 'basic' does not include a 'map'
node` — before anybody presses Run.

## What validation checks, and what it refuses to guess

Against `limits`, static validation checks only what is **soundly knowable
from the JSON**:

| Checked | Why it is honest |
|---|---|
| the shortest path through the graph against `steps` | every run passes at least that many nodes, so a graph whose cheapest path does not fit cannot finish at all |
| the nesting depth of declared subpipelines against `depth` | the nesting is written down, not computed |
| what each `retry` asks for against `max_retries` and `max_delay_seconds` | the numbers are in the JSON |

A loop's cost is not on that list and never will be: the number of passes
comes from the data and the cost of a pass from its arguments. Any
multiplication there would refuse graphs that fit, and a false refusal of a
working graph is worse than the ceiling it was protecting.

## The ceiling is hard

`BudgetExceeded` inherits from **`BaseException`**, deliberately outside the
`StageFlowError` hierarchy: `try` blocks catch `Exception` and so does the
retry machinery, and a tenant who can wrap a graph in a `try` and swallow the
refusal would turn every limit into decoration. Exactly one place catches it —
`Session.run` — and ends the run the way `stop` does:

```python
result.result   # {"status": "budget_exceeded", "meter": "kb_lookups", "limit": 20, "spent": 21}
result.meters   # everything spent — this is what an invoice is built from
```

---

Next: [what a run costs](4-metering.md) — filling those meters in.
