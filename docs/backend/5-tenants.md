# 5. Who is calling

A `Policy` is an object. Turning an HTTP request into one is the last step,
and it is the one StageFlow deliberately has no opinion about.

**Authentication is not in the framework and will not be.** The core takes a
policy; tokens, headers, sessions, tenants and subscriptions are things every
platform already has its own answer to, and a framework's answer would be one
more thing to fight. In the example the whole of it is forty lines in
`app/auth.py`, and a real backend replaces that file without touching
anything else.

## Plans are a table

```python
PLANS: dict[str, Policy] = {
    "full":  Policy(),                     # the unrestricted example
    "basic": Policy(stages=RULES_ONLY | PLUMBING, node_types={...}, limits=...),
    "pro":   Policy(node_types=None, limits=Limits(counters={"tokens": 200_000, ...})),
}
```

A real platform looks a tenant up and builds the policy from their
subscription. The words "plan", "basic" and "pro" are the platform's business
— so is the price list that turns `result.meters` into an invoice.

One thing is worth copying from the example even so: **`basic` can run
something**. It has its own pipeline, `06-rules-only.json`, which is the whole
bot with no model in it. A tier that can run nothing at all is not a cheaper
tier, it is a broken one, and it teaches a user nothing except that the
product does not work.

## The credential decides

```python
AUTH_HEADER = os.environ.get("SF_AUTH_HEADER", "Authorization")
TOKENS = _parse_tokens(os.environ.get("SF_TOKENS", ""))   # "tok:plan,tok:plan"


def caller_plan(request: Request) -> str:
    if not TOKENS:                       # nothing configured: tell nobody apart
        return OPEN_PLAN
    token = credential(request)
    if token is None:
        raise HTTPException(401, f"no credentials: send a token in the '{AUTH_HEADER}' header")
    plan = TOKENS.get(token)
    if plan is None:
        raise HTTPException(401, "the credentials were not recognised")
    return plan
```

The header's **name** is configurable because backends disagree —
`Authorization: Bearer …`, `X-Api-Key: …`, whatever a gateway put there — and
the editor has no business deciding. With nothing configured there is no
authentication at all, and the example starts and works, which is the point
of an example.

On the editor's side that is a field, and its value is checked by the same
request that checks the address — so a wrong token is wrong on the screen
where it can be corrected, not at the first run:

![The connection screen refusing, with the credential field unfolded](../img/be-auth.png)

## Being shown a plan is not being on one

Here is the part worth getting right, because the obvious-looking shortcut
destroys everything the previous two steps built.

The read-only endpoints take `?plan=`, **unverified, from anyone**:

```python
@router.get("/meta")
async def meta(caller: CallerPlan, plan: ShownPlan = None) -> dict:
    shown = plan or caller
    policy = policy_for(shown)
    return {"api": 1, "plan": shown, "plan_source": source_of(plan),
            "plans": plan_names(), **capabilities(policy), "limits": _limits_of(policy)}
```

A name is not a permission. Drawing is not running, and an editor that has to
authenticate before it can grey out a palette entry is an editor nobody
configures — so "what would this graph look like on the cheaper tier" is a
question anybody may ask. `plans` is the list to offer, so no client has to
hard-code names it cannot know.

The run takes no such parameter:

```python
@router.post("/run", status_code=201)
async def start_run(body: RunRequest, caller: CallerPlan) -> dict:
    if body.plan is not None and body.plan != caller:
        raise HTTPException(403, f"this graph was prepared for plan '{body.plan}', "
                                 f"and these credentials are on '{caller}'")
    run = runs.start(body.model_dump(), policy_for(caller))
```

`body.plan` travels in the opposite direction: it is what the client **drew
against**, not a request to run on it. Saying it lets the disagreement be
named, which is a far better answer than a pile of validation errors about
individual stages:

![The refusal, naming both plans](../img/be-run-refused.png)

Wire `?plan=` into the run instead — one line, and an obvious-looking one —
and every ceiling in the previous two steps becomes a query parameter. The
example's `check_pipelines.py` checks that nobody did; it is the kind of
property that no endpoint test notices being broken.

## What the editor makes of it

All of it lands in one dialog — "File" → "Connection…", or a click on the
backend line in the status bar:

![The connection dialog](../img/be-connection.png){ width="520" }

The address (changing it reloads; it is the ground the session stands on), the
credential (applied in place — a token goes stale *during* the work, and a
refused one is rolled back so the session survives), the plan to draw against,
and what the backend answered. `?plan=basic` on the editor's own URL opens it
that way, and the status bar marks a preview as a preview so it cannot be
mistaken for an allowance.

## Where the line falls

| | Framework | Platform |
|---|---|---|
| what a graph may contain | `Policy` | which policy |
| what a run may spend | `Limits`, meters, `BudgetExceeded` | what a unit costs |
| who the caller is | — | all of it |
| a monthly quota, a queue, a pool | — | all of it |

The last row matters as much as the others. `concurrency: 8` bounds **one
run**; a hundred runs are eight hundred tasks, and the core cannot know how
many runs exist. Admission — the queue, the per-tenant pool, the monthly
budget — is the platform's, and the core's part is to hold one run's ceiling
and report honestly what it cost.

---

That is the whole backend: [stages](1-stages.md), [endpoints](2-endpoints.md),
a [policy](3-policy.md), [meters](4-metering.md) and a credential. The
finished thing is
[stageflow-example](https://github.com/leo-need-more-coffee/stageflow-example),
about six hundred lines of Python.
