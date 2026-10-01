# Policy: what a pipeline may be made of

!!! tip "Building one?"
    [Step 3 of the backend track](backend/3-policy.md) puts this page to work
    in a running backend: a policy per caller, both endpoints narrowed by it,
    and what the editor then shows the author.

The registry says what the **process** can execute. That is a different
question from what a **given pipeline** may use. A platform that runs
pipelines on behalf of several customers hands each of them a subset — these
stages, these node types — and that subset is the boundary the author of the
graph cannot argue with, because they only write the graph.

```python
from stageflow import Context, Pipeline, Policy, Session

basic = Policy(
    stages={"LoadTicket", "ClassifyByRules", "Template"},
    node_types={"entry", "stage", "condition", "switch", "terminal"},
)

session = Session(id="run-1", pipeline=pipeline, context=Context(vars=vars),
                  policy=basic)
```

A policy is given to the session by the **host**, next to the debugger. It is
not read from the pipeline JSON and nothing in the JSON can widen it — a graph
that could raise its own allowance would not be an allowance.

## `None` is not the empty set

The two mean opposite things, and the difference is the point:

| Written | Means |
|---|---|
| `Policy()` | no opinion — everything the process has registered |
| `Policy(stages=set())` | nothing at all |
| `Policy(stages={"A"})` | stage `A`, and no other |

A policy that forgot to list its stages must not silently grant every stage
the process happens to have imported, so the default of each field is `None`
and an empty set is taken literally.

## Refused twice: at validation and while running

```python
pipeline.validate(basic)
# PipelineValidationError: Pipeline validation failed:
#   work: stage 'LlmReply' is not allowed by the policy
#   loop: node type 'map' is not allowed by the policy
```

Validation collects **everything** the policy refuses rather than stopping at
the first, because somebody saving a graph wants the list of what to change.
`Session` validates on construction, so a pipeline with a forbidden stage
never begins — the stage is not reached rather than stopped halfway.

The session also checks while running, on every node and before every stage.
That second line matters for a graph built in Python and never validated, and
it costs a set lookup that is skipped entirely when the field is `None`.

A refusal at run time is a `PolicyViolationError`, which is a `PermissionError`
as well as a `StageFlowError`.

## A subpipeline is not a way out

The child graph of a [`subpipeline`](subpipeline-node.md) node is checked
twice, for different reasons. Validation walks the declared `subpipelines` in
the JSON, nested ones included, and says where the trouble is:

```
[inner] w: stage 'LlmReply' is not allowed by the policy
```

Without that, "you may save this" and "you may not run it" would be hours
apart. At run time the child graph becomes a `Pipeline` of its own and is
validated again, with the parent's policy, which travels down exactly as the
debugger does — so a nested graph is not a way out at either moment.

## Telling a client what it may use

[`capabilities()`](schema-and-stage-specs.md) takes a policy and narrows its
answer to it:

```python
capabilities(basic)
# {"stageflow": "0.13.0",
#  "node_types": ["condition", "entry", "stage", "switch", "terminal"],
#  "stages": 3}
```

That is what a backend should serve to an editor: the editor needs to know
what **this** caller may draw, and whether a node type is missing because the
core is older or because the allowance is narrower is not a distinction it
has to make.

## Letting a road come back

A cycle in the order edges is refused by validation. The language has a loop of
its own — the [`map` node](map-node.md), bounded by the list it walks — and a
back edge is bounded by nothing: it spins for as long as the process lives, and
it is far more often a `next` pointed at the wrong id than a loop somebody
meant.

A host that has something to stop one with can say so:

```python
Policy(allow_cycles=True, limits=Limits(counters={"steps": 5_000}))
```

```json
{"id": "bump",    "type": "stage", "stage": "Increment",
 "arguments": {"vars": {"current": "n"}}, "outputs": {"value": "n"},
 "next": "enough"},
{"id": "enough",  "type": "condition", "condition": "vars.n >= 3",
 "then": "done", "else": "bump"}
```

This is the one field here whose default is a restriction rather than the
absence of one, and the asymmetry is deliberate: every other field narrows what
the process can already do, while this one permits a shape the language does
not otherwise have.

**What makes it safe is not in the graph.** A loop that the graph does not end
is ended by the budget — `BudgetExceeded` is outside the `Exception` tree, so
no `try` in the pipeline can swallow it, and the run comes back as a result
saying which meter ran out rather than as a process that stopped answering
([Limits](limits.md)). Setting `allow_cycles` without a ceiling is permitted
and is a host's business: a platform may bound its work with a request timeout
or a supervisor, and a framework arguing with that would be a framework fighting
its host.

Everything else is still checked. A graph with a permitted loop in it is
refused for a `next` that names nothing, for a stage outside the policy, for a
type that does not exist — allowing a loop is not switching validation off.

Clients are told: `capabilities(policy)` carries `"cycles": true | false`
alongside the node types, because finding the rule out after writing a graph is
the worst moment to learn it.

## What a policy does not do

It restricts **what a pipeline is made of**, not **how much it consumes**. A
graph of allowed stages can still loop, fan out or run for a long time —
`retry`, `map` over a large list, `parallel` branches.

That is the other half, and it lives in the same object: `Policy(limits=...)`,
described in [Limits](limits.md). A plan is one value — which blocks, and how
much of anything.
