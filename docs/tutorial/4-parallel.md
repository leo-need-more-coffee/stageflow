# 4. Two things at once

Two things the bot needs are independent: who the customer is, and what the
knowledge base says. There is no reason to do them one after the other.

## Branches

A `parallel` node names its branches. Each branch is an entry node id; the
branch runs from there until its road ends:

```json
{"id": "gather", "type": "parallel",
 "branches": [{"id": "who", "entry": "customer"},
              {"id": "kb",  "entry": "search"}],
 "next": "route"}
```

```json
{"id": "customer", "type": "stage", "stage": "LoadCustomerStage",
 "arguments": {"vars": {"customer_id": "customer_id"}},
 "outputs": {"name": "customer_name", "plan": "plan"},
 "next": null},

{"id": "search", "type": "stage", "stage": "SearchKnowledgeStage",
 "arguments": {"vars": {"text": "text", "topic": "topic"}},
 "outputs": {"found": "found", "answer": "answer", "article_id": "article_id"},
 "next": null}
```

`"next": null` ends the branch. When every branch has ended, execution
continues at the `next` of the parallel node.

![A parallel node and the branches it frames](../img/tut-parallel.png){ width="418" }

## What comes back

Each branch gets its own copy of the frame, so branches cannot interfere with
each other. When they finish, only the names that did not exist before the
parallel node are merged back:

```
parallel_completed  {"merged": ["answer", "article_id", "customer_name", "found", "plan"],
                     "dropped": []}
```

A branch overwriting a name that already existed keeps that write to itself.
Two branches writing the same new name is an error (`BranchError`), and it
names both branches.

If a branch fails, the others are cancelled by default and the node fails with
that error. `"cancel_on_error": false` lets the siblings finish first. Details
are on the [parallel branches](../parallel-branches.md) page.

## More than two roads

The bot now has three outcomes: urgent tickets go to a person straight away,
unanswerable ones go to a different queue, and the rest get a reply. A
`condition` gives two roads, so use a `switch`:

```json
{"id": "route", "type": "switch",
 "cases": [{"when": "vars.urgency == 'high'", "next": "escalate_urgent"},
           {"when": "!vars.found",            "next": "escalate_unknown"}],
 "default": "render"}
```

Cases are tried in order and the first true one wins. `default` is where
everything else goes.

![The switch and the three roads out of it](../img/tut-switch.png){ width="598" }

The run says which case matched:

```
switch_evaluated  {"matched": null, "next": "render"}                       # T-1001
switch_evaluated  {"matched": "!vars.found", "next": "escalate_unknown"}    # T-1005
```

Next: [when a node fails](5-errors.md).
