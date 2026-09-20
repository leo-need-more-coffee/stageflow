# 5. When a node fails

Keyword rules are reliable but dull. A language model reads the ticket better:
it can tell the topic, the urgency and the mood. It can also be unreachable,
rate-limited, or refused for lack of a key.

That is not a reason to put a `try` in Python. A failure here is a fork in the
graph like any other: if the model cannot answer, fall back to the rules.

## Retry belongs to the node

Some failures are worth simply repeating. Rate limits, for example:

```json
{"id": "triage", "type": "stage", "stage": "LlmTriageStage",
 "arguments": {"vars": {"text": "text", "subject": "subject", "api_key": "OPENAI_API_KEY"}},
 "outputs": {"topic": "topic", "urgency": "urgency", "summary": "summary"},
 "retry": [{"error_equals": ["LlmRateLimited", "LlmUnavailable"],
            "max_attempts": 3, "interval_seconds": 2}]}
```

`max_attempts` counts the first run too, so `3` means one attempt and two
retries. Any node can carry `retry`, not only stage nodes.

## Try covers a region

A `try` node does not wrap a single node, it covers a piece of the graph:
everything reachable from `body` but not from `next`.

```json
{"id": "guard", "type": "try", "body": "triage", "next": "gather",
 "except": [{"error_equals": ["LlmAuthError"], "next": "rules", "result_var": "llm_error"},
            {"error_equals": ["*"],            "next": "rules", "result_var": "llm_error"}]}
```

`error_equals` takes a class name, a fully qualified path, or `*` for
anything. `result_var` puts the error into the frame as an object with `type`,
`full_type`, `message` and `node`, so a later node can see what went wrong.

![The try node, its body and its except road](../img/tut-try.png){ width="418" }

The editor frames both regions: the body around `triage`, the handler around
`rules`.

## Where a road ends

Both roads of the block end the same way. A road that simply stops
(`"next": null`) hands control back to the `try` node, and the graph goes on
at the node's own `next` — whether that road was the body or a handler.

So the rules stage can either name where it continues or leave `next` out; in
both cases the graph carries on at `gather`:

```json
{"id": "rules", "type": "stage", "stage": "ClassifyByRulesStage",
 "arguments": {"vars": {"text": "text", "subject": "subject"}},
 "outputs": {"topic": "topic", "urgency": "urgency"},
 "next": "gather"}
```

A `terminal` inside the block is the exception: it ends the whole run, and
nothing after the block is executed.

## What it looks like when it fires

Run the bot without a working key. The model stage fails, the handler catches
it, and the rules take over:

```
stage_failed  triage  stage: LlmTriageStage, error: no OpenAI API key …
try_caught    guard   type: LlmAuthError, next: rules
node_enter    rules
```

The path of the run, from the start to the reply:

```
start → load → guard → triage → rules → gather → customer → search → route → render → send → answered
```

The result is the same `{'status': 'answered'}` as before. The bot lost the
model's reading of the ticket and kept working.

Next: [a graph inside a node](6-subpipelines.md).
