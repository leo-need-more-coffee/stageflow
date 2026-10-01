# Assistants (MCP)

A pipeline is JSON, and a language model can write JSON. What it cannot do is
know whether the graph it wrote is one *your* backend will accept: the stages
are yours, the node types are your core's, and the ceilings belong to whoever
is calling.

[stageflow-mcp](https://github.com/leo-need-more-coffee/stageflow-mcp) is the
missing half — an [MCP](https://modelcontextprotocol.io) server that speaks the
same seven endpoints the editor does, so an assistant can check a graph against
a real backend, run it, and read what happened.

**Your backend needs no changes.** It is a second client of a contract you
already serve, and it works against a deployment that predates it.

## Setting it up

```bash
claude mcp add stageflow -- uvx stageflow-mcp --backend https://your-backend.example
```

That address is the one you would type on the editor's connection screen, and
it is read the same way — `localhost:8765` and `…/api` both mean one backend.
A bare hostname gets `https://` unless it is loopback.

Check it before wiring an assistant to it:

```console
$ stageflow-mcp --backend https://your-backend.example --check
backend      https://your-backend.example
credential   none sent
stages       23
core         0.13.0
plan         demo (open)
node types   condition, entry, map, parallel, stage, subpipeline, switch, terminal, try
limits       {"counters": {"seconds": 30, "steps": 300, …}}
```

### A credential, if your backend wants one

It is the same credential the editor carries, in the same header, and it is not
this tool's to issue: the core has no idea what a token is
([Who is calling](backend/5-tenants.md)), so you decide what one looks like.

```bash
stageflow-mcp --backend https://… --token "$TOKEN"
stageflow-mcp --backend https://… --token "$TOKEN" --auth-header X-Api-Key
```

Without one, the caller is whatever your backend calls an anonymous visitor.

With no `--backend` at all, the first tool that needs an address asks for one
through the MCP client's own prompt and remembers the answer. A credential is
never written to disk and there is deliberately no tool that accepts one: a
tool argument is written by the model, which puts it in the conversation and in
that client's logs.

## What an assistant is given

| Tool | |
|---|---|
| `validate_pipeline` | every violation at once — schema, graph, declared types, and what the caller's plan refuses. **Costs no run.** |
| `run_pipeline` | runs it and reports the frame at the end, the meters against your ceilings, and the path of nodes it took |
| `stop_run` | stops one that is still going |

| Resource | |
|---|---|
| `stageflow://guide` | how a graph is written: the frame, the node types, expressions |
| `stageflow://stages` | your stages, a line each |
| `stageflow://stages/{name}` | one stage in full — types, optionality, what each argument means |
| `stageflow://capabilities` | node types, plan and ceilings, from `/api/meta` |
| `stageflow://schema` | the pipeline JSON Schema, with your stage names and node types as enums |

The working method the server asks for is write, `validate_pipeline`, fix
everything it listed, repeat — and only then run. Checking is free, so there is
no reason to spend a run finding out that a field name was wrong.

### How checking costs no run

There is no `/api/validate` in the contract and none is asked for.
`POST /api/run` with `mode: "step"` parses the graph and validates it against
the caller's policy **before** admitting a run — both reference backends do it
in that order, before a thread, a slot or an id exists. An invalid graph is
refused for free; a valid one comes back parked before its first node, having
executed nothing, and is stopped immediately.

So a check briefly occupies one of the caller's run slots and executes nothing.
If your backend admits runs before validating them, that is worth knowing about
for other reasons too.

## What you may want to do about it

Nothing is required. Two things are worth having.

**Answer in the caller's language.** The assistant passes `Accept-Language`;
without this, the core's own messages come back in the language they were
written in ([Localization](localization.md)):

```python
from stageflow import i18n

asked = request.headers.get("accept-language", "")
with i18n.use_locale(i18n.negotiate(i18n.parse_accept_language(asked))):
    ...
```

**Keep the core current.** A refusal names the node it belongs to and lists
every problem at once ([Errors](errors.md#both-halves-of-a-refusal-name-the-place)),
which is the difference between a list an assistant works through and a
sentence it has to decode:

```
guard.except[0]: 'next' is a required property;
risky.arguments: Additional properties are not allowed ('oops' was unexpected)
```

## The bridge to an open editor

```bash
stageflow-mcp --backend https://… --bridge
```

prints a link. Open it, and the
[editor](https://github.com/leo-need-more-coffee/stageflow-ui) is looking at the
same graph the assistant is: what it draws lands on the canvas, what you change
there is read back. An incoming graph keeps your view and the placement of
every card you have dragged, marks what it touched, and waits to be kept or put
back.

The bridge is a loopback socket on the machine the assistant runs on, under a
token, and your backend is not involved — beyond allowing the editor's origin,
which it already does if anybody uses the editor. One browser rule is worth
knowing: a page served from the internet reaching `127.0.0.1` is gated behind a
permission prompt in current Chrome. Allow it, or serve the editor from the
same machine (`--editor http://127.0.0.1:8080/`) and nothing crosses anything.

## What it does not do

- **Inputs.** A stage can await input in the core, but the HTTP contract has no
  channel for it — the editor has none either — so a graph that waits will time
  out ([Session control](session-control.md)).
- **Step debugging.** Stepping exists so a person can watch a graph go by; an
  assistant that is not watching would only be holding a slot.
- **Anything local.** Nothing is validated or executed in that process. The
  semantics live in the core your backend runs, and a second opinion would be a
  second implementation to drift from it.
