# The subpipeline node

A whole graph behind one card. The child runs in its own session with a
**fresh frame**: nothing crosses the boundary that was not named.

```json
{
  "id": "write_reply",
  "type": "subpipeline",
  "subpipeline_id": "reply",
  "inputs": { "topic": "topic", "text": "ticket_text" },
  "artifact_outputs": { "reply": "answer" },
  "result_output": "reply_status",
  "next": "send"
}
```

The child graph is declared in the header of the pipeline, next to `nodes`:

```json
{
  "entry": "start",
  "nodes": [ … ],
  "subpipelines": {
    "reply": {
      "entry": "compose",
      "nodes": [
        { "id": "compose", "type": "stage", "stage": "TemplateStage",
          "arguments": { "const": { "template": "About {topic}: we are on it" },
                         "vars": { "topic": "topic" } },
          "outputs": { "value": "answer" }, "next": "done" },
        { "id": "done", "type": "terminal",
          "result": { "status": "written" }, "artifacts": ["answer"] }
      ]
    }
  }
}
```

![A subpipeline node and the graph behind it](img/tut-subpipeline.png){ width="657" }

## What crosses the boundary

| Field | Direction | Reads as |
|---|---|---|
| `inputs` | in | `{ "name inside the child": "variable of the parent" }` |
| `artifact_outputs` | out | `{ "variable of the parent": "artifact of the child" }` |
| `result_output` | out | one variable ← the `result` of the child's terminal |

Note that the two directions are written the other way round from each other:
in both cases the **child's** name is the one next to the word `child` in the
table — `inputs` is keyed by it, `artifact_outputs` is valued by it. The rule
behind it is that the parent's variable is always on the side the value moves
towards.

Everything else stays where it is. The child cannot see the parent's frame,
and a write inside the child is invisible outside unless its terminal exports
it as an artifact.

## The rest of the contract

- An artifact asked for and not returned is an `ArtifactNotFoundError` naming
  what the child did return — like a missing stage output, it fails at the
  cause.
- [Type declarations](variable-typing.md) of the parent are inherited by a
  child that declares none of its own, so `ticket_id` means the same thing on
  both sides. The `subpipelines` map is passed down as well: a child may use
  a subpipeline node itself.
- The child's events reach the parent's stream, each tagged with
  `subpipeline_node: "<id>"`, and the session id is `parent:node` — a nested
  run is readable in the log without being confused with the outer one.
- The [debugger](step-debugging.md) is inherited too: stepping walks into the
  child node by node, it is not one opaque jump.
- `retry` on the node repeats the whole child run.
- The id must be a key of `subpipelines`, and it may not be the id of the
  root entry node — validation says so before the run.

!!! warning "Recursion is not guarded"
    Because the child inherits the same `subpipelines` map, a subpipeline that
    references itself will keep opening child sessions until the process runs
    out of stack. There is no depth limit in the core: a recursive graph needs
    its own stopping condition — a `condition` on a depth variable passed
    through `inputs`.
