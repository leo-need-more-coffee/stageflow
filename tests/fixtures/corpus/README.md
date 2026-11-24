# The pipeline corpus

One file, one pipeline, and the runs it must survive. `tests/test_system_corpus.py`
runs every file through the public API — `Pipeline.from_dict`, `validate`,
`Session` — and compares what comes out. A new case is a new file.

```json
{
  "description": "what this pipeline is about",
  "pipeline": { "nodes": [ ... ], "subpipelines": { ... } },
  "runs": [
    {
      "name": "what this run checks",
      "vars":   { "seed": 1 },
      "policy": { "stages": [...], "node_types": [...], "allow_cycles": true,
                  "limits": { "counters": {}, "gauges": {}, "max_retries": 2 } },
      "inputs": [ { "type": "answer", "payload": { "text": "hi" } } ],
      "expect": { ... }
    }
  ]
}
```

`vars`, `policy` and `inputs` are optional. `inputs` are given in order, each
as soon as the run waits for that type. With `"durable": true` the run has a
checkpoint store: each time it stops at a `wait` node it is restored as a new
session, as another process would, and given the next input.

`expect` takes any of:

| Key | Means |
|---|---|
| `waiting_at` | the run ended saved at this `wait` node (durable runs) |
| `invalid` | `validate(policy)` refuses the graph; every fragment is in the message |
| `error` | the run raises `{"type": "ClassName", "match": "regex"}` |
| `result` | the terminal's `result`, exactly (`null` for a road that just ended); for a budget result only the keys given are compared |
| `artifacts` | all the artifacts, exactly |
| `artifacts_include` | these artifacts have these values |
| `artifacts_match` | these artifacts, as text, match these regexes |
| `meters` / `meters_at_most` | meters equal to / not above these numbers |
| `events` | `include` and `exclude` event types, `count` of each type |

The stages are the built-in ones plus four defined in the test module:
`CorpusFlaky` (fails the first N calls per key), `CorpusSpend` (reserves and
charges `tokens`), `CorpusAsk` (waits for an `answer` input), `CorpusBoom`
(raises `ValueError` on a negative number).

```bash
python -m unittest tests/test_system_corpus.py               # all
CORPUS=07 python -m unittest tests/test_system_corpus.py     # names containing "07"
```

Together the files must use every node type and every built-in stage; the
test says which one went missing if a change drops it.
