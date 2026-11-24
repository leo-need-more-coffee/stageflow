# Testing

Two halves: testing **your** stages and pipelines with `stageflow.testing`,
and how StageFlow tests itself.

## Testing a stage

`run_stage` runs one stage the way the engine does — inside a real `Session`,
under its timeout, with the budget, the events and the inputs — and returns
what it produced:

```python
from stageflow.testing import run_stage

async def test_greeting():
    run = await run_stage("GreetStage", {"name": "Ann"})
    assert run.outputs == {"text": "Hello, Ann", "length": 10}
    assert run.emitted("greeted") == [{"to": "Ann"}]   # payloads of its events
    assert run.charged == {"greetings": 1}            # what it charge()d
```

It also holds the stage to the contract in its docstring, which nothing else
does at run time: an argument or output the spec does not declare, a required
one missing, or a value of the wrong type is a `ContractViolation` naming it.

```
ContractViolation: GreetStage: required output 'length' is missing
ContractViolation: GreetStage: output text: expected string, got int
```

The rest of `run_stage`:

| | |
|---|---|
| `inputs=[{"type": "approval", "payload": {...}}]` | queued before the stage starts; a stage that calls `wait_input` gets them at once |
| `limits=` / `policy=` | the real budget: `reserve`, `charge()` and ceilings behave as in production, and `run.meters` is the result |
| `check_contract=False` | skip the contract check |

A stage refused by the budget raises `BudgetExceeded`; an exception from the
stage propagates unchanged.

## Testing a pipeline

`run_pipeline_test` runs a whole pipeline, sends the inputs it is waiting for,
and compares the result, the artifacts and the order of nodes:

```python
from stageflow.testing import PipelineTestSpec, run_pipeline_test

result, events = await run_pipeline_test(PipelineTestSpec(
    pipeline=json.load(open("pipelines/approval.json")),
    payload={"amount": 120},
    inputs=[{"type": "approval", "payload": {"ok": True}}],
    expected_result={"status": "approved"},
    expected_history=["start", "check", "ask", "pay", "done"],
))
```

## Stages registered by a test

The stage registry is global, so a test that registers a stage in its body
fails the second time it runs in the same process — under a mutation tester,
a `--count` plugin, or in a REPL. `isolated_stages()` forgets everything
registered inside it:

```python
from stageflow.testing import isolated_stages

class MyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(isolated_stages())
```

## How StageFlow tests itself

```bash
pip install -e ".[test]"
python -m unittest discover -s tests          # ~615 tests, ~20 s
```

| kind | where | what it checks |
|---|---|---|
| unit | `tests/test_unit_*.py` | one class at a time, against a stub session (`tests/_support.py`) |
| integration | `tests/test_integration_*.py` | the walker, the debugger, errors and observability together |
| system | `tests/test_system_*.py` | a whole application (`fixtures/approval/`) and a corpus of 26 involved pipelines (`fixtures/corpus/`) |
| property | `tests/test_property.py` | Hypothesis generates graphs and documents; invariants must hold for all of them |

### Coverage

```bash
pip install coverage
coverage run --branch --source=stageflow -m unittest discover -s tests
coverage report
```

**95 %** of branches. The least covered modules are the stage node (85 %,
mostly error paths of argument binding) and the payload schema checker
(87 %).

### Mutation testing

Coverage says which lines ran, not whether a test would notice them being
wrong. Mutation testing checks that: [mutmut](https://mutmut.readthedocs.io)
makes thousands of small changes to the code — `<` into `<=`, `and` into
`or`, a constant into another — and runs the tests against each. A change the
tests do not notice (a *surviving* mutant) is either a missing test or code
that does not matter.

```bash
pip install mutmut pytest
mutmut run              # tens of minutes on all cores; config in pyproject.toml
mutmut results          # the survivors
mutmut show <name>      # the diff of one
```

Results for `stageflow/core` and `stageflow/builtins` (6 275 mutants):

| | mutants | |
|---|---|---|
| caught by a test (failed, hung or crashed) | 4 548 | 72 % of all |
| survived, but only the text of a message changed | 1 065 | |
| survived, behaviour changed | 601 | |
| no test runs that code at all | 61 | |

Messages are checked by meaning in a few places and by exact text in none, on
purpose — so of the mutants that change what the code **does**, the tests
catch **87 %**. The 601 behavioural survivors are mostly of three kinds:
equivalent mutants that cannot be told apart from the original (a default of
`getattr` that is never used, `round(x, 4)` versus `round(x, 5)`), fields of
events nobody asserts on (a `session_id` set to `None`), and real gaps.

The real gaps the first run found are closed: a condition inside a `try` whose
`else` leaves the block early made `reachable` stop too soon, and nothing
checked the block still covered the `then` branch (`test_try_block`); and a
skip request was never checked to be used up by the one stage it skipped
(`test_session_control`).

The property tests are left out of mutation runs: they draw random inputs,
so whether they kill a mutant would change from one run to the next.
