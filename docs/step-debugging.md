# Step debugging

`Session` accepts a debugger that is given control before and after every
node. The in-core implementation is `StepDebugger`.

```python
from stageflow import Pipeline, Session, StepDebugger

debugger = StepDebugger(mode="step", delay=0.0, on_event=print)
session = Session("s1", Pipeline.from_dict(data), debugger=debugger)
task = asyncio.create_task(session.run())   # stops before the first node

debugger.step()                  # let one node run, then stop again
debugger.set_vars({"n": 42})     # applied before the next node
debugger.set_delay(0.5)          # run on its own, pausing between nodes
debugger.resume()                # continue without stopping
result = await task
```

Available from outside: the stop point (`debugger.node`), the frame at that
point (`debugger.vars`), and the `on_event` stream: `node_enter`, `node_exit`,
`paused`, `var_set`, `var_rejected`. Frame edits are checked against declared
types — a mismatch is rejected with an event rather than crashing the session.

The debugger also applies inside a `try` body, inside `parallel` branches and
inside a subpipeline: every node goes through `Session.execute_node`, and a
child session inherits the debugger. Commands are thread-safe.

The [editor](tutorial/7-debugger.md) is a front end for exactly this: it stops
between nodes, shows the frame on the left and the event stream on the right.

![The debugger stopped before a node](img/debug-paused.png)

![The events of a run](img/debug-events.png)

