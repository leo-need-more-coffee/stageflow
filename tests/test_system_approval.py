"""System: a host running a real pipeline, across process boundaries.

Nothing here reaches inside the framework. A separate Python process — the
host in `fixtures/approval/driver.py` — loads a pipeline from a JSON file,
validates it against its policy, runs it with its own stages, and when the
run reaches its `wait` node with nobody to answer, saves it in an SQLite file
and exits. A second process restores the run, gives the answer and finishes
it. The tests only see what a
host sees: what was printed and what was written.

The pipeline uses every node type, so a run end to end is the whole
language at once rather than one feature at a time.
"""
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "approval"


def host(*args: str) -> dict:
    done = subprocess.run(
        [sys.executable, str(FIXTURE / "driver.py"), *args],
        capture_output=True, text=True, timeout=60,
    )
    if done.returncode != 0:
        raise AssertionError(f"the host failed:\n{done.stderr}")
    return json.loads(done.stdout.strip().splitlines()[-1])


class ApprovalSystemTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.dir = Path(self._dir.name)
        self.state = str(self.dir / "runs.db")

    def tearDown(self):
        self._dir.cleanup()

    def submit(self, lines) -> dict:
        path = self.dir / "lines.json"
        path.write_text(json.dumps(lines))
        return host("run", str(path), self.state)

    def test_the_pipeline_uses_every_node_type(self):
        from stageflow import get_node_types
        data = json.loads((FIXTURE / "pipeline.json").read_text())
        used = {n["type"] for n in data["nodes"]}
        for sub in data["subpipelines"].values():
            used |= {n["type"] for n in sub["nodes"]}
        self.assertEqual(used, set(get_node_types()))

    def test_small_lines_are_approved_by_the_rules(self):
        out = self.submit([{"what": "taxi", "amount": 30},
                           {"what": "lunch", "amount": 25}])
        self.assertEqual(out["result"], {"status": "approved", "by": "rules"})
        self.assertEqual(out["artifacts"], {"total": 55, "verdicts": ["ok", "ok"]})
        self.assertEqual(out["meters"]["checks"], 2, "one reserved check per line")
        self.assertEqual(out["status"], "finished", "nobody was asked")
        self.assertNotIn("waiting_started", out["events"])

    def test_a_high_line_waits_for_a_person_across_a_restart(self):
        lines = [{"what": "laptop", "amount": 1800}, {"what": "bag", "amount": 60}]
        first = self.submit(lines)
        self.assertEqual(first["status"], "waiting")
        self.assertEqual(first["at"], "ask")
        self.assertTrue(Path(self.state).exists())

        second = host("resume", self.state, first["run"], "approve")
        self.assertEqual(second["result"], {"status": "approved", "by": "person"})
        self.assertEqual(second["artifacts"]["told"],
                         ["told manager", "told finance", "told requester"])
        self.assertEqual(second["artifacts"]["verdicts"], ["high", "ok"])
        self.assertEqual(second["artifacts"]["decision"], "approve")
        # spending carried over from the first process instead of restarting
        self.assertGreater(second["meters"]["steps"], first["meters"]["steps"])
        self.assertEqual(second["meters"]["messages"], 3)
        # the run is over: a second answer has nothing to continue
        with self.assertRaisesRegex(AssertionError, "already finished"):
            host("resume", self.state, first["run"], "reject")

    def test_a_person_can_say_no(self):
        first = self.submit([{"what": "chair", "amount": 1200}])
        out = host("resume", self.state, first["run"], "reject")
        self.assertEqual(out["result"], {"status": "rejected", "why": "person"})

    def test_a_bad_line_is_caught_and_reported(self):
        out = self.submit([{"what": "refund", "amount": -10},
                           {"what": "taxi", "amount": 20}])
        self.assertEqual(out["result"], {"status": "invalid"})
        self.assertEqual(out["artifacts"]["problem"]["type"], "ValueError")
        self.assertIn("refund", out["artifacts"]["problem"]["message"])

    def test_the_early_exits(self):
        self.assertEqual(self.submit([])["result"], {"status": "nothing to approve"})
        out = self.submit([{"what": "car", "amount": 9000}])
        self.assertEqual(out["result"], {"status": "rejected", "why": "over 5000"})

    def test_the_policy_ceiling_holds_end_to_end(self):
        # twenty-five lines want twenty-five checks; the host allows twenty
        out = self.submit([{"what": f"item {i}", "amount": 1} for i in range(25)])
        self.assertEqual(out["result"]["status"], "budget_exceeded")
        self.assertEqual(out["result"]["meter"], "checks")


class ReadmeExampleTests(unittest.TestCase):
    def test_the_readme_example_runs_as_printed(self):
        readme = (Path(__file__).resolve().parents[1] / "README.md").read_text("utf-8")
        start = readme.index("## A pipeline in 30 seconds")
        code = readme[start:].split("```python", 1)[1].split("```", 1)[0]
        done = subprocess.run([sys.executable, "-c", code],
                              capture_output=True, text=True, timeout=60)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertEqual(done.stdout.splitlines(),
                         ["{'status': 'ok'}", "{'greeting': 'Hello, Alice!'}"])


if __name__ == "__main__":
    unittest.main()
