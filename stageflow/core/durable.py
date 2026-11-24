"""Checkpoints: what lets a run outlive the process that started it.

A run given a `CheckpointStore` is saved when it reaches a `wait` node that
has no input yet, and `Session.run()` returns at once with
`{"status": "waiting", ...}`. Days later, `Session.resume(store, run_id,
input=...)` loads it in whatever process is running then and carries on.

A checkpoint is the session's snapshot (frame, position, meters, inputs, the
pipeline itself) plus a little bookkeeping: a version for optimistic
locking, a state and a deadline. Every save names the version it expects to
replace, so two processes resuming the same run cannot both succeed: the
second gets `CheckpointConflict` before it runs anything.

Two stores come with StageFlow: `MemoryStore` (tests, one process) and
`SQLiteStore` (one machine, standard library only). Anything else, such as
Postgres or Redis, is a class with the four methods of `CheckpointStore`.
"""
from __future__ import annotations

import json
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Iterator, Protocol

from ..exceptions import StageFlowError

#: A run that is executing somewhere right now.
RUNNING = "running"
#: Saved at a `wait` node, waiting for an input or its deadline.
WAITING = "waiting"
#: Ended: a terminal, a stop, a budget ceiling, or nothing left to run.
FINISHED = "finished"
#: Ended by an exception nothing caught.
FAILED = "failed"


class CheckpointError(StageFlowError):
    """A run cannot be resumed: unknown, already over, or not saved."""


class CheckpointConflict(CheckpointError):
    """Someone else saved the run first: it is running or was resumed
    elsewhere. Nothing was run."""


class RunSuspended(BaseException):
    """Raised by a `wait` node to save the run and return from `run()`.

    A BaseException, like `BudgetExceeded`, so that nothing on the way out
    (a stage's `except Exception`, a `try` node) can catch it.
    """

    def __init__(self, node_id: str, input_type: str, deadline: float | None):
        super().__init__(node_id)
        self.node_id = node_id
        self.input_type = input_type
        self.deadline = deadline

    def as_result(self) -> dict:
        return {"status": WAITING, "node": self.node_id, "input": self.input_type,
                "deadline": self.deadline}


@dataclass(frozen=True, slots=True)
class Checkpoint:
    run_id: str
    version: int
    state: str
    snapshot: dict[str, Any]
    #: When a waiting run's timeout passes, as a Unix time; None for no
    #: timeout or a run that is not waiting.
    deadline: float | None = None
    updated_at: float = 0.0

    @property
    def waiting_for(self) -> dict | None:
        """What a waiting run is waiting for: `{"node", "input", "deadline"}`."""
        return self.snapshot.get("waiting") if self.state == WAITING else None


class CheckpointStore(Protocol):
    """Where checkpoints are kept. All methods are coroutines, so a store
    may use an async driver; the bundled ones are fast enough to block."""

    async def load(self, run_id: str) -> Checkpoint | None:
        """The run's checkpoint, or None if it was never saved."""

    async def save(self, checkpoint: Checkpoint, expected: int | None) -> Checkpoint:
        """Save `checkpoint` if the stored version is still `expected` (None:
        the run must not exist yet). Return what was stored, with the next
        version. Raise `CheckpointConflict` otherwise."""

    async def due(self, now: float | None = None) -> list[str]:
        """Waiting runs whose deadline has passed, oldest deadline first."""

    async def delete(self, run_id: str) -> None:
        """Forget a run. Deleting an unknown run is not an error."""


def _stamped(checkpoint: Checkpoint, version: int) -> Checkpoint:
    return replace(checkpoint, version=version, updated_at=time.time())


class MemoryStore:
    """Checkpoints in a dict: for tests, and for a single process that only
    wants `wait` nodes to free it while nobody answers."""

    def __init__(self) -> None:
        self._runs: dict[str, Checkpoint] = {}
        # a thread lock: one store may serve runs on several event loops
        self._lock = threading.Lock()

    async def load(self, run_id: str) -> Checkpoint | None:
        return self._runs.get(run_id)

    async def save(self, checkpoint: Checkpoint, expected: int | None) -> Checkpoint:
        with self._lock:
            current = self._runs.get(checkpoint.run_id)
            found = current.version if current else None
            if found != expected:
                raise CheckpointConflict(_conflict(checkpoint.run_id, expected, found))
            stored = _stamped(checkpoint, (found or 0) + 1)
            # A deep copy through JSON: what is stored must not change when
            # the session goes on, and must be JSON, as in any real store.
            stored = replace(stored, snapshot=json.loads(json.dumps(stored.snapshot)))
            self._runs[checkpoint.run_id] = stored
            return stored

    async def due(self, now: float | None = None) -> list[str]:
        now = time.time() if now is None else now
        ready = [c for c in self._runs.values()
                 if c.state == WAITING and c.deadline is not None and c.deadline <= now]
        return [c.run_id for c in sorted(ready, key=lambda c: c.deadline)]

    async def delete(self, run_id: str) -> None:
        self._runs.pop(run_id, None)


class SQLiteStore:
    """Checkpoints in an SQLite file. Safe for several processes on one
    machine: the version check is a single `UPDATE ... WHERE version = ?`."""

    _SCHEMA = """
        CREATE TABLE IF NOT EXISTS stageflow_checkpoints (
            run_id     TEXT PRIMARY KEY,
            version    INTEGER NOT NULL,
            state      TEXT NOT NULL,
            deadline   REAL,
            updated_at REAL NOT NULL,
            snapshot   TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS stageflow_checkpoints_due
            ON stageflow_checkpoints (state, deadline);
    """

    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        with self._connect() as db:
            db.executescript(self._SCHEMA)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        # A connection per call: cheap for SQLite, and safe from any thread.
        # Autocommit: every statement is its own transaction.
        db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        try:
            db.execute("PRAGMA journal_mode=WAL")
            yield db
        finally:
            db.close()

    async def load(self, run_id: str) -> Checkpoint | None:
        with self._connect() as db:
            row = db.execute(
                "SELECT run_id, version, state, snapshot, deadline, updated_at "
                "FROM stageflow_checkpoints WHERE run_id = ?", (run_id,)).fetchone()
        if row is None:
            return None
        return Checkpoint(run_id=row[0], version=row[1], state=row[2],
                          snapshot=json.loads(row[3]), deadline=row[4], updated_at=row[5])

    async def save(self, checkpoint: Checkpoint, expected: int | None) -> Checkpoint:
        stored = _stamped(checkpoint, (expected or 0) + 1)
        values = (stored.version, stored.state, stored.deadline, stored.updated_at,
                  json.dumps(stored.snapshot, ensure_ascii=False))
        with self._connect() as db:
            if expected is None:
                try:
                    db.execute(
                        "INSERT INTO stageflow_checkpoints "
                        "(version, state, deadline, updated_at, snapshot, run_id) "
                        "VALUES (?, ?, ?, ?, ?, ?)", values + (stored.run_id,))
                except sqlite3.IntegrityError:
                    found = db.execute("SELECT version FROM stageflow_checkpoints "
                                       "WHERE run_id = ?", (stored.run_id,)).fetchone()
                    raise CheckpointConflict(
                        _conflict(stored.run_id, None, found[0] if found else None)) from None
            else:
                updated = db.execute(
                    "UPDATE stageflow_checkpoints SET version = ?, state = ?, deadline = ?, "
                    "updated_at = ?, snapshot = ? WHERE run_id = ? AND version = ?",
                    values + (stored.run_id, expected)).rowcount
                if updated != 1:
                    found = db.execute("SELECT version FROM stageflow_checkpoints "
                                       "WHERE run_id = ?", (stored.run_id,)).fetchone()
                    raise CheckpointConflict(
                        _conflict(stored.run_id, expected, found[0] if found else None))
        return stored

    async def due(self, now: float | None = None) -> list[str]:
        now = time.time() if now is None else now
        with self._connect() as db:
            rows = db.execute(
                "SELECT run_id FROM stageflow_checkpoints WHERE state = ? "
                "AND deadline IS NOT NULL AND deadline <= ? ORDER BY deadline",
                (WAITING, now)).fetchall()
        return [row[0] for row in rows]

    async def delete(self, run_id: str) -> None:
        with self._connect() as db:
            db.execute("DELETE FROM stageflow_checkpoints WHERE run_id = ?", (run_id,))


def _conflict(run_id: str, expected: int | None, found: int | None) -> str:
    # Messages for developers operating a store, like the testing helpers:
    # not translated.
    if expected is None:
        return f"run '{run_id}' already exists (version {found})"
    if found is None:
        return f"run '{run_id}' is gone (expected version {expected})"
    return (f"run '{run_id}' was saved by someone else: "
            f"expected version {expected}, found {found}")


_DURATION = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([smhd])\s*$")
_UNIT = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_duration(value: Any) -> float:
    """Seconds from a number or a string such as '90s', '15m', '2h', '7d'."""
    if isinstance(value, bool):
        raise ValueError(f"not a duration: {value!r}")
    if isinstance(value, (int, float)):
        if value <= 0:
            raise ValueError(f"a duration must be positive: {value!r}")
        return float(value)
    match = _DURATION.match(value) if isinstance(value, str) else None
    if not match:
        raise ValueError(f"not a duration: {value!r}")
    return float(match.group(1)) * _UNIT[match.group(2)]
