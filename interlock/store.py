"""SQLite-backed task store: schema, atomic claim/fence, and ready propagation.

This is the entire correctness surface of the engine. Everything else (the
scheduler, the subprocess worker) is a client of this module and trusts it
to make the following true no matter how many workers race against it:

- A task is claimed by at most one worker at a time (claim() is one atomic
  UPDATE...RETURNING; SQLite serializes writers, so two concurrent claims
  can never both match the same row).
- A worker that held a task's lease past its expiry, and had that lease
  reclaimed by another worker, can never have its late completion write
  accepted — complete()/fail() are fenced on the epoch the worker claimed
  with, and epoch only advances at claim time.
- Reclaiming an expired lease (crash recovery) does NOT bump epoch itself;
  it only flips a stale claimed/running row back to ready. The NEXT claim()
  bumps epoch, which is what actually fences the zombie worker's write.
"""

from __future__ import annotations

import json
import random
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id          TEXT PRIMARY KEY,
    status      TEXT NOT NULL DEFAULT 'running',   -- running, succeeded, failed
    created_at  REAL NOT NULL,
    finished_at REAL
);

CREATE TABLE IF NOT EXISTS tasks (
    id                TEXT PRIMARY KEY,
    run_id            TEXT NOT NULL REFERENCES runs(id),
    name              TEXT NOT NULL,
    command           TEXT NOT NULL,                 -- JSON list, e.g. ["python3","step.py"]
    needs             TEXT NOT NULL DEFAULT '[]',     -- JSON list of task ids this depends on
    status            TEXT NOT NULL DEFAULT 'pending',-- pending/ready/claimed/running/done/failed/blocked/dead
    epoch             INTEGER NOT NULL DEFAULT 0,
    worker_id         TEXT,
    lease_expires_at  REAL,
    idempotency_key   TEXT NOT NULL,
    attempts          INTEGER NOT NULL DEFAULT 0,
    max_retries       INTEGER NOT NULL DEFAULT 3,
    not_before        REAL,                          -- ready tasks aren't claimable until this time (backoff)
    output            TEXT,
    error             TEXT,
    created_at        REAL NOT NULL,
    claimed_at        REAL,
    finished_at       REAL,
    UNIQUE(run_id, name)
);

CREATE INDEX IF NOT EXISTS idx_tasks_claimable ON tasks (status, not_before, created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_run ON tasks (run_id, status);
CREATE INDEX IF NOT EXISTS idx_tasks_expiry ON tasks (status, lease_expires_at);
"""


def _backoff_delay(attempt: int, base: float = 1.0, cap: float = 60.0) -> float:
    """Full-jitter exponential backoff: uniform(0, min(cap, base * 2**attempt)).
    Jitter (not just exponential growth) is what actually prevents a
    thundering-herd reclaim when many tasks fail around the same moment —
    without it every task failing at t=0 would all become ready again at
    the exact same t=base*2**attempt and re-collide on claim_next()."""
    ceiling = min(cap, base * (2 ** attempt))
    return random.uniform(0, ceiling)


@dataclass(frozen=True)
class TaskLease:
    """What a worker holds after a successful claim(). ``epoch`` is the
    fencing token every subsequent write for this task must present."""
    task_id: str
    run_id: str
    name: str
    command: list
    epoch: int
    inputs: dict  # name -> output, for every task in `needs`


class Store:
    """One SQLite connection wrapper. Not thread-safe across threads sharing
    one instance — open one Store per worker process/connection, same as
    Ferry's and Retrace's own per-thread/per-process connection pattern."""

    def __init__(self, path: str | Path = "interlock.db"):
        self.path = str(path)
        self._conn = sqlite3.connect(self.path, timeout=30.0, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=30000")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "Store":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # -- DAG creation ---------------------------------------------------------
    def create_run(self, run_id: str, tasks: list[dict]) -> None:
        """``tasks``: list of {"name", "command", "needs": [names], "max_retries"?}.
        Tasks with no needs start ``ready``; everything else starts ``pending``
        and is promoted by propagate_ready() as dependencies finish."""
        now = time.time()
        with self._tx():
            self._conn.execute(
                "INSERT INTO runs (id, status, created_at) VALUES (?, 'running', ?)",
                (run_id, now),
            )
            name_to_id = {t["name"]: f"{run_id}:{t['name']}" for t in tasks}
            for t in tasks:
                needs_ids = [name_to_id[n] for n in t.get("needs", [])]
                status = "ready" if not needs_ids else "pending"
                task_id = name_to_id[t["name"]]
                self._conn.execute(
                    """INSERT INTO tasks
                       (id, run_id, name, command, needs, status,
                        idempotency_key, max_retries, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        task_id, run_id, t["name"], json.dumps(t["command"]),
                        json.dumps(needs_ids), status,
                        f"{task_id}:complete", t.get("max_retries", 3), now,
                    ),
                )

    # -- claim / fence ----------------------------------------------------------
    def reclaim_expired_leases(self, now: float | None = None) -> int:
        """Crash recovery sweep: a claimed/running task whose lease expired
        (its worker died or hung) goes back to `ready`. Does NOT touch epoch —
        the zombie worker's fencing token stays valid-looking to itself, but
        the NEXT claim() bumps epoch past it, which is what actually rejects
        a late write. Safe to call from any worker, any time; idempotent."""
        now = now if now is not None else time.time()
        cur = self._conn.execute(
            """UPDATE tasks SET status='ready', worker_id=NULL, lease_expires_at=NULL
               WHERE status IN ('claimed','running') AND lease_expires_at < ?""",
            (now,),
        )
        return cur.rowcount

    def claim_next(self, worker_id: str, lease_ttl: float, now: float | None = None) -> TaskLease | None:
        """Atomically claim the oldest `ready` task, if any. One UPDATE with a
        correlated subquery to pick the row: SQLite serializes writers, so two
        concurrent claim_next() calls can never both match the same task id —
        the second one's subquery re-evaluates against the first one's write."""
        now = now if now is not None else time.time()
        expires_at = now + lease_ttl
        row = self._conn.execute(
            """UPDATE tasks
               SET status='claimed', worker_id=?, epoch=epoch+1,
                   lease_expires_at=?, claimed_at=?
               WHERE id = (
                   SELECT id FROM tasks
                   WHERE status='ready' AND (not_before IS NULL OR not_before <= ?)
                   ORDER BY created_at ASC
                   LIMIT 1
               )
               RETURNING *""",
            (worker_id, expires_at, now, now),
        ).fetchone()
        if row is None:
            return None
        needs = json.loads(row["needs"])
        inputs = {}
        for dep_id in needs:
            dep = self._conn.execute("SELECT name, output FROM tasks WHERE id=?", (dep_id,)).fetchone()
            inputs[dep["name"]] = json.loads(dep["output"]) if dep["output"] else None
        return TaskLease(
            task_id=row["id"], run_id=row["run_id"], name=row["name"],
            command=json.loads(row["command"]), epoch=row["epoch"], inputs=inputs,
        )

    def mark_running(self, lease: TaskLease) -> bool:
        """claimed -> running, fenced. False means the lease was already lost
        (reclaimed+re-claimed by someone else) before execution even started."""
        cur = self._conn.execute(
            "UPDATE tasks SET status='running' WHERE id=? AND epoch=? AND status='claimed'",
            (lease.task_id, lease.epoch),
        )
        return cur.rowcount > 0

    def heartbeat(self, lease: TaskLease, lease_ttl: float, now: float | None = None) -> bool:
        """Extend the lease. Fenced: a worker that already lost ownership
        cannot resurrect it by heartbeating (rowcount 0 -> caller should stop)."""
        now = now if now is not None else time.time()
        cur = self._conn.execute(
            "UPDATE tasks SET lease_expires_at=? WHERE id=? AND epoch=? AND status='running'",
            (now + lease_ttl, lease.task_id, lease.epoch),
        )
        return cur.rowcount > 0

    def complete(self, lease: TaskLease, output) -> bool:
        """Fenced completion — the core correctness mechanism of the project.
        A worker whose lease expired and was reclaimed writes against a stale
        epoch and this returns False; the caller MUST discard its result
        rather than treat False as an error to retry."""
        with self._tx():
            cur = self._conn.execute(
                """UPDATE tasks SET status='done', output=?, finished_at=?,
                       worker_id=NULL, lease_expires_at=NULL
                   WHERE id=? AND epoch=?""",
                (json.dumps(output), time.time(), lease.task_id, lease.epoch),
            )
            accepted = cur.rowcount > 0
            if accepted:
                self._propagate_ready(lease.run_id)
                self._maybe_finish_run(lease.run_id)
        return accepted

    def fail(
        self,
        lease: TaskLease,
        error: str,
        now: float | None = None,
        backoff_base: float = 1.0,
        backoff_cap: float = 60.0,
    ) -> bool:
        """Fenced failure. Increments `attempts`; if that reaches the task's
        `max_retries`, transitions to terminal 'dead' (propagates 'blocked'
        to descendants, same as the old terminal 'failed' did). Otherwise
        requeues to 'ready' with `not_before` set via exponential backoff +
        full jitter, so claim_next() won't hand it back out immediately.

        The pre-check SELECT and the final UPDATE both filter on
        `id=? AND epoch=?`, inside one BEGIN IMMEDIATE transaction (so no
        other writer can interleave between them): a lease that's already
        been reclaimed matches neither, so this is a no-op and returns
        False, same fencing guarantee as complete()."""
        now = now if now is not None else time.time()
        with self._tx():
            row = self._conn.execute(
                "SELECT attempts, max_retries FROM tasks WHERE id=? AND epoch=?",
                (lease.task_id, lease.epoch),
            ).fetchone()
            if row is None:
                return False
            new_attempts = row["attempts"] + 1
            if new_attempts >= row["max_retries"]:
                cur = self._conn.execute(
                    """UPDATE tasks SET status='dead', error=?, attempts=?,
                           not_before=NULL, finished_at=?,
                           worker_id=NULL, lease_expires_at=NULL
                       WHERE id=? AND epoch=?""",
                    (error, new_attempts, now, lease.task_id, lease.epoch),
                )
                accepted = cur.rowcount > 0
                if accepted:
                    self._propagate_blocked(lease.run_id)
                    self._maybe_finish_run(lease.run_id)
            else:
                delay = _backoff_delay(new_attempts, backoff_base, backoff_cap)
                cur = self._conn.execute(
                    """UPDATE tasks SET status='ready', error=?, attempts=?,
                           not_before=?, worker_id=NULL, lease_expires_at=NULL
                       WHERE id=? AND epoch=?""",
                    (error, new_attempts, now + delay, lease.task_id, lease.epoch),
                )
                accepted = cur.rowcount > 0
        return accepted

    # -- ready/blocked propagation ---------------------------------------------
    def _propagate_ready(self, run_id: str) -> None:
        pending = self._conn.execute(
            "SELECT id, needs FROM tasks WHERE run_id=? AND status='pending'", (run_id,)
        ).fetchall()
        for row in pending:
            needs = json.loads(row["needs"])
            if not needs:
                continue
            placeholders = ",".join("?" for _ in needs)
            statuses = {
                r["id"]: r["status"]
                for r in self._conn.execute(
                    f"SELECT id, status FROM tasks WHERE id IN ({placeholders})", needs
                ).fetchall()
            }
            if all(statuses.get(n) == "done" for n in needs):
                self._conn.execute("UPDATE tasks SET status='ready' WHERE id=?", (row["id"],))

    def _propagate_blocked(self, run_id: str) -> None:
        """A failed (terminal) or already-blocked dependency blocks descendants,
        transitively — repeat until a pass makes no change."""
        changed = True
        while changed:
            changed = False
            pending = self._conn.execute(
                "SELECT id, needs FROM tasks WHERE run_id=? AND status='pending'", (run_id,)
            ).fetchall()
            for row in pending:
                needs = json.loads(row["needs"])
                if not needs:
                    continue
                placeholders = ",".join("?" for _ in needs)
                statuses = {
                    r["id"]: r["status"]
                    for r in self._conn.execute(
                        f"SELECT id, status FROM tasks WHERE id IN ({placeholders})", needs
                    ).fetchall()
                }
                if any(statuses.get(n) in ("failed", "blocked", "dead") for n in needs):
                    self._conn.execute("UPDATE tasks SET status='blocked' WHERE id=?", (row["id"],))
                    changed = True

    def _maybe_finish_run(self, run_id: str) -> None:
        rows = self._conn.execute("SELECT status FROM tasks WHERE run_id=?", (run_id,)).fetchall()
        statuses = [r["status"] for r in rows]
        if any(s in ("pending", "ready", "claimed", "running") for s in statuses):
            return
        final = "failed" if any(s in ("failed", "blocked", "dead") for s in statuses) else "succeeded"
        self._conn.execute(
            "UPDATE runs SET status=?, finished_at=? WHERE id=?",
            (final, time.time(), run_id),
        )

    # -- inspection --------------------------------------------------------------
    def get_task(self, task_id: str) -> dict | None:
        row = self._conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        return dict(row) if row else None

    def tasks_for_run(self, run_id: str) -> list[dict]:
        return [dict(r) for r in self._conn.execute(
            "SELECT * FROM tasks WHERE run_id=? ORDER BY name", (run_id,)
        ).fetchall()]

    def run_status(self, run_id: str) -> str:
        row = self._conn.execute("SELECT status FROM runs WHERE id=?", (run_id,)).fetchone()
        return row["status"]

    def list_runs(self) -> list[dict]:
        return [dict(r) for r in self._conn.execute(
            "SELECT id, status, created_at, finished_at FROM runs ORDER BY created_at DESC"
        ).fetchall()]

    def new_worker_id(self) -> str:
        return uuid.uuid4().hex

    # -- transactions --------------------------------------------------------------
    def _tx(self):
        return _ImmediateTx(self._conn)


class _ImmediateTx:
    """BEGIN IMMEDIATE context manager: takes the write lock up front instead
    of on first write, so two connections racing into the same multi-statement
    transaction fail fast on the second's BEGIN rather than deadlocking or
    interleaving. isolation_level=None on the connection means autocommit is
    off by default only inside this block."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def __enter__(self):
        self._conn.execute("BEGIN IMMEDIATE")
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self._conn.execute("COMMIT")
        else:
            self._conn.execute("ROLLBACK")
        return False
