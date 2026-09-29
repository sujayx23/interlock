# interlock

**Leaderless, crash-resumable DAG task execution on SQLite.**

Any worker in a pool can claim any ready task from any run — no leader, no
single-owner run, no coordinator process. Every write is fenced by the epoch
the worker claimed with, so a crashed or reclaimed worker's stale write is
provably rejected, not just assumed safe.

**Early prototype.** Two of the highest-risk correctness properties are built
and tested; the rest is in progress. Don't use this for anything real yet.

## Why

Two existing projects each solve half of this:

- [Ferry](https://github.com/Sanjays2402/ferry) is a SQLite-backed distributed
  task queue — any worker can atomically claim any independent task — but has
  no concept of a dependency graph.
- [Retrace](https://github.com/Sanjays2402/retrace) is a SQLite-backed
  crash-resumable DAG workflow engine — but only the single process that
  claims a *run's* lease executes that run's tasks; there's no pool of
  workers pulling individual tasks off a shared queue.

interlock is the combination neither does alone: a crash-resumable DAG whose
individual tasks — not whole runs — are claimed and executed by any worker in
a shared pool.

## The core mechanism

A claim is a fence bump in one atomic statement:

```sql
UPDATE tasks SET status='claimed', worker_id=?, epoch=epoch+1, lease_expires_at=?
WHERE id = (SELECT id FROM tasks WHERE status='ready' ORDER BY created_at ASC LIMIT 1)
RETURNING *
```

Completion is fenced against the epoch the worker claimed with:

```sql
UPDATE tasks SET status='done', output=? WHERE id=? AND epoch=?
```

A worker whose lease already expired and got reclaimed writes against a
stale epoch and is silently rejected — `rowcount == 0`. This is the entire
correctness mechanism the project is built on.

## What's proven so far

- **`tests/test_claim_race.py`** — many real OS threads, each with an
  independent SQLite connection, racing to claim from the same ready pool.
  Asserts no task is ever claimed twice, and every task is eventually
  claimed by exactly one worker. Includes a direct check that epoch values
  are strictly increasing across reclaims and that a stale-epoch completion
  write is rejected.
- **`tests/test_crash_recovery.py`** — the single most important test in the
  project. Launches a real worker subprocess, waits until it's observably
  `running` a task, sends a genuine `SIGKILL` (not a graceful shutdown), then
  lets a second, independent worker reclaim the abandoned lease once it
  expires. Asserts the DAG still completes, with output matching a
  no-failure baseline exactly.
- **`tests/test_heartbeat.py`** — a second worker actively tries to steal a
  still-running task every 50ms for its full duration. Proves a task that
  legitimately runs longer than its lease TTL is never wrongly reclaimed
  while its owner is alive and heartbeating. (This test caught a real bug
  during development — a heartbeat thread sharing its parent's SQLite
  connection across threads, which silently failed every call. Fixed by
  giving the heartbeat thread its own connection.)

## Not yet built

- A proper `Workflow`/`Task` definition API (DAGs are currently built from
  raw dicts passed to `Store.create_run`)
- Retry backoff beyond immediate re-ready
- A benchmark against a real baseline (Celery+Redis or Airflow's local
  executor), reported honestly including where this loses
- CLI, dashboard/inspector

## Task execution model

Subprocess-based, language-agnostic: a worker spawns the task's declared
`command`, writes upstream dependencies' outputs (JSON) to stdin, reads the
result from stdout. Exit 0 = success. This has real overhead (process spawn
per task) compared to in-process execution — a deliberate tradeoff to be
measured honestly in the benchmark, not hidden.

## Development

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

## License

MIT
