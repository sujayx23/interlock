# interlock

**Leaderless, crash-resumable DAG task execution on SQLite.** Any worker in
a pool can claim any ready task from any run — no leader, no single-owner
run, no coordinator process. Every write is fenced by the epoch the worker
claimed with, so a crashed or reclaimed worker's stale write is provably
rejected, not just assumed safe.

**Early prototype.** The core correctness properties, retry backoff, both
benchmarks, and a CLI are built and tested; a dashboard/inspector is not.
Don't use this for anything real yet.

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

## Quick example

```python
from interlock.store import Store
from interlock.workflow import Workflow

wf = (
    Workflow()
    .task("fetch", command=["python3", "fetch.py"])
    .task("transform", command=["python3", "transform.py"], needs=["fetch"])
    .task("write", command=["python3", "write.py"], needs=["transform"])
)

store = Store("pipeline.db")
store.create_run("run-1", wf.tasks())
```

Then run any number of workers, from any number of processes or machines,
against the same SQLite file:

```bash
python3 -m interlock.worker pipeline.db
```

`Workflow.validate()` catches an undefined dependency or a cycle at
definition time — before it can become a run that silently sits `pending`
forever.

## CLI

Thin wrappers over the same `Store`/`Worker`/`Workflow` APIs above — no
logic lives in the CLI beyond argument parsing, module loading, and output
formatting:

```bash
interlock submit pipeline.db workflow.py   # imports workflow.py's module-level
                                            # `workflow`, prints the new run id
interlock worker pipeline.db               # run forever; any number of these,
                                            # from any number of machines
interlock status pipeline.db <run_id>      # table by default, or --json
```

`workflow.py` just needs a module-level `workflow = Workflow()...` — the
same builder from the Quick example above.

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
  Asserts no task is ever claimed twice, every task is eventually claimed
  by exactly one worker, and — deterministically, no threads or sleeps —
  that a second `claim_next()` call can never return a task someone else
  already owns while its lease is valid.
- **`tests/test_crash_recovery.py`** — the single most important test in the
  project. Launches a real worker subprocess, waits until it's observably
  `running` a task, sends a genuine `SIGKILL` (not a graceful shutdown), then
  lets a second, independent worker reclaim the abandoned lease once it
  expires. Asserts the DAG still completes, with output matching a
  no-failure baseline exactly.
- **`tests/test_heartbeat.py`** — a second worker actively tries to steal a
  still-running task every 50ms for its full duration. Proves a task that
  legitimately runs longer than its lease TTL is never wrongly reclaimed
  while its owner is alive and heartbeating.
- **`tests/test_workflow.py`** — the `Workflow`/`Task` builder: dangling
  dependency and cycle detection (including a diamond dependency, which a
  naive "seen this node before" check would wrongly flag as a cycle), plus
  an end-to-end run through `Store`/`Worker`.

17 tests, stable across repeated full-suite runs.

## Benchmarked against Celery+Redis, honestly

Two separate writeups, not one number:

- **[`benchmarks/RESULTS.md`](benchmarks/RESULTS.md)** — raw throughput.
  Celery is **~5-6x faster** on a trivial-task workload, warm. That gap is
  the real, measured cost of interlock's subprocess-per-task execution
  model (language-agnostic) against Celery's in-process Python calls — not
  hidden, not rounded up. Includes a real environment finding along the
  way: Celery's default `prefork` pool doesn't work under Python 3.14 here,
  documented rather than worked around silently.
- **[`benchmarks/crash_comparison/CRASH_COMPARISON.md`](benchmarks/crash_comparison/CRASH_COMPARISON.md)**
  — the actual differentiator. A real worker `SIGKILL` mid-task on both
  systems, Celery configured realistically for redelivery (not left on
  defaults, which don't retry at all). Both recover automatically — but
  interlock re-executes exactly the interrupted step, while Celery's
  redelivery, even correctly configured, re-ran an already-succeeded step
  and double-fired two others. Traced against Kombu's actual source, not
  assumed. The real gap over Celery isn't "can it recover" — it's
  duplicate-execution risk on non-idempotent tasks.

## Not yet built

- A dashboard/inspector

## Task execution model

Subprocess-based, language-agnostic: a worker spawns the task's declared
`command`, writes upstream dependencies' outputs (JSON) to stdin, reads the
result from stdout. Exit 0 = success. This has real overhead (process spawn
per task) compared to in-process execution — measured, not hidden, in
`benchmarks/RESULTS.md`.

## Development

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

## License

MIT
