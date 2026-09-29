# Benchmark: interlock vs. Celery+Redis

**Bottom line: Celery is roughly 5-6x faster than interlock on this
workload, once both are warm.** The gap is almost entirely explained by one
structural decision — interlock spawns a subprocess per task; Celery's
tasks are plain in-process Python function calls. This was a known,
declared tradeoff going in (language-agnostic task execution vs. speed),
and these numbers are the honest cost of it, not something to round up or
bury.

## Workload

Both systems ran the same logical DAG: 3 sequential steps per run
(`fetch -> transform -> write`, arithmetic `1 -> +1 -> *10`, trivial
compute), N independent runs submitted together, timed from just before
submission to all N confirmed complete. Correctness matched exactly on
every run (`value: 20`, `succeeded: N/N`, `failed: 0` on both sides, every
run).

- interlock: `benchmarks/bench_interlock.py` — real `Worker` instances,
  one per pool thread, polling and executing exactly like the test suite
  does. Each task is a subprocess (`python3 fetch.py` etc.), JSON in on
  stdin, JSON out on stdout, per the settled task-execution design.
- Celery: `benchmarks/bench_celery.py` — `chain(fetch.s(), transform.s(),
  write.s())()` against a real `celery worker` process, Redis broker and
  result backend, both on localhost.

## A real environment finding, not a config mistake

Celery's default worker pool, `prefork` (a `billiard`-managed multiprocess
pool), **does not work under Python 3.14** in this environment:

```
ValueError: not enough values to unpack (expected 3, got 0)
  File ".../celery/app/trace.py", line 762, in fast_trace_task
    tasks, accept, hostname = _loc
```

This reproduced immediately on the very first task, every time, and is
clearly an internal `billiard`/Celery incompatibility with a Python version
this new (3.14), not something wrong with the task definitions or broker
config — `-P threads` (a plain `ThreadPoolExecutor`-backed pool) runs the
identical task code correctly. The benchmark below uses `-P threads`
throughout.

**This matters for interpreting the numbers, not just as a footnote.**
`-P threads` is a materially different execution model from `prefork`:
threads share one GIL, so CPU-bound task bodies would not get true
parallelism the way `prefork`'s separate processes do. This benchmark's
task bodies are trivial (a few arithmetic ops), so GIL contention is not
the bottleneck here — but a `prefork`-based number, if it existed on this
Python version, might look different (better for CPU-bound work at higher
concurrency, likely with more process-startup overhead of its own). This
benchmark can only honestly report what `-P threads` produced; it does not
claim to have measured Celery's best-case configuration.

## Numbers

First invocation of each system paid a cold-start cost (interlock: SQLite
file/WAL creation, bytecode compilation; Celery: worker/broker connection
warm-up) — reported separately below rather than folded into the "warm"
numbers, since a single cold run would misrepresent steady-state throughput
in either direction.

### Cold start (first run after worker/process startup)

| system            | n   | concurrency | elapsed | runs/sec |
|-------------------|-----|-------------|---------|----------|
| interlock         | 50  | 4           | 1.578s  | 31.7     |
| celery (threads)  | 50  | 4           | 1.888s  | 26.5     |

Roughly comparable cold, both dominated by one-time setup cost rather than
per-task overhead — not a meaningful comparison of the systems themselves,
included for transparency since it was the first thing measured.

### Warm, n=50, concurrency=4 (5 runs each)

| system            | run 1  | run 2  | run 3  | run 4  | run 5  | mean runs/sec |
|-------------------|--------|--------|--------|--------|--------|----------------|
| interlock         | 56.8   | 59.8   | 58.5   | 60.1   | 60.3   | **59.1**       |
| celery (threads)  | 344.3  | 354.1  | 356.2  | 356.3  | 353.1  | **352.8**      |

Celery ≈ **6.0x** interlock's throughput, warm, at this concurrency.

### Warm, n=200

| system                        | elapsed | runs/sec |
|-------------------------------|---------|----------|
| interlock, concurrency=4      | 3.268s  | 61.2     |
| interlock, concurrency=8      | 2.332s  | 85.8     |
| celery (threads), concurrency=8 | 0.435s  | 459.9    |

Interlock scales with worker count (61.2 -> 85.8 runs/sec, concurrency
4 -> 8) but sub-linearly — doubling workers gave ~1.4x throughput, not 2x.
The likely reason (not independently confirmed by a separate profiling run,
so stated as the likely explanation rather than a proven one): SQLite
serializes writers, and every claim/heartbeat/complete is a write, so
beyond some worker count the bottleneck shifts from "waiting on subprocess
spawn" to "waiting on the SQLite write lock." Celery ≈ **5.4x** interlock's
throughput at concurrency=8.

## What this means

- **Subprocess-per-task is real, measured overhead, not a hidden cost.**
  ~5-6x is the actual price of interlock's language-agnostic execution
  model against Celery's in-process Python functions, on trivial task
  bodies where the ratio is least favorable to interlock (subprocess spawn
  cost dominates total time; there's almost no task compute to amortize it
  against). A workload with heavier per-task compute would narrow this gap,
  since the fixed subprocess-spawn cost becomes a smaller fraction of total
  task time — not measured here, and not claimed.
- **This is the tradeoff that was declared up front**, not a surprise the
  benchmark uncovered: subprocess execution was chosen for language-agnostic
  tasks (any command, not just Python), at a known throughput cost. These
  numbers are what that cost actually is on this machine, for this workload.
- **Not measured here — but see `crash_comparison/CRASH_COMPARISON.md`,
  which is:** this benchmark is about raw throughput on a workload both
  systems can express. The actual differentiator — crash recovery — is a
  separate, dedicated demonstration: a real `SIGKILL` mid-task on both
  systems, correctly configured for redelivery on the Celery side, exec
  counts observed directly rather than assumed. Short version: both
  recover automatically (Celery is not "unable to recover"), but interlock
  re-executes exactly the interrupted step and nothing else, while Celery's
  redelivery — even correctly configured — re-ran an already-succeeded
  step and double-fired two others. That's the real gap: duplicate-
  execution risk for non-idempotent tasks, not "can it recover at all."

## Reproducing

```bash
# Terminal 1
redis-server --port 6379

# Terminal 2
cd benchmarks && celery -A tasks_celery worker --loglevel=info --concurrency=4 -P threads

# Terminal 3
python3 benchmarks/bench_interlock.py 50 4
python3 benchmarks/bench_celery.py 50
```
