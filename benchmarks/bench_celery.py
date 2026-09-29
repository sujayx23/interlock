"""Time N independent 3-step chains (fetch -> transform -> write) through
Celery+Redis. Requires a worker already running:

    celery -A tasks_celery worker --loglevel=info --concurrency=4 -P threads

(prefork, Celery's default pool, does not work under Python 3.14 in this
environment -- see benchmarks/RESULTS.md. threads is the closest working
substitute, and a materially different execution model: GIL-shared threads,
not separate processes.)
"""
from __future__ import annotations

import sys
import time

from celery import chain

from tasks_celery import fetch, transform, write


def run_benchmark(n_runs: int) -> dict:
    # Timed window starts before dispatch, symmetric with bench_interlock.py
    # (which times from before its own task-creation loop) -- Celery's
    # worker is already running continuously in the background here, so
    # dispatch and processing genuinely overlap in real time; excluding
    # dispatch from the timed window would understate Celery's real elapsed
    # time relative to interlock's, whose workers in this script only start
    # after task creation finishes.
    t0 = time.monotonic()
    results = [chain(fetch.s(), transform.s(), write.s())() for _ in range(n_runs)]
    outputs = [r.get(timeout=60) for r in results]
    elapsed = time.monotonic() - t0

    succeeded = sum(1 for o in outputs if o == {"value": 20})
    return {
        "system": "celery",
        "n_runs": n_runs,
        "elapsed_s": elapsed,
        "runs_per_sec": n_runs / elapsed,
        "succeeded": succeeded,
        "failed": n_runs - succeeded,
    }


if __name__ == "__main__":
    import json

    n_runs = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    print(json.dumps(run_benchmark(n_runs), indent=2))
