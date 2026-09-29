"""Time N independent 3-step DAG runs (fetch -> transform -> write) through
interlock, at a fixed worker pool concurrency. Each worker is a real Worker
instance in its own thread, polling and executing exactly like the tests do
-- no special benchmark-only fast path.
"""
from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from interlock.store import Store
from interlock.worker import Worker
from interlock.workflow import Workflow

TASKS_DIR = Path(__file__).parent / "interlock_tasks"
PYTHON = sys.executable


def build_workload(n_runs: int) -> list[tuple[str, list[dict]]]:
    runs = []
    for i in range(n_runs):
        wf = (
            Workflow()
            .task("fetch", command=[PYTHON, str(TASKS_DIR / "fetch.py")])
            .task("transform", command=[PYTHON, str(TASKS_DIR / "transform.py")], needs=["fetch"])
            .task("write", command=[PYTHON, str(TASKS_DIR / "write.py")], needs=["transform"])
        )
        runs.append((f"run-{i}", wf.tasks()))
    return runs


def run_benchmark(n_runs: int, concurrency: int, db_path: Path) -> dict:
    # Timed window starts here, before ANY submission work -- symmetric with
    # bench_celery.py, which times from before its dispatch loop too. Task
    # creation (writing N runs to SQLite) is a real cost a user pays, same as
    # Celery's dispatch-to-broker cost; excluding it from one side and not
    # the other would understate whichever system's worker happens to start
    # consuming concurrently with submission (Celery's does; interlock's
    # workers here start only after creation finishes, so this ordering
    # doesn't currently change interlock's own number -- it matters for
    # comparability with the Celery side, not for this line by itself).
    t0 = time.monotonic()
    setup = Store(db_path)
    for run_id, tasks in build_workload(n_runs):
        setup.create_run(run_id, tasks)
    setup.close()

    stop = threading.Event()

    def worker_loop():
        w = Worker(db_path, lease_ttl=30.0, poll_interval=0.01)
        try:
            while not stop.is_set():
                did_work = w.run_one_cycle()
                if not did_work:
                    time.sleep(0.01)
        finally:
            w.close()

    threads = [threading.Thread(target=worker_loop) for _ in range(concurrency)]
    for t in threads:
        t.start()

    check_store = Store(db_path)
    while True:
        rows = check_store._conn.execute("SELECT status FROM runs").fetchall()
        if all(r["status"] in ("succeeded", "failed") for r in rows):
            break
        time.sleep(0.02)
    elapsed = time.monotonic() - t0

    stop.set()
    for t in threads:
        t.join(timeout=5.0)

    statuses = [r["status"] for r in check_store._conn.execute("SELECT status FROM runs").fetchall()]
    check_store.close()

    return {
        "system": "interlock",
        "n_runs": n_runs,
        "concurrency": concurrency,
        "elapsed_s": elapsed,
        "runs_per_sec": n_runs / elapsed,
        "succeeded": statuses.count("succeeded"),
        "failed": statuses.count("failed"),
    }


if __name__ == "__main__":
    import json
    import tempfile

    n_runs = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    concurrency = int(sys.argv[2]) if len(sys.argv) > 2 else 4

    with tempfile.TemporaryDirectory() as tmp:
        result = run_benchmark(n_runs, concurrency, Path(tmp) / "bench.db")
    print(json.dumps(result, indent=2))
