"""Crash-comparison demonstration, interlock side. Same pattern as
tests/test_crash_recovery.py, but reported as a standalone demonstration
with timing and the execution-log evidence, for direct comparison against
the Celery side (run_celery_crash.py).
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from interlock.store import Store
from interlock.worker import Worker

TASKS = Path(__file__).parent / "interlock_tasks"
PYTHON = sys.executable
TRANSFORM_DURATION = 4.0
LEASE_TTL = 2.0


def main() -> dict:
    tmp_dir = Path(tempfile.mkdtemp())
    db_path = tmp_dir / "crash_demo.db"
    exec_log = tmp_dir / "exec_log.jsonl"
    exec_log.touch()

    # Set in THIS process's own environ, not just a local dict: the first
    # worker is a real subprocess (gets `env=env` explicitly below), but the
    # recovery worker runs in-process in this script and its own
    # subprocess.run() calls for fetch/transform/write inherit os.environ
    # directly -- a local dict alone would leave those un-set.
    os.environ["INTERLOCK_CRASH_LOG"] = str(exec_log)
    env = os.environ.copy()

    store = Store(db_path)
    store.create_run(
        "demo",
        [
            {"name": "fetch", "command": [PYTHON, str(TASKS / "fetch.py")], "needs": []},
            {"name": "transform", "command": [PYTHON, str(TASKS / "transform.py"), str(TRANSFORM_DURATION)], "needs": ["fetch"]},
            {"name": "write", "command": [PYTHON, str(TASKS / "write.py")], "needs": ["transform"]},
        ],
    )
    store.close()

    t_start = time.monotonic()
    proc = subprocess.Popen(
        [PYTHON, "-m", "interlock.worker", str(db_path), str(LEASE_TTL)],
        cwd=str(Path(__file__).parent.parent.parent),
        env=env,
    )

    observer = Store(db_path)
    deadline = time.monotonic() + 15.0
    while time.monotonic() < deadline:
        task = observer.get_task("demo:transform")
        if task and task["status"] == "running":
            break
        time.sleep(0.02)
    else:
        raise TimeoutError("transform never reached running before the kill deadline")

    t_kill = time.monotonic()
    print(f"[interlock] killing worker at t={t_kill - t_start:.2f}s, transform is running", file=sys.stderr)
    proc.kill()
    proc.wait(timeout=10.0)

    # Wait for the abandoned lease to actually expire, then a fresh worker
    # picks up where it left off -- no artificial shortcut, real wall-clock
    # time, same as the lease_ttl a real deployment would set.
    fresh_worker = Worker(db_path, lease_ttl=LEASE_TTL, poll_interval=0.02)
    try:
        recovery_deadline = time.monotonic() + 20.0
        while time.monotonic() < recovery_deadline:
            if observer.run_status("demo") in ("succeeded", "failed"):
                break
            fresh_worker.run_one_cycle()
        else:
            raise TimeoutError("run did not recover within the deadline")
    finally:
        fresh_worker.close()
    t_done = time.monotonic()

    final_status = observer.run_status("demo")
    tasks = {t["name"]: t for t in observer.tasks_for_run("demo")}
    observer.close()

    exec_counts = {}
    for line in exec_log.read_text().splitlines():
        entry = json.loads(line)
        exec_counts[entry["task"]] = exec_counts.get(entry["task"], 0) + 1

    return {
        "system": "interlock",
        "final_status": final_status,
        "final_output": json.loads(tasks["write"]["output"]) if tasks["write"]["output"] else None,
        "time_to_kill_s": t_kill - t_start,
        "time_kill_to_recovered_s": t_done - t_kill,
        "total_time_s": t_done - t_start,
        "execution_counts": exec_counts,
        "manual_intervention_required": False,
    }


if __name__ == "__main__":
    result = main()
    print(json.dumps(result, indent=2))
