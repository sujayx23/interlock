"""Crash-comparison demonstration, Celery side. Same shape as
run_interlock_crash.py: real worker subprocess, real SIGKILL mid-transform,
a fresh worker started afterward, timing and exec-log evidence reported the
same way for direct comparison.

Requires Redis running on localhost:6379 (db 0 broker, db 1 backend).
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

TRANSFORM_DURATION = 4.0
VISIBILITY_TIMEOUT = 8  # must match tasks_celery_crash.py's broker_transport_options


def _start_worker(env: dict, logfile: str, pidfile: str) -> None:
    subprocess.run(
        [
            "celery", "-A", "tasks_celery_crash", "worker",
            "--loglevel=info", "--concurrency=4", "-P", "threads",
            f"--pidfile={pidfile}", f"--logfile={logfile}", "--detach",
        ],
        cwd=str(Path(__file__).parent),
        env=env,
        check=True,
    )


def _worker_pid(pidfile: str) -> int:
    for _ in range(50):
        if os.path.exists(pidfile):
            return int(Path(pidfile).read_text().strip())
        time.sleep(0.1)
    raise TimeoutError("celery worker pidfile never appeared")


def main() -> dict:
    tmp_dir = Path(tempfile.mkdtemp())
    exec_log = tmp_dir / "celery_exec_log.jsonl"
    exec_log.touch()

    env = os.environ.copy()
    env["CELERY_CRASH_LOG"] = str(exec_log)

    logfile1 = str(tmp_dir / "worker1.log")
    pidfile1 = str(tmp_dir / "worker1.pid")
    _start_worker(env, logfile1, pidfile1)
    pid1 = _worker_pid(pidfile1)
    time.sleep(2.0)  # let the worker actually finish mingling/booting before we dispatch

    from tasks_celery_crash import fetch, transform, write
    from celery import chain

    t_start = time.monotonic()
    write_result = chain(fetch.s(), transform.s(TRANSFORM_DURATION), write.s())()
    transform_result = write_result.parent

    deadline = time.monotonic() + 15.0
    while time.monotonic() < deadline:
        if transform_result.state == "STARTED":
            break
        time.sleep(0.02)
    else:
        raise TimeoutError("transform never reached STARTED before the kill deadline")

    t_kill = time.monotonic()
    print(f"[celery] killing worker pid={pid1} at t={t_kill - t_start:.2f}s, transform state=STARTED", file=sys.stderr)
    os.kill(pid1, 9)  # SIGKILL -- no graceful shutdown, same rigor as the interlock side
    for _ in range(50):
        try:
            os.kill(pid1, 0)
            time.sleep(0.1)
        except ProcessLookupError:
            break

    # Real wall-clock wait for the broker's visibility_timeout to elapse --
    # no shortcut, same standard as interlock's real lease-expiry wait.
    print(f"[celery] waiting {VISIBILITY_TIMEOUT + 2}s for visibility_timeout redelivery window", file=sys.stderr)
    time.sleep(VISIBILITY_TIMEOUT + 2)

    logfile2 = str(tmp_dir / "worker2.log")
    pidfile2 = str(tmp_dir / "worker2.pid")
    _start_worker(env, logfile2, pidfile2)

    try:
        final_output = write_result.get(timeout=30.0)
        final_status = "succeeded"
    except Exception as e:
        final_output = None
        final_status = f"failed: {e}"
    t_done = time.monotonic()

    pid2 = _worker_pid(pidfile2)
    subprocess.run(["celery", "-A", "tasks_celery_crash", "control", "shutdown"],
                    cwd=str(Path(__file__).parent), env=env, capture_output=True)
    time.sleep(1.0)

    exec_counts: dict[str, int] = {}
    for line in exec_log.read_text().splitlines():
        entry = json.loads(line)
        exec_counts[entry["task"]] = exec_counts.get(entry["task"], 0) + 1

    return {
        "system": "celery",
        "final_status": final_status,
        "final_output": final_output,
        "time_to_kill_s": t_kill - t_start,
        "time_kill_to_recovered_s": t_done - t_kill,
        "total_time_s": t_done - t_start,
        "execution_counts": exec_counts,
        "manual_intervention_required": False,
        "visibility_timeout_configured_s": VISIBILITY_TIMEOUT,
    }


if __name__ == "__main__":
    result = main()
    print(json.dumps(result, indent=2))
