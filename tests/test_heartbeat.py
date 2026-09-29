"""Heartbeat's whole job: a task that runs longer than lease_ttl must NOT be
reclaimed by another live worker while its original worker is still working
on it. Unlike the crash test (nothing else is contending for the lease while
it's abandoned), this test has a second worker actively trying to steal the
task every 50ms for the task's full duration — if heartbeats ever silently
fail, this is the test that catches it as a hard assertion failure, not a
background thread warning.

Ownership of the task is established with a direct claim_next() call BEFORE
the scavenger thread starts, not by racing worker.run_one_cycle() against the
scavenger's own first claim attempt. Racing them was tried first and is
wrong: claim_next() is correctly fair between any two workers racing for a
genuinely `ready` task, so on some runs the scavenger itself would win that
initial race legitimately (confirmed directly: ~1 in 20 trials) — at which
point the test's premise (a task someone already owns and is heartbeating)
never gets set up, and the run finishes near-instantly with nothing proven.
That was a bug in this test, not in claim_next() or heartbeat().
"""

from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

from interlock.store import Store
from interlock.worker import Worker

FIXTURES = Path(__file__).parent / "fixtures"
PYTHON = sys.executable


def test_heartbeat_prevents_reclaim_of_a_still_alive_slow_task(tmp_path):
    db_path = tmp_path / "heartbeat.db"
    lease_ttl = 0.4  # shorter than the task, so without heartbeating it WOULD expire mid-run
    task_duration = 2.0

    store = Store(db_path)
    store.create_run(
        "run1",
        [{"name": "slow", "command": [PYTHON, str(FIXTURES / "step_a.py"), str(task_duration)], "needs": []}],
    )

    worker = Worker(db_path, lease_ttl=lease_ttl, poll_interval=0.02)
    # Claim deterministically, in this thread, before any scavenger exists —
    # ownership must be established first for "steal an owned task" to mean
    # anything.
    lease = worker.store.claim_next(worker.worker_id, lease_ttl)
    assert lease is not None
    assert worker.store.mark_running(lease)

    steal_attempts = {"count": 0, "succeeded": []}
    stop = threading.Event()

    def scavenger() -> None:
        """A second worker, hammering the exact reclaim+claim path a real
        idle worker would use, the whole time the slow task is running."""
        scav_store = Store(db_path)
        try:
            while not stop.is_set():
                scav_store.reclaim_expired_leases()
                scav_lease = scav_store.claim_next(scav_store.new_worker_id(), lease_ttl)
                steal_attempts["count"] += 1
                if scav_lease is not None:
                    steal_attempts["succeeded"].append(scav_lease)
                time.sleep(0.05)
        finally:
            scav_store.close()

    scav_thread = threading.Thread(target=scavenger, daemon=True)
    scav_thread.start()

    try:
        worker._execute(lease)  # runs the slow task to completion, heartbeating throughout
    finally:
        stop.set()
        scav_thread.join(timeout=5.0)
        worker.close()
        store.close()

    assert steal_attempts["count"] > 5, "the scavenger should have gotten many chances to steal"
    assert steal_attempts["succeeded"] == [], (
        f"a second worker reclaimed a task whose original owner was still alive and "
        f"heartbeating — heartbeat is not preventing wrongful reclaim: "
        f"{steal_attempts['succeeded']}"
    )

    verify = Store(db_path)
    task = verify.get_task("run1:slow")
    assert task["status"] == "done"
    assert task["attempts"] == 0
    assert task["epoch"] == 1  # claimed exactly once, never reclaimed
    verify.close()
