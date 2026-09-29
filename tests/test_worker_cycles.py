"""Regression test for a real bug found while smoke-testing the CLI:
Worker's cycle-counting loop (worker.py's main(), mirrored into
cli._cmd_worker) used to do `cycles += did_work`, so an empty poll (no
ready task to claim) never counted toward `max_cycles`. Once the ready
pool drained before `max_cycles` *successful claims* had happened, the
loop spun forever instead of terminating — exactly the case here: 1
ready task, max_cycles=5.

Run through a background thread with a bounded `join(timeout=...)` rather
than calling `_cmd_worker` directly in the test body: if this ever
regresses back to the old behavior, the test fails fast on the
`is_alive()` assertion instead of hanging the whole suite.
"""

from __future__ import annotations

import argparse
import threading

from interlock.cli import _cmd_worker
from interlock.store import Store
from interlock.worker import Worker


def test_worker_max_cycles_terminates_even_when_queue_drains_early(tmp_path, monkeypatch):
    db_path = tmp_path / "cycles.db"
    store = Store(db_path)
    store.create_run("run1", [{"name": "a", "command": ["true"]}])
    store.close()

    call_count = 0
    original_run_one_cycle = Worker.run_one_cycle

    def counting_run_one_cycle(self):
        nonlocal call_count
        call_count += 1
        return original_run_one_cycle(self)

    monkeypatch.setattr(Worker, "run_one_cycle", counting_run_one_cycle)

    args = argparse.Namespace(
        db=str(db_path),
        lease_ttl=30.0,
        poll_interval=0.01,  # short and injected, not real sleeping in the test itself
        task_timeout=10.0,
        max_cycles=5,
    )

    thread = threading.Thread(target=_cmd_worker, args=(args,), daemon=True)
    thread.start()
    thread.join(timeout=5.0)

    assert not thread.is_alive(), (
        "worker loop did not terminate within max_cycles — "
        "regression of the cycles-counting fix (cycles must count every "
        "loop iteration, not just successful claims)"
    )
    assert call_count == 5

    check = Store(db_path)
    task = check.get_task("run1:a")
    check.close()
    assert task["status"] == "done"
