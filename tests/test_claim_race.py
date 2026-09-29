"""The single highest-risk correctness property of the whole engine: with N
workers racing against the same SQLite file, no ready task is ever claimed
by more than one worker, and every ready task is eventually claimed by
exactly one.

Threads with independent connections, not asyncio tasks sharing one
connection: sqlite3's C extension releases the GIL during actual disk I/O,
so separate connections genuinely interleave at the SQLite write-lock level.
Asyncio tasks on a single shared connection would only prove the Python-level
scheduling never interleaves two calls on one object, which is a much weaker
claim than "the SQL itself is safe under real concurrent writers."
"""

from __future__ import annotations

import threading

import pytest

from interlock.store import Store


def _make_independent_tasks(n: int) -> list[dict]:
    """N tasks with no dependencies — all start `ready` immediately, so every
    worker is racing for the same pool from the first claim_next() call."""
    return [{"name": f"t{i}", "command": ["true"]} for i in range(n)]


@pytest.mark.parametrize("n_tasks,n_workers", [(200, 20), (1, 8), (50, 50)])
def test_no_task_claimed_twice(tmp_path, n_tasks, n_workers):
    db_path = tmp_path / "race.db"
    setup = Store(db_path)
    setup.create_run("run1", _make_independent_tasks(n_tasks))
    setup.close()

    claimed_by_thread: list[list[str]] = [[] for _ in range(n_workers)]
    barrier = threading.Barrier(n_workers)

    def worker(idx: int) -> None:
        store = Store(db_path)
        try:
            worker_id = store.new_worker_id()
            barrier.wait()  # maximize actual overlap, not just "ran at some point"
            while True:
                lease = store.claim_next(worker_id, lease_ttl=30.0)
                if lease is None:
                    break
                claimed_by_thread[idx].append(lease.task_id)
        finally:
            store.close()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
        assert not t.is_alive(), "worker thread hung — possible SQLite deadlock in claim_next"

    all_claimed = [tid for claims in claimed_by_thread for tid in claims]

    # The core property: no id appears twice across ALL workers combined.
    assert len(all_claimed) == len(set(all_claimed)), (
        f"a task was claimed by more than one worker: "
        f"{len(all_claimed)} claims but only {len(set(all_claimed))} distinct ids"
    )
    # And nothing was left behind or invented.
    assert len(all_claimed) == n_tasks

    verify = Store(db_path)
    statuses = {t["id"]: t["status"] for t in verify.tasks_for_run("run1")}
    verify.close()
    assert set(statuses) == set(all_claimed)
    assert all(s == "claimed" for s in statuses.values())


def test_claim_next_returns_none_when_pool_exhausted(tmp_path):
    store = Store(tmp_path / "empty.db")
    store.create_run("run1", _make_independent_tasks(3))
    worker_id = store.new_worker_id()
    got = [store.claim_next(worker_id, lease_ttl=30.0) for _ in range(3)]
    assert all(g is not None for g in got)
    assert store.claim_next(worker_id, lease_ttl=30.0) is None
    store.close()


def test_claim_bumps_epoch_and_no_two_claims_share_an_epoch_value(tmp_path):
    """Direct check on the fencing token itself: each successful claim of the
    same task id (across its retry lifecycle) gets a strictly higher epoch
    than the last, which is the whole basis for complete()/fail() rejecting
    a stale writer."""
    store = Store(tmp_path / "epoch.db")
    store.create_run("run1", _make_independent_tasks(1))
    worker_id = store.new_worker_id()

    lease1 = store.claim_next(worker_id, lease_ttl=30.0)
    assert lease1.epoch == 1  # epoch column defaults to 0; first claim -> 1

    # Simulate a crash: expire the lease immediately, then reclaim.
    store._conn.execute(
        "UPDATE tasks SET lease_expires_at=0 WHERE id=?", (lease1.task_id,)
    )
    reclaimed = store.reclaim_expired_leases(now=1.0)
    assert reclaimed == 1

    lease2 = store.claim_next(worker_id, lease_ttl=30.0, now=1.0)
    assert lease2 is not None
    assert lease2.task_id == lease1.task_id
    assert lease2.epoch == 2
    assert lease2.epoch > lease1.epoch

    # The zombie's late write, fenced on the OLD epoch, must be rejected.
    assert store.complete(lease1, output="late-and-wrong") is False
    # The current owner's write, fenced on the CURRENT epoch, must succeed.
    assert store.complete(lease2, output="correct") is True
    store.close()
