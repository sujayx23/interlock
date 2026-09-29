"""Retry backoff and the `dead` terminal status.

A failed task no longer goes straight back to `ready` for an immediate
re-attempt: it accumulates `attempts`, gets exponential-backoff-with-jitter
`not_before` gating on its next `ready` window, and — once `attempts`
reaches `max_retries` — becomes permanently `dead` instead of endlessly
retrying, the same way `failed` already blocked descendants but without
ever pretending the task might still succeed.
"""

from __future__ import annotations

from interlock.store import Store


def _claim_and_fail(store: Store, run_id: str, task_name: str, worker_id: str, now: float, error: str = "boom"):
    lease = store.claim_next(worker_id, lease_ttl=30.0, now=now)
    assert lease is not None
    assert lease.name == task_name
    store.fail(lease, error, now=now)


def test_task_that_fails_then_succeeds_on_last_allowed_attempt_completes(tmp_path):
    store = Store(tmp_path / "retry.db")
    store.create_run("run1", [
        {"name": "flaky", "command": ["true"], "max_retries": 3},
    ])

    t = 0.0
    # Two failures, backoff each time but never exhausting max_retries=3.
    for _ in range(2):
        lease = store.claim_next("w1", lease_ttl=30.0, now=t)
        assert lease is not None
        store.fail(lease, "transient", now=t, backoff_base=1.0, backoff_cap=1.0)
        task = store.get_task(lease.task_id)
        assert task["status"] == "ready"
        assert task["not_before"] is not None and task["not_before"] > t
        # Jump time past the backoff window before the next claim attempt.
        t = task["not_before"] + 0.01

    # Third attempt succeeds.
    lease = store.claim_next("w1", lease_ttl=30.0, now=t)
    assert lease is not None
    assert store.complete(lease, {"ok": True})

    task = store.get_task(lease.task_id)
    assert task["status"] == "done"
    assert task["attempts"] == 2
    assert store.run_status("run1") == "succeeded"
    store.close()


def test_task_that_fails_max_retries_times_ends_up_dead_not_looping_forever(tmp_path):
    store = Store(tmp_path / "dead.db")
    store.create_run("run1", [
        {"name": "doomed", "command": ["false"], "max_retries": 3},
    ])

    t = 0.0
    for _ in range(3):
        lease = store.claim_next("w1", lease_ttl=30.0, now=t)
        assert lease is not None
        store.fail(lease, "always fails", now=t, backoff_base=1.0, backoff_cap=1.0)
        task = store.get_task(lease.task_id)
        if task["status"] == "ready":
            t = task["not_before"] + 0.01

    task = store.get_task(lease.task_id)
    assert task["status"] == "dead"
    assert task["attempts"] == 3
    assert task["not_before"] is None

    # And it's genuinely inert: no further claim ever returns it again.
    assert store.claim_next("w2", lease_ttl=30.0, now=t + 100.0) is None
    assert store.run_status("run1") == "failed"
    store.close()


def test_claim_next_does_not_return_a_task_before_its_not_before_time(tmp_path):
    """Deterministic fake-clock check — no real sleeping, no thread races."""
    store = Store(tmp_path / "notbefore.db")
    store.create_run("run1", [
        {"name": "a", "command": ["false"], "max_retries": 5},
    ])

    lease = store.claim_next("w1", lease_ttl=30.0, now=0.0)
    assert lease is not None
    store.fail(lease, "boom", now=0.0, backoff_base=10.0, backoff_cap=10.0)

    task = store.get_task(lease.task_id)
    assert task["status"] == "ready"
    not_before = task["not_before"]
    assert not_before > 0.0

    # Strictly before not_before: must not be claimable.
    assert store.claim_next("w2", lease_ttl=30.0, now=not_before - 0.001) is None
    # At/after not_before: claimable again.
    lease2 = store.claim_next("w2", lease_ttl=30.0, now=not_before)
    assert lease2 is not None
    assert lease2.task_id == lease.task_id
    store.close()


def test_downstream_task_is_blocked_when_its_dependency_goes_dead(tmp_path):
    store = Store(tmp_path / "blocked.db")
    store.create_run("run1", [
        {"name": "upstream", "command": ["false"], "max_retries": 1},
        {"name": "downstream", "command": ["true"], "needs": ["upstream"]},
    ])

    lease = store.claim_next("w1", lease_ttl=30.0, now=0.0)
    assert lease is not None
    assert lease.name == "upstream"
    # max_retries=1 means this single failure already exhausts it -> dead.
    store.fail(lease, "boom", now=0.0)

    upstream = store.get_task(f"run1:upstream")
    assert upstream["status"] == "dead"

    downstream = store.get_task(f"run1:downstream")
    assert downstream["status"] == "blocked"

    # Never silently becomes ready/claimable.
    assert store.claim_next("w2", lease_ttl=30.0, now=1000.0) is None
    assert store.run_status("run1") == "failed"
    store.close()
