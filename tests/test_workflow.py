"""Workflow/Task builder: validated at definition time, not discovered as a
run that silently never completes."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from interlock.store import Store
from interlock.workflow import Workflow, WorkflowError

FIXTURES = Path(__file__).parent / "fixtures"
PYTHON = sys.executable


def test_valid_dag_builds_and_produces_expected_task_dicts():
    wf = (
        Workflow()
        .task("a", command=["true"])
        .task("b", command=["true"], needs=["a"])
        .task("c", command=["true"], needs=["a", "b"])
    )
    tasks = wf.tasks()
    by_name = {t["name"]: t for t in tasks}
    assert set(by_name) == {"a", "b", "c"}
    assert by_name["a"]["needs"] == []
    assert by_name["b"]["needs"] == ["a"]
    assert set(by_name["c"]["needs"]) == {"a", "b"}


def test_diamond_dependency_is_not_a_false_positive_cycle():
    """A -> B, A -> C, B -> D, C -> D: D is reachable from A via two paths.
    A naive "have I seen this node before, anywhere" check would wrongly
    flag this as a cycle; proper DFS coloring (white/gray/black) does not."""
    wf = (
        Workflow()
        .task("a", command=["true"])
        .task("b", command=["true"], needs=["a"])
        .task("c", command=["true"], needs=["a"])
        .task("d", command=["true"], needs=["b", "c"])
    )
    wf.validate()  # must not raise


def test_empty_workflow_is_rejected():
    with pytest.raises(WorkflowError, match="no tasks"):
        Workflow().validate()


def test_duplicate_task_name_is_rejected():
    wf = Workflow().task("a", command=["true"])
    with pytest.raises(WorkflowError, match="duplicate"):
        wf.task("a", command=["true"])


def test_dependency_on_undefined_task_is_rejected():
    wf = Workflow().task("a", command=["true"], needs=["ghost"])
    with pytest.raises(WorkflowError, match="undefined task 'ghost'"):
        wf.validate()


def test_direct_self_cycle_is_rejected():
    wf = Workflow().task("a", command=["true"], needs=["a"])
    with pytest.raises(WorkflowError, match="cycle"):
        wf.validate()


def test_indirect_cycle_is_rejected():
    wf = (
        Workflow()
        .task("a", command=["true"], needs=["c"])
        .task("b", command=["true"], needs=["a"])
        .task("c", command=["true"], needs=["b"])
    )
    with pytest.raises(WorkflowError, match="cycle"):
        wf.validate()


def test_workflow_drives_a_real_run_end_to_end(tmp_path):
    """Not just the builder in isolation — the Workflow's output actually
    creates a runnable DAG through Store.create_run, matching what the raw-
    dict tests already prove works for claim/fence/execute."""
    from interlock.worker import Worker

    wf = (
        Workflow()
        .task("a", command=[PYTHON, str(FIXTURES / "step_a.py"), "0.05"])
        .task("b", command=[PYTHON, str(FIXTURES / "step_b.py")], needs=["a"])
        .task("c", command=[PYTHON, str(FIXTURES / "step_c.py")], needs=["b"])
    )

    db_path = tmp_path / "workflow_e2e.db"
    store = Store(db_path)
    store.create_run("run1", wf.tasks())

    worker = Worker(db_path, lease_ttl=5.0, poll_interval=0.02)
    try:
        for _ in range(10):
            if store.run_status("run1") in ("succeeded", "failed"):
                break
            worker.run_one_cycle()
        else:
            raise TimeoutError("run did not finish")
    finally:
        worker.close()

    assert store.run_status("run1") == "succeeded"
    import json
    tasks = {t["name"]: t for t in store.tasks_for_run("run1")}
    assert json.loads(tasks["c"]["output"]) == {"c": 20}
    store.close()
