"""Regression tests from a weak-spot review of the CLI and Store:

- Store.run_status() used to raise TypeError for a nonexistent run_id
  (indexing `row["status"]` against a None row) instead of returning None.
  Both cli.py and inspector.py had grown separate try/except TypeError
  workarounds for this; fixed at the source instead, and both workarounds
  removed. The CLI test here checks the actual user-facing behavior
  (a clean error, not a workaround).
- `interlock submit` with a `--run-id` that already exists used to crash
  with a raw sqlite3.IntegrityError traceback instead of a clean error.
"""

from __future__ import annotations

import argparse

import pytest

from interlock.cli import _cmd_status, _cmd_submit
from interlock.store import Store


def _write_workflow(tmp_path, name="wf.py"):
    path = tmp_path / name
    path.write_text(
        "from interlock.workflow import Workflow\n"
        'workflow = Workflow().task("a", command=["true"])\n'
    )
    return path


def test_store_run_status_returns_none_for_a_nonexistent_run(tmp_path):
    store = Store(tmp_path / "s.db")
    assert store.run_status("no-such-run") is None
    store.close()


def test_status_on_a_nonexistent_run_gives_a_clean_error_not_a_traceback(tmp_path):
    db_path = tmp_path / "s.db"
    Store(db_path).close()  # just create the schema

    args = argparse.Namespace(db=str(db_path), run_id="no-such-run", json=False)
    with pytest.raises(SystemExit) as exc_info:
        _cmd_status(args)
    assert exc_info.value.code == 1


def test_submit_with_a_duplicate_run_id_gives_a_clean_error_not_a_traceback(tmp_path):
    db_path = tmp_path / "s.db"
    wf_path = _write_workflow(tmp_path)

    args = argparse.Namespace(db=str(db_path), workflow_file=str(wf_path), run_id="dup")
    _cmd_submit(args)  # first submission succeeds

    with pytest.raises(SystemExit) as exc_info:
        _cmd_submit(args)  # second submission with the same run_id
    assert exc_info.value.code == 1
