"""Thin CLI over the existing Store/Worker/Workflow APIs.

Deliberately no logic lives here beyond argument parsing, module loading for
`submit`, and formatting for `status` — every actual behavior (claim/fence,
retry backoff, DAG validation, ready propagation) is already implemented and
tested in interlock.store / interlock.worker / interlock.workflow. This file
should never need its own correctness tests, only "does it call the right
thing with the right arguments" ones.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import time
import uuid
from pathlib import Path

from interlock.store import Store
from interlock.worker import Worker
from interlock.workflow import Workflow, WorkflowError


def _load_workflow(path: str) -> Workflow:
    """Imports `path` as a standalone module and returns its module-level
    `workflow` variable. This is the one new convention the CLI introduces —
    everything else in this file is a direct pass-through to existing APIs."""
    module_path = Path(path)
    if not module_path.is_file():
        print(f"error: no such file: {path}", file=sys.stderr)
        raise SystemExit(1)

    spec = importlib.util.spec_from_file_location(module_path.stem, module_path)
    if spec is None or spec.loader is None:
        print(f"error: could not import {path} as a Python module", file=sys.stderr)
        raise SystemExit(1)
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as e:
        print(f"error: {path} raised while being imported: {e}", file=sys.stderr)
        raise SystemExit(1)

    wf = getattr(module, "workflow", None)
    if wf is None:
        print(
            f"error: {path} has no module-level `workflow` variable.\n"
            f"  interlock submit expects the file to define one, e.g.:\n\n"
            f"    from interlock.workflow import Workflow\n\n"
            f"    workflow = (\n"
            f"        Workflow()\n"
            f'        .task("fetch", command=["python3", "fetch.py"])\n'
            f'        .task("transform", command=["python3", "transform.py"], needs=["fetch"])\n'
            f"    )",
            file=sys.stderr,
        )
        raise SystemExit(1)
    if not isinstance(wf, Workflow):
        print(
            f"error: {path}'s module-level `workflow` is a {type(wf).__name__}, "
            f"not a Workflow instance",
            file=sys.stderr,
        )
        raise SystemExit(1)
    return wf


def _cmd_submit(args: argparse.Namespace) -> None:
    wf = _load_workflow(args.workflow_file)
    try:
        tasks = wf.tasks()
    except WorkflowError as e:
        print(f"error: invalid workflow: {e}", file=sys.stderr)
        raise SystemExit(1)

    run_id = args.run_id or uuid.uuid4().hex
    store = Store(args.db)
    try:
        store.create_run(run_id, tasks)
    finally:
        store.close()
    print(run_id)


def _cmd_worker(args: argparse.Namespace) -> None:
    worker = Worker(
        args.db,
        lease_ttl=args.lease_ttl,
        poll_interval=args.poll_interval,
        task_timeout=args.task_timeout,
    )
    cycles = 0
    try:
        while args.max_cycles is None or cycles < args.max_cycles:
            did_work = worker.run_one_cycle()
            # Every loop iteration counts, not just successful claims — see
            # the matching fix/comment in interlock.worker.main().
            cycles += 1
            if not did_work:
                time.sleep(worker.poll_interval)
    except KeyboardInterrupt:
        pass
    finally:
        worker.close()


def _cmd_status(args: argparse.Namespace) -> None:
    store = Store(args.db)
    try:
        try:
            run_status = store.run_status(args.run_id)
        except TypeError:
            # Store.run_status() assumes the run exists; a nonexistent run_id
            # raises TypeError on `row["status"]` against a None row rather
            # than returning None. Translated into a clean CLI error here
            # rather than changed in Store, which is out of scope for a
            # wrapper-only change.
            print(f"error: no such run: {args.run_id}", file=sys.stderr)
            raise SystemExit(1)
        tasks = store.tasks_for_run(args.run_id)
    finally:
        store.close()

    if args.json:
        print(json.dumps({"run_id": args.run_id, "status": run_status, "tasks": tasks}, indent=2))
        return

    print(f"run {args.run_id}: {run_status}")
    if not tasks:
        return
    name_w = max(len(t["name"]) for t in tasks)
    status_w = max(len(t["status"]) for t in tasks)
    for t in tasks:
        line = f"  {t['name']:<{name_w}}  {t['status']:<{status_w}}  attempts={t['attempts']}"
        if t["error"]:
            line += f"  error={t['error'][:80]!r}"
        print(line)


def _cmd_inspect(args: argparse.Namespace) -> None:
    from interlock.inspector import serve

    try:
        serve(args.db, port=args.port)
    except KeyboardInterrupt:
        pass


def main() -> None:
    parser = argparse.ArgumentParser(prog="interlock")
    sub = parser.add_subparsers(dest="command", required=True)

    p_submit = sub.add_parser("submit", help="submit a workflow.py's `workflow` as a new run")
    p_submit.add_argument("db", help="path to the SQLite database file")
    p_submit.add_argument("workflow_file", help="path to a .py file defining a module-level `workflow`")
    p_submit.add_argument("--run-id", default=None, help="run id (default: a generated uuid4 hex)")
    p_submit.set_defaults(func=_cmd_submit)

    p_worker = sub.add_parser("worker", help="run a worker loop against a database")
    p_worker.add_argument("db", help="path to the SQLite database file")
    p_worker.add_argument("--lease-ttl", type=float, default=15.0, help="seconds before an unrenewed claim is reclaimable")
    p_worker.add_argument("--poll-interval", type=float, default=0.1, help="seconds to sleep between empty polls")
    p_worker.add_argument("--task-timeout", type=float, default=60.0, help="seconds before a running task's subprocess is killed")
    p_worker.add_argument("--max-cycles", type=int, default=None, help="stop after this many claimed cycles (default: run forever)")
    p_worker.set_defaults(func=_cmd_worker)

    p_status = sub.add_parser("status", help="show a run's status and per-task detail")
    p_status.add_argument("db", help="path to the SQLite database file")
    p_status.add_argument("run_id", help="run id, as printed by `interlock submit`")
    p_status.add_argument("--json", action="store_true", help="print machine-readable JSON instead of a table")
    p_status.set_defaults(func=_cmd_status)

    p_inspect = sub.add_parser("inspect", help="run a read-only local web inspector")
    p_inspect.add_argument("db", help="path to the SQLite database file")
    p_inspect.add_argument("--port", type=int, default=8765, help="port to listen on (127.0.0.1 only)")
    p_inspect.set_defaults(func=_cmd_inspect)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
