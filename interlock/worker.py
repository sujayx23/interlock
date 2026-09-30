"""A leaderless worker: poll, claim, execute the task's command as a
subprocess, fence the completion write. No coordination with any other
worker beyond what the Store's atomic SQL already guarantees.

Task execution contract: the worker spawns `lease.command` as a subprocess,
writes ``json.dumps(lease.inputs)`` (a dict of upstream task name -> output)
to its stdin, waits for it to exit, and reads its whole stdout as the JSON
result. Exit code 0 = success, stdout parsed as JSON becomes the task's
output. Nonzero = failure; stderr is captured into the task's error field.
"""

from __future__ import annotations

import json
import subprocess
import sys
import threading
import time

from interlock.store import Store, TaskLease


class Worker:
    def __init__(
        self,
        db_path: str,
        *,
        lease_ttl: float = 15.0,
        poll_interval: float = 0.1,
        task_timeout: float = 60.0,
    ):
        self.store = Store(db_path)
        self.worker_id = self.store.new_worker_id()
        self.lease_ttl = lease_ttl
        self.poll_interval = poll_interval
        self.task_timeout = task_timeout
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run_forever(self) -> None:
        while not self._stop.is_set():
            if not self.run_one_cycle():
                time.sleep(self.poll_interval)

    def run_one_cycle(self) -> bool:
        """Try one claim+execute+complete cycle. Returns True if a task was
        claimed (whether it then succeeded or failed), False if the pool was
        empty (caller should back off before polling again)."""
        self.store.reclaim_expired_leases()
        lease = self.store.claim_next(self.worker_id, self.lease_ttl)
        if lease is None:
            return False
        if not self.store.mark_running(lease):
            # Lost the lease between claim and mark_running (reclaimed already
            # by a very short TTL under test) — nothing to execute, move on.
            return True
        self._execute(lease)
        return True

    def _execute(self, lease: TaskLease) -> None:
        stop_heartbeat = threading.Event()
        alive = threading.Event()
        alive.set()

        def heartbeat_loop() -> None:
            # sqlite3 connections cannot cross threads (check_same_thread
            # default), so the heartbeat thread opens its own connection to
            # the same file rather than sharing self.store's — sharing it
            # silently raised ProgrammingError on every tick, which meant
            # heartbeats never actually landed and a task slower than
            # lease_ttl would look abandoned to another worker while this
            # one was still alive and working on it.
            hb_store = Store(self.store.path)
            try:
                while not stop_heartbeat.wait(self.lease_ttl / 3):
                    if not hb_store.heartbeat(lease, self.lease_ttl):
                        alive.clear()
                        return
            finally:
                hb_store.close()

        hb_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        hb_thread.start()
        try:
            proc = subprocess.run(
                lease.command,
                input=json.dumps(lease.inputs),
                capture_output=True,
                text=True,
                timeout=self.task_timeout,
            )
        except subprocess.TimeoutExpired:
            stop_heartbeat.set()
            hb_thread.join()
            if alive.is_set():
                # Same invariant as the non-timeout path below: don't write
                # if the lease was already lost mid-execution. fail()'s
                # epoch fence would reject a stale write anyway, but
                # checking here too keeps this path consistent with the
                # rest of _execute() instead of relying on the fence alone.
                self.store.fail(lease, "task timed out")
            return
        stop_heartbeat.set()
        hb_thread.join()

        if not alive.is_set():
            # Lease was lost mid-execution (reclaimed by someone else); this
            # worker's result, win or lose, must not be written. This is the
            # in-process mirror of what complete()/fail()'s epoch fence also
            # enforces at the SQL level — belt and suspenders, same invariant.
            return

        if proc.returncode == 0:
            try:
                output = json.loads(proc.stdout) if proc.stdout.strip() else None
            except json.JSONDecodeError as e:
                self.store.fail(lease, f"task stdout was not valid JSON: {e}")
                return
            self.store.complete(lease, output)
        else:
            error = f"exit {proc.returncode}: {proc.stderr[-2000:]}"
            self.store.fail(lease, error)

    def close(self) -> None:
        self.store.close()


def main() -> None:
    if len(sys.argv) < 2:
        print("usage: python -m interlock.worker <db_path> [lease_ttl] [max_cycles]", file=sys.stderr)
        raise SystemExit(2)
    db_path = sys.argv[1]
    lease_ttl = float(sys.argv[2]) if len(sys.argv) > 2 else 15.0
    max_cycles = int(sys.argv[3]) if len(sys.argv) > 3 else None

    worker = Worker(db_path, lease_ttl=lease_ttl)
    cycles = 0
    try:
        while max_cycles is None or cycles < max_cycles:
            did_work = worker.run_one_cycle()
            # Counts every loop iteration, not just successful claims — an
            # empty-poll iteration still counts, so max_cycles reliably
            # terminates instead of spinning forever once the queue drains
            # before max_cycles claims have happened.
            cycles += 1
            if not did_work:
                time.sleep(worker.poll_interval)
    finally:
        worker.close()


if __name__ == "__main__":
    main()
