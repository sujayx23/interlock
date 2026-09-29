"""Celery app configured realistically for crash recovery, not left on
defaults (which have task_acks_late=False -- a task is acked the moment a
worker RECEIVES it, before executing it, so a worker dying mid-task means
Redis has already discarded the message and it is lost forever, with no
redelivery ever attempted; that would make this comparison unfairly
one-sided).

- task_acks_late=True: ack happens after the task finishes (success or
  failure), so a worker that dies mid-task leaves the message unacked.
- task_reject_on_worker_lost=True: best practice alongside acks_late, so a
  worker process the pool detects as gone gets its unacked task explicitly
  requeued rather than left to redelivery timing alone. NOTE, stated here
  and in RESULTS.md: this benchmark runs under -P threads (see
  benchmarks/RESULTS.md's Python-3.14/prefork finding), where there is no
  separate child process for the pool to detect as lost -- SIGKILLing the
  worker kills the one process outright, so this setting cannot fire here.
  Recovery in this demonstration depends entirely on the passive path
  below. Included anyway because it is what a real prefork deployment
  would also want configured, and the honest thing is to say plainly that
  it doesn't get to help in this test rather than quietly drop it.
- broker_transport_options visibility_timeout: Redis-transport-specific.
  An unacked message becomes eligible for redelivery to another consumer
  once this many seconds have passed with no ack. 8s here -- short enough
  to demonstrate in reasonable time, not Kombu's default of 3600s (1 hour),
  which would make this comparison technically correct but useless to run.
  A real deployment would set this based on expected task duration, not
  benchmark convenience.
- task_track_started=True: without this, AsyncResult.state stays PENDING
  until the task finishes; this benchmark needs to observe STARTED to know
  when to kill the worker at the same logical point (mid-transform) that
  the interlock side uses.
"""
import json
import os
import time

from celery import Celery

app = Celery(
    "bench_crash",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/1",
)
app.conf.update(
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_track_started=True,
    broker_transport_options={"visibility_timeout": 8},
    worker_prefetch_multiplier=1,
)

EXEC_LOG = os.environ.get("CELERY_CRASH_LOG", "/tmp/celery_crash_exec_log.jsonl")


def _record(task_name: str) -> None:
    with open(EXEC_LOG, "a") as f:
        f.write(json.dumps({"task": task_name, "ts": time.time(), "pid": os.getpid()}) + "\n")


@app.task
def fetch():
    _record("fetch")
    return {"value": 1}


@app.task(bind=True)
def transform(self, prev, duration=4.0):
    _record("transform")
    time.sleep(duration)
    return {"value": prev["value"] + 1}


@app.task
def write(prev):
    _record("write")
    return {"value": prev["value"] * 10}
