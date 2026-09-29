"""Celery equivalent of benchmarks/interlock_tasks/{fetch,transform,write}.py
-- same logical chain, same arithmetic (1 -> +1 -> *10), so the two systems
run genuinely the same workload. The difference that matters for the
benchmark is the execution model: these are plain in-process Python
functions run by a long-lived Celery worker, not subprocesses spawned per
task. That's the whole point of comparing them.
"""
from celery import Celery

app = Celery("bench", broker="redis://localhost:6379/0", backend="redis://localhost:6379/1")
app.conf.worker_prefetch_multiplier = 1
app.conf.task_acks_late = False


@app.task
def fetch():
    return {"value": 1}


@app.task
def transform(prev):
    return {"value": prev["value"] + 1}


@app.task
def write(prev):
    return {"value": prev["value"] * 10}
