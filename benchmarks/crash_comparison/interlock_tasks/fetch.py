#!/usr/bin/env python3
"""Crash-comparison fixture. Appends one line to EXEC_LOG on every
invocation (env var, shared with the Celery side) before doing anything
else -- this is the actual proof mechanism: after the demonstration, the
log is read back to see exactly how many times each step really executed,
rather than assuming it from the framework's documented behavior."""
import json
import os
import sys
import time

EXEC_LOG = os.environ["INTERLOCK_CRASH_LOG"]
with open(EXEC_LOG, "a") as f:
    f.write(json.dumps({"task": "fetch", "ts": time.time(), "pid": os.getpid()}) + "\n")

sys.stdin.read()
print(json.dumps({"value": 1}))
