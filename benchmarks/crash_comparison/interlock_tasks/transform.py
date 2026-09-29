#!/usr/bin/env python3
"""Step 2 of 3, deliberately slow so a test can catch it mid-execution and
kill the worker at a known point."""
import json
import os
import sys
import time

EXEC_LOG = os.environ["INTERLOCK_CRASH_LOG"]
with open(EXEC_LOG, "a") as f:
    f.write(json.dumps({"task": "transform", "ts": time.time(), "pid": os.getpid()}) + "\n")

DURATION = float(sys.argv[1]) if len(sys.argv) > 1 else 4.0
inputs = json.loads(sys.stdin.read())
time.sleep(DURATION)
print(json.dumps({"value": inputs["fetch"]["value"] + 1}))
