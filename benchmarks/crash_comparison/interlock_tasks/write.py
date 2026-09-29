#!/usr/bin/env python3
import json
import os
import sys
import time

EXEC_LOG = os.environ["INTERLOCK_CRASH_LOG"]
with open(EXEC_LOG, "a") as f:
    f.write(json.dumps({"task": "write", "ts": time.time(), "pid": os.getpid()}) + "\n")

inputs = json.loads(sys.stdin.read())
print(json.dumps({"value": inputs["transform"]["value"] * 10}))
