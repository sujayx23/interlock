#!/usr/bin/env python3
"""A deliberately slow first step, so a test can observe it as `running` in
the DB and kill its worker mid-execution at a known point. Ignores stdin
(no upstream deps)."""
import json
import sys
import time

DURATION = float(sys.argv[1]) if len(sys.argv) > 1 else 2.0

sys.stdin.read()  # drain stdin per the worker contract, even though unused
time.sleep(DURATION)
print(json.dumps({"a": 1}))
