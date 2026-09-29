#!/usr/bin/env python3
"""Trivial first step: no upstream deps, returns a fixed small payload.
Deliberately near-zero compute -- the benchmark measures framework overhead
(claim, dispatch, subprocess spawn), not task work."""
import json
import sys

sys.stdin.read()
print(json.dumps({"value": 1}))
