#!/usr/bin/env python3
"""Reads {"a": <a's output>} from stdin, derives b deterministically from it."""
import json
import sys

inputs = json.loads(sys.stdin.read())
print(json.dumps({"b": inputs["a"]["a"] + 1}))
