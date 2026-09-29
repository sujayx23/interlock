#!/usr/bin/env python3
"""Reads {"b": <b's output>} from stdin, derives the final result."""
import json
import sys

inputs = json.loads(sys.stdin.read())
print(json.dumps({"c": inputs["b"]["b"] * 10}))
