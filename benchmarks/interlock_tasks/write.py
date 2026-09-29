#!/usr/bin/env python3
import json
import sys

inputs = json.loads(sys.stdin.read())
print(json.dumps({"value": inputs["transform"]["value"] * 10}))
