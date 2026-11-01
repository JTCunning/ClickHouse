#!/usr/bin/env python3
"""Set an outbox item's status (and optional fields) in index.json and README.md.

Usage: python3 outbox/set_status.py <slug> <status> [field=value ...]
Example: python3 outbox/set_status.py grid-gap-wraparound submitted submitted_head=4629104 upstream_pr=128500
Run from the root of the research repository. issue, upstream_pr and priority are stored as numbers.
"""
import datetime
import json
import pathlib
import re
import sys

root = pathlib.Path(__file__).resolve().parent
index_path, readme_path = root / "index.json", root / "README.md"

if len(sys.argv) < 3:
    sys.exit(__doc__)
slug, status, fields = sys.argv[1], sys.argv[2], sys.argv[3:]

index = json.loads(index_path.read_text())
if status not in index["statuses"]:
    sys.exit(f"unknown status {status!r}; known: {', '.join(index['statuses'])}")
item = next((i for i in index["items"] if i["slug"] == slug), None)
if item is None:
    sys.exit(f"no item {slug!r}")

item["status"] = status
for f in fields:
    key, _, value = f.partition("=")
    item[key] = int(value) if key in ("issue", "upstream_pr", "priority") and value.isdigit() else value
index["updated"] = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
index_path.write_text(json.dumps(index, indent=2, ensure_ascii=False) + "\n")

# README table: | # | [slug](...) | status | ...
lines = readme_path.read_text().split("\n")
pattern = re.compile(r"^(\|\s*\d+\s*\|\s*\[" + re.escape(slug) + r"\]\([^)]*\)\s*\|)\s*[^|]*?\s*(\|.*)$")
hits = 0
for n, line in enumerate(lines):
    m = pattern.match(line)
    if m:
        lines[n] = f"{m.group(1)} {status} {m.group(2)}"
        hits += 1
readme_path.write_text("\n".join(lines))
print(f"{slug}: {status} (index.json updated, README rows updated: {hits})")
