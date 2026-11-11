# tags-in-subquery-cold

Lab for `reports/2026-11-11-tags-in-subquery-cold.md`: a cold PromQL range query on an `s3` disk with a filesystem cache, against a local S3 mock that adds latency to every GET.

- `s3mock.py`: path-style S3 endpoint (GET with Range, HEAD, PUT, multipart, DELETE, ListObjectsV2), no auth. Latency per GET comes from `--get-latency-ms` or the file `<root>/.latency_ms` (re-read every 0.5 s). Every GET is logged as one JSON line.
- `server-config.xml.template`: a server with disk `s3` (endpoint `http://127.0.0.1:9100/bucket/data/`) and cache disk `s3_cache` (4 MiB default alignment), policy `s3`. Replace `$D` with a data directory.
- `gen.sh`: the dataset (64 inserts of 15 minutes, merges stopped, 21,560 active series with 2% pod churn per insert); writes `time_range.txt`.
- `query.sql.example`: the target query (its time range comes from `time_range.txt`).
- `run.py`: one case in a cache state (`cold`, `meta` = marks/primary keys/statistics in memory, data and text index not cached, `warm`), with `--keep` / `--evict` per file class, `--settings`, `--pre`. Maps every GET to (table, file kind) through the `s3` disk's metadata files and records the timeline.
- `matrix.sh`: every case of the report. `summarize.py`: the medians table. `stacks.sh`: one profiled cold run, stacks of the threads loading a primary index.

Pitfalls met while building it: Python's `http.server` needs `TCP_NODELAY` (headers and body are separate writes: +40 ms per response from Nagle and delayed ACK) and a listen backlog set as a class attribute (an instance attribute is read too late; a backlog of 5 drops SYNs under a burst, 1 s retransmits).
