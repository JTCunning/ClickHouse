# Lab: text index, statistics and join planning in the id lookup (handoff #20)

Report: `../../reports/2026-11-14-idlookup-adaptive-index.md`. Runs on the cold S3 lab `../tags-in-subquery-cold/` (S3 mock at 50 ms per GET,
`s3` disk under a 4 MiB cache, `gen.sh` = 64 fresh parts per inner table). The lab server resumes merges when it restarts: run `gen.sh` again
after every start (and `capture.py` again: the time range moves with it).

- `capture.py <port> <table> <t0> <t1> name=selector …`: the ids query of each selector as master builds it (from the `Will execute query`
  debug line), into `q/<name>.sql`. `q/*.explain.sql` = `EXPLAIN indexes = 1` of the same.
- `matrix-cold.sh`: selectors `nc nr w lf` × states cold / meta (prewarmed) / warm × variants base, noidx, nosp, nojo, nostat, combo through
  `../tags-in-subquery-cold/run.py`, plus 3 `EXPLAIN` runs per cell for the planning share. ~40 min. `summarize20.py results20.jsonl [--md]`.
- `warm.py <port> warm|dropped <rounds> q <sels> [variants]`: interleaved rounds on a local-disk or warm server (`results20-warm15.jsonl`).
- `gap.py <state> '<selector>' [k=v,…]`: the full PromQL query with trace logs and the real-time profiler; prints the timeline of the ids
  query (planning end → `Creating set` → `Created Set`) and the top symbolized frames per phase (`gap-out/`).
- `equality.txt`: count/hash of the ids per selector for the six variants (identical).
