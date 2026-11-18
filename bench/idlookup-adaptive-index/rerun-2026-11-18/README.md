# Rerun 2026-11-18: James's T1–T4 end to end on the 2 vCPU lab (handoff #20, run 1 failed)

Why: the built branch was 15–40% slower prewarmed on James's 16-core host, while the emulation (the ids query timed alone) said ±2%.
This reruns his four selectors through `prometheusQueryRange` (step 30 s, the last 3 h of the data) on the cold S3 lab
(`../../tags-in-subquery-cold/`, `gen.sh` rerun first: 64 parts per inner table), master `f0a9e6bffa71`.

- A = master. B~ = master with `query_plan_optimize_join_order_limit = 0` on the outer query. The PromQL SQL has one join only,
  the selector's time ranges join, and this table runs no whole-metric probe, so the setting reaches exactly what the branch changes.
- `e2e.sh`: `STATE=meta|cold N=<rounds>`, A and B interleaved with the order alternated per round, through `run.py`.
  `e2e-own.sh`: prewarmed, with `SYSTEM DROP STATISTICS CACHE` before each variant's prewarm query, so that the cache holds only that
  variant's own loads (as on two servers). `summ.py <jsonl>`: medians.
- `q/t1.sql` … `q/t4.sql`: the queries. The window is fixed to the data of this run: regenerate the data and rewrite the window together.

| state | T1 A → B | T2 | T3 | T4 |
|---|---|---|---|---|
| cold, 3 rounds (`cold.jsonl`) | 3,241 → 2,988 ms (−7.8%) | 2,269 → 2,122 (−6.5%) | 4,405 → 4,124 (−6.4%) | 3,525 → 3,226 (−8.5%) |
| prewarmed, shared statistics cache, 7 rounds (`meta-shared-cache.jsonl`) | 1,377 → 1,388 (+0.8%) | 845 → 836 (−1.1%) | 2,125 → 2,175 (+2.4%) | 1,273 → 1,257 (−1.3%) |
| prewarmed, own statistics cache, 5 rounds (`meta-own-cache.jsonl`) | 1,381 → 1,362 (−1.4%) | 832 → 824 (−1.0%) | 2,088 → 2,133 (+2.2%) | 1,230 → 1,255 (+2.0%) |

Statistics loaded cold: A 92 / 36 / 167 / 90, B 0 / 0 / 13 / 0 (James's B: 0 / 0 / 13 / 0). `SelectedParts`, `SelectedMarks` and S3 GETs
are the same on A and B in every state.
