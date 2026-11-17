# A/B plan: label value regex vs. the `IN` list it would become
**Experiment:** ../reports/2026-11-01-label-regex-in.md · **Branch:** none yet: decides whether a branch is worth building · **Task:** 2026-10-31-handoff-rewrite-label-value-regex-to-in (handoff #7)
**Claim:** Rewriting `metric_0{instance=~"host-1.*"}` into `instance IN (<values from the text index dictionary>)` is not worth a branch: at 16 threads and up to 1M series in one metric, the `IN` form plus its value lookup is at most ~5% faster than the regex end to end (111 values), and at most ~10% (10 values). A branch is worth it only if the net gain reaches 10% on a common query.
**Evidence so far:** 2 vCPU / 4 GiB, `max_threads = 2`, 300k series (100k in `metric_0`), one part per table.
- 2026-11-01 on `a7f713612c8`: −4–14% end to end before the lookup, ~3–12% after it.
- 2026-11-17 smoke on master `f0a9e6bffa71` (`bench/label-regex-in/smoke-2026-11-17.txt`): the gain is gone for 111 values (selector 123 → 125 ms, `sum(rate)` 169 → 172, instant 160 → 160; all inside the noise) and smaller for 10 values (115 → 111, 124 → 121, 131 → 120).
- Inside the real selector, the id set (built in place during plan optimization) costs the same for both forms (plan time 50.4 vs 52.8 ms and 45.2 vs 41.8 ms) and reads the same bytes (±0.6 MiB). The extracted tags subquery alone still shows −19 ms, so on this master the extracted subquery no longer stands for what the selector runs.
- Master's dictionary scan (upstream #121650, merged 2026-11-02) does not serve `match`, `LIKE` or `startsWith` on a `keyValuePairs` Map element: 0 automaton builds, 0 posting lookups.
- Unproven: the shares at 16 threads (the samples read shrinks, the id set build may not) and at 1M series in one metric (the handoff says the gap grows with series count).

## Builds
- One build: A = master at the base sha (any master at or after `74bc5daa929`, so it includes #121650, #127847 and #131678). Release build, no sanitizers. No branch: A and B are two query shapes on the same server.
- If a text index side change exists by then (the `keyValuePairs` tokenizer added to the dictionary scan for `match` on a Map element, a follow-up to #121650), it becomes build C. Run the guards against it as a normal two-server A/B.

## Machine
- 16 vCPU / 64 GiB, local NVMe, nothing else running. Note the CPU model.
- 16 vCPU because the claim depends on how much of the selector the id set build takes once the samples read runs on 16 threads. Before trusting a 16-thread number, check that `EXPLAIN PIPELINE <q> SETTINGS max_threads = 16` shows `× 16`. 64 GiB keeps `max_threads_min_free_memory_per_thread` from capping the streams.

## Data
- Generator: `bench/label-regex-in/gen.sh`, which runs the handoff #7 `INSERT`. Six labels: `job` (10 values), `instance` (500), `cluster` (5), `namespace` (20), `pod` (one per series) and `mode` (8). There are 200 metrics. 15 s scrape, `T_END` from `bench/label-regex-in/t_end` (2026-11-01).
  - That date is older than `recent_samples_ttl_seconds` (4 days), so the recent samples table stays empty and every case reads the samples table.
  - `samples_index_granularity_bytes` is pinned with `GRAN` (default 262144, the master default since #131678). Every inner table gets `OPTIMIZE FINAL` to one part.
- Sizes:
  - S: `SERIES=300000 WIDE=100000 SAMPLES=40` (the report's data; 40 MiB, 6 s to load on 2 vCPU)
  - M: `SERIES=1000000 WIDE=100000 SAMPLES=240` (the handoff's size)
  - L: `SERIES=3000000 WIDE=1000000 SAMPLES=40` (one metric with 1M series, where the handoff expects the largest gap)
- Exact command for M: `CH=<build>/clickhouse PORT=19000 THREADS=16 SERIES=1000000 WIDE=100000 SAMPLES=240 bench/label-regex-in/gen.sh`. Record the `system.parts` lines it prints.

## Cases
B is always the literal alternation of the values that A's regex matches. `tryGetLiteralAlternatives` already turns it into `IN`, so B is exactly the rewrite's output. Get the values with R1 and save them as `bench/label-regex-in/alt<N>-<size>.txt`; `alt111.txt` is the S list. "Net B" = B + R1.

| case | query | role | predicted A → net B |
|---|---|---|---|
| T1 | `metric_0{instance=~"host-1.*"}` (22% of the metric) vs. B of its 111 values: selector, `sum(rate(x[5m]))`, instant `x` | target | S/M at 16 threads: 0 to −5% |
| T2 | `metric_0{instance=~"host-1[0-9]:9100"}` (2%) vs. B of its 10 values: same three forms | target | −3 to −10% |
| T3 | T1 on L (222k matching series) | target, largest scale | up to −10%; the case that decides |
| T4 | `sum by (job) (rate(metric_0{instance=~"host-1.*"}[5m]))` vs. B, through `prometheusQuery` | target, common query | same as T1 |
| R1 | `SELECT DISTINCT token_value FROM mergeTreeTextIndex(default, <tags table>, tags_idx) WHERE startsWith(token, 'instancehost-1') AND token_key = 'instance' AND match(token_value, '^host-1.*$')` | B's extra cost | 2–3 ms on S; record M and L |
| R2 | R1 with `startsWith(token, 'instance')` only (a regex without a literal prefix) | cost | 2–3 ms on S (the `instance` slice is small) |
| G1 | `promql_selector_name_alternatives.xml`, the regex control `{__name__=~"m_1[0-9][0-9]"}` | guard (build C only) | unchanged |
| G2 | `promql_selector_pk_range.xml`, `sum(pkr_metric{dc=~".*"})` (matches everything, including empty) | guard (build C only), must not be rewritten | unchanged |
| G3 | `promql_selector_pk_range.xml`, `sum(pkr_metric{dc="0"})` | guard (build C only) | unchanged |
| E1 | `metric_0{pod=~"pod-1.*"}`: 11,111 matching series in `metric_0`, but R1 over the `pod` slice resolves 111,111 values on S (the dictionary is not per metric) | edge: high cardinality | net B slower: R1 27–57 ms on S, more on M/L |
| E2 | `metric_0{instance=~".*:9100"}` | edge: no literal prefix | a rewrite must skip it; R2 on S 2–3 ms, the `pod` analogue 50–57 ms |
| E3 | `metric_0{instance=~"host-1.*\|"}` | edge: matches the empty string (series without the label) | a rewrite must skip it; A only |
| E4 | resolve T1's values (R1), insert one series `instance="host-1999:9100"` into `metric_0`, then run B and A | edge: two snapshots | B misses the new series and A returns it (the behaviour change the report names) |

## Method
- One server on port 19000 with the data above. Check `system.parts` after start.
- Interleave A and B round by round with `bench/label-regex-in/ab.py`, so both see the same cache state. Each round runs the selector, `sum(rate)` and instant forms of every shape given. Use `MAX_THREADS=2`, `8` and `16`, and 30 rounds (the queries take ~100 ms). Example: `MAX_THREADS=16 python3 ab.py 30 'metric_0{instance=~"host-1.*"}' "metric_0{instance=~\"$(cat alt111.txt)\"}" > ab-M-16.jsonl` (`ab.py` drops round 0 as warm-up).
- Significance, as the CI comparison does it: take the paired per-round differences from the `walls` arrays and bootstrap their median 10,000 times. The change is significant if the 99.5% interval excludes 0 and the median is at least 5%. `python3 -c "import json,random,statistics as s,sys;a,b=[json.loads(l)['walls'] for l in open(sys.argv[1])][:2];d=[y/x-1 for x,y in zip(a,b)];m=sorted(s.median(random.choices(d,k=len(d))) for _ in range(10000));print(s.median(d),m[25],m[-26])" <file>` (lines 1 and 2 of the file must be the same form of A and B).
- R1/R2: 30 runs each, median, `max_threads = 16`. Run E4 by hand once, and paste the two counts.
- Don't use `bench.py`'s extracted tags subquery as evidence until its settings match `applySettingsForSelectIDsQuery` at the base sha (see smoke C). The real selector's plan time is the number that counts.

## Metrics
- Wall (median, p95), user CPU (`ProfileEvents['UserTimeMicroseconds']`), `read_rows`, `read_bytes`, `memory_usage` from `system.query_log`.
- `plan_ms` = `QueryPlanBuildMicroseconds + QueryPlanOptimizeMicroseconds`: the id set is built in place there, so this shows the lookup inside the real query.
- The counter that proves B took the direct read: `TextIndexPostingsCacheHits + TextIndexPostingsCacheMisses` = 0 for A, the number of values for B. For build C: `TextIndexDictionaryDFABuilds` > 0 on A's own regex.

## Correctness
- A and B return identical rows for T1–T4 and E1–E2: `SELECT * FROM timeSeriesSelector(...) ORDER BY id FORMAT TSV | md5sum` for both shapes.
- E4 shows the expected difference of exactly one series.
- Build C only: a stateless test where a regex on a Map element returns the same rows with `use_text_index_like_evaluation_by_dictionary_scan` 0 and 1, including a value that matches only through the regex's alternation.

## Pass / fail
- **A branch is worth building** if net B is at least 10% faster than A, significant, at 16 threads, on T3 or T4, for both the selector and `sum(rate)`, with E1/E2 shown to be skippable by a cheap guard. Then write the branch with the guards from the report (literal prefix, value cap, positive matchers only, no empty match) and give it its own plan.
- **No branch** (the predicted result) if T1–T4 stay below 10% net. Mark row 8 "no gain at scale" with the numbers, and add an "A/B result" line to the report's verdict. The idea that remains is on the text index side (build C). Offer it to James only if A's `plan_ms` on T3 at 16 threads is ≥ 10% of the selector's wall time, because otherwise the per-row regex isn't worth an index change either.
- If T1 at S on 16 threads contradicts the 2 vCPU smoke (net B ≥ 10%), re-check smoke C first: the extracted-subquery gap may be real inside the selector on more threads.

## Checklist
- [ ] build master; record the sha and the CPU model
- [ ] `gen.sh` at S, M and L; record the parts and sizes
- [ ] R1 for each size; save the `alt<N>-<size>.txt` lists
- [ ] `ab.py` for T1–T4 and E1–E2 at 2, 8 and 16 threads; save the JSON lines under `bench/label-regex-in/ab/`
- [ ] the bootstrap for every A/B pair; E4 by hand
- [ ] update the report verdict and the backlog row
