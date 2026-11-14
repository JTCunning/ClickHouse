# A/B plan: does a plain UUID series id still beat LowCardinality(UUID) at realistic scale?
**Experiment:** ../reports/2026-11-01-lowcardinality-uuid-id.md · **Branch:** none yet: verifies a report conclusion (the default flip is a one-line branch, written only if James asks for it) · **Task:** 2026-10-31-timeseries-id-what-lowcardinalityuuid
**Claim:** with the series id declared as `Tuple(UInt64, UUID)` instead of the v11 default `Tuple(UInt64, LowCardinality(UUID))`, on 16 vCPU: the in-tree `promql_selector_pk_range` queries run **20–40% faster**, a one-series selector 10–20% faster, a ~1k-series metric 5–10% faster, and wide queries over 100k–1M series 0–5% faster (never slower). Remote-write ingest takes **5–10% less server CPU per sample** at 1, 4 and 8 senders, merges included. The id column is no larger on disk.
**Evidence so far:** measured on 2 vCPU / 4 GiB at `max_threads` 2 only, on build a7f713612c80 (2026-11-01), with every inner table merged to one part: pk_range LC/plain 1.73, 1.75, 1.52, 1.66; narrow 1.09–1.22; wide 1.00–1.05; ingest −7% CPU per sample (1 sender, paired median of 5 runs that were ±20% noisy because merges spilled across runs). A smoke run of this plan's scripts on 2026-11-14 (master 26.11.1.2253, f0a9e6bffa71; 2 vCPU, tiny servers, 10 iterations) still gave pk_range LC/plain **1.27, 1.37, 1.47, 1.59**, three of four significant at 99.5%, identical results, and a 22% larger LowCardinality id column on the samples table. Unproven: whether the per-range dictionary cost holds at 16 streams (on 2 vCPU it showed as 2.7× system time and 2.5× page faults, which may be contention), whether the gap holds with many unmerged parts, at 1M series, and with 4–8 senders.

## Builds
- One build, current master, release (as the CI performance job). No branch: both id types can be declared on master (`TAGS INNER COLUMNS (id Tuple(UInt64, UUID))` propagates to every inner table). Every server runs the same binary. Record the version and git hash.
- Check first that the default is still LowCardinality: `src/Storages/TimeSeries/normalizeTimeSeriesDefinitionImpl.cpp:486-487` (it was at 5157e6d4e02c, 2026-11-14; table version still 11). If upstream changed the default, the experiment is moot: mark the row and stop.

## Machine
- 16 vCPU, 64 GiB RAM, local NVMe. Why: pk_range and the wide selectors build up to 16 read streams, and the LowCardinality cost is paid per read range per stream, so the claim is about a host where that work is not fighting for 2 cores. The widest case (1M series, range query) needs about 10 GiB. Ingest at 8 senders needs the load generator on cores of its own (`taskset -c 12-15` for `rw.py`, the servers on 0–11).
- Nothing else running. Note the CPU model. Pass `max_threads_min_free_memory_per_thread=0` (the scripts do) and confirm `× 16` in `EXPLAIN PIPELINE` once.

## Data
Scripts: `bench/lc-uuid-id/ab/` (`server.sh` starts one server per variant from `config.xml.in`; `ab.py` fills, checks and runs). The fill is single-threaded with fixed 16,384-row blocks, so every server gets the same parts; `ab.py check` prints rows, parts and id-column bytes per inner table and stops if the servers differ. The dataset ends at the fill time (`out/t_end`), so the recent-samples table is populated (a fixed old end time lets its 4-day TTL empty it).

Own dataset (the report's generator: 6 labels, `pod` unique, `job` 10 values, 30 s scrape; `WIDE` series in `metric_0`, the rest over `metric_1` … `metric_49`):

| size | command | why |
|---|---|---|
| report | `ab.py fill-own 150000 100000 480` (4 h) | same data as the report, so the numbers line up |
| realistic | `ab.py fill-own 1000000 100000 960` (8 h) | 1M series: wider tags and time ranges tables, ~18k series per other metric |
| wide | `ab.py fill-own 2000000 1000000 240` (2 h) | 1M series in one metric: the wide cases at the size where LowCardinality's smaller in-memory id could matter |

Each size twice: `MERGED=1` (every inner table `OPTIMIZE FINAL`, the report's layout) and `MERGED=0` (`SYSTEM STOP MERGES` before the fill: about 10–120 parts per inner table, closer to a live remote-write table). In-tree tests: `ab.py xml <name>` uses each XML's own CREATE and fill, adds the plain id on B, then runs its queries.

Ingest: `bench/remote-write-baseline/` (`rw.py`, `run.sh`, `server-config.xml`). Create the plain table as `CREATE TABLE prometheus_uuid ENGINE = TimeSeries TAGS INNER COLUMNS (id Tuple(UInt64, UUID))`, not from `variant-plain-uuid-id.sql`: that file is a 2026-11-01 `SHOW CREATE` copy and would also pin that day's other defaults.

## Cases
Predictions for B (plain) against A (LowCardinality), at 16 threads. The report's 2 vCPU LC/plain ratio comes first.

| case | query | role | predicted A → B |
|---|---|---|---|
| T1 pk_range | `ab.py xml promql_selector_pk_range`: `pkr_metric`, `sum(pkr_metric{dc=~".*"})`, `sum(rate(pkr_metric[30m]))`, `sum(pkr_metric{dc="0"})` (20,000 series, one granule each) | target | 1.73 / 1.75 / 1.52 / 1.66 → **−20% to −40%** on each |
| T2 one series | own `metric_0{pod="pod-5"}` instant | target | 1.22 (28 → 23 ms) → **−10% to −20%** |
| T3 small metric | own `sum(rate(metric_7[5m]))` (~1k series at report size, ~18k realistic) | target | 1.09 → −5% to −10% |
| T4 tags transform | `ab.py xml promql_tags_transform` (3 TimeSeries queries) | target | 1.08–1.12 → −5% to −10% |
| T5 ingest, 1 sender | `SETTLE=1 ./run.sh warm write 1 2000 1 20000 20` vs the same on `write_uuid`, 5 interleaved rounds | target | −7% (25.8 → 23.3 µs/sample) → **−5% to −10%** CPU per sample |
| T6 ingest, 4 and 8 senders | 1M active series, one full scrape per round: `SETTLE=1 ./run.sh warm write 4 2000 1 250000 125 --ts0 $((NOW + k*15000))` and `... write 8 2000 1 125000 62 ...`, each vs `write_uuid`; round 0 (first sight of the series) is the cold pass and is reported apart | target | not measured → −4% to −8% (LowCardinality is 12% of ingest CPU, 9 points of it in merges; more senders mean bigger parts and fewer merges) |
| T7 disk | `ab.py check` after each fill | target | id column compressed: recent samples 3.13 → 2.89 MiB (−8%), samples 2.92 → 2.81 MiB (−4%) at report size → B ≤ A on every inner table |
| G1 wide own cases | own `metric_0` instant, `sum(rate(metric_0[5m]))`, `sum by (job) (rate(metric_0[5m]))` 4 h step 60, `rate(metric_0[5m])` 1 h, `timeSeriesSelector` raw 4 h, `metric_0 + metric_0` | guard | 1.00–1.05 → 0% to −5%, never slower |
| G2 label filter | own `sum(rate(metric_0{job="job3"}[5m]))` (10%) | guard | 1.05 time → −3% to −6%; memory 27 → 30 MiB (+11%), see E1 |
| G3 topk | `ab.py xml promql_topk` (topk, bottomk, topk by, limitk) | guard | 1.05 / 1.06 / 1.05 / 0.95 → unchanged to −6%; limitk unchanged |
| G4 set operators | `ab.py xml promql_set_operator_presence_mask` (and / or / unless, dense and sparse) | guard | 0.99–1.08 → unchanged to −7% |
| G5 id range row cost | `ab.py xml promql_selector_id_range_row_cost` | guard | 1.00–1.03 → unchanged |
| G6 name alternatives | `ab.py xml promql_selector_name_alternatives` (200k series; 100-name alternation, `m_100`, `m_1[0-9][0-9]`, each `max_threads = 4`) | guard | added upstream 2026-11-03, not in the report → unchanged to −5% |
| E1 memory, widest | G1 and G2 on the `wide` size: `memory_usage` per query | edge | LowCardinality holds the id in memory as a small index (25 vs 55 MiB decompressed on the report's recent-samples table), so B may use more memory: within +10% |
| E2 tune LowCardinality instead | server C: default id, started with `PROFILE_EXTRA='<low_cardinality_use_single_dictionary_for_part>1</low_cardinality_use_single_dictionary_for_part>'` (one dictionary per part instead of one per 8,192 keys; the writer takes it from the settings it is given, `MergeTreeIOSettings.cpp:108-109`, so a default-profile value should reach merges too: checked in Method); T1, T2, G1 as pairs (A, C) and (B, C) | edge | not measured. This is the reviewer's question "why not keep LowCardinality and tune it?". If C reads as fast as B, the report must offer the setting as an alternative |
| E3 many parts | T1–T3 and G1 with `MERGED=0` | edge | the dictionary is per part, so more parts should widen the gap: B at least as far ahead as with `MERGED=1` |

## Method
- Servers, all from the same binary, each on a fresh directory (raise the memory limits for this machine):
  `export SERVER_MEM=28000000000 QUERY_MEM=20000000000; ./server.sh start A 9001; ./server.sh start B 9002` (both run at once, so each gets less than half the RAM)
  (`server.sh stop A` / `stop B` at the end; it checks the pid is a clickhouse server before the kill.)
- Per size and layout: `MERGED=1 python3 ab.py fill-own <size>`, then `python3 ab.py own 40` (40 queries in total, about 20 per server; use 100 when the medians are under 100 ms). Then each XML: `python3 ab.py xml <name> 100`. `ab.py` checks the results first (exact, or 1e-9 relative for float sums), then runs `clickhouse benchmark` with both servers interleaved at `--confidence 5` (99.5%). Raw outputs go to `ab/out/`; copy them to `bench/lc-uuid-id/ab-<date>/` afterwards.
- `MAX_THREADS=16` (default) is applied where a query has no `max_threads` of its own; G6's `max_threads = 4` stays as the XML has it. One pass of T1 and G1 also at `MAX_THREADS=4`, to see whether the gap depends on the stream count.
- One A/A pass on the report size before the A/B: `./server.sh start A2 9004`, `PORTS=9001,9004 VARIANTS=lc,lc python3 ab.py fill-own 150000 100000 480 && PORTS=9001,9004 VARIANTS=lc,lc python3 ab.py own 40`. Any "difference proven" there is the method's false-positive rate; report it next to the results.
- E2: `PROFILE_EXTRA='<low_cardinality_use_single_dictionary_for_part>1</low_cardinality_use_single_dictionary_for_part>' ./server.sh start C 9003`, then `PORTS=9001,9003 VARIANTS=lc,lc` and `PORTS=9002,9003 VARIANTS=plain,lc` for `fill-own` (report size), `own`, `xml promql_selector_pk_range`. Confirm the setting took effect: C's id column bytes in `ab.py check` differ from A's. If they are identical, drop E2 and say so.
- Ingest (T5, T6): one server from `bench/remote-write-baseline/server-config.xml` holding both `prometheus` (default) and `prometheus_uuid`, as on 2026-11-01, now with `SETTLE=1`: `run.sh` waits for `system.merges` to empty before and after each run, so each run's CPU includes the merges it caused and none from the run before. Rounds alternate default / plain; at least 5 warm rounds per sender count; report the paired per-round change with its min–max. `rw.py` walks the universe round-robin in shards of 2,000 series, so a round of `universe / 2000` requests per sender sends every series once; `--ts0` moves one scrape (15 s) per round so no sample repeats. T5 keeps the report's shape (a 10-request cold run, then warm runs at `--ts0 $((NOW+1200000))` and later).

## Metrics
- Wall time: the median and p95 from `clickhouse benchmark` (one column per server) and its 99.5% verdict.
- Per query, on each server, from `system.query_log`: CPU, system time, page faults, memory, bytes read. These carry the mechanism: the report's 2 vCPU profile showed the LowCardinality reads as 2.7× system time and 2.5× page faults.
  ```sql
  SELECT normalized_query_hash, any(substring(query, 1, 80)) AS q, count() AS n, median(query_duration_ms) AS ms,
         median(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1000 AS cpu_ms, median(ProfileEvents['SystemTimeMicroseconds']) / 1000 AS sys_ms,
         median(ProfileEvents['SoftPageFaults']) AS faults, median(memory_usage) AS mem, median(read_bytes) AS rb
  FROM system.query_log WHERE type = 'QueryFinish' AND event_time > now() - INTERVAL 2 HOUR AND query NOT LIKE '%query_log%'
  GROUP BY normalized_query_hash ORDER BY q
  ```
- Mechanism, one pass of T1 with `query_profiler_cpu_time_period_ns = 1000000` and `allow_introspection_functions = 1`: the share of `system.trace_log` samples whose stack contains `SerializationLowCardinality` on A (predicted the bulk of A's extra time) and on B (zero).
- Ingest: CPU µs per sample, parts per request and peak RSS (the `run.sh` line).
- Disk: id-column compressed and uncompressed bytes per inner table (`ab.py check`).

## Correctness
- `ab.py` compares every case's result across servers before timing it; any `same_result=False` stops the conclusion for that case. `ab.py check` must show the same rows and parts per inner table on every server (it exits otherwise).
- No new test: nothing in the code changes. If the default-flip branch follows, `04891_timeseries_lowcardinality_id` keeps covering explicit LowCardinality ids, and the ~13 `.reference` files that print the default schema change with it.

## Pass / fail
- **Pass** (the report's recommendation stands): all four T1 queries at least 20% faster on B and significant at 99.5%; T2 and T4 at least 5% faster and significant; T5 and T6 at least 3% less CPU per sample, with the same sign in at least 4 of 5 rounds; T7 B ≤ A on every inner table; no guard case (G1–G6) significantly slower by more than 5% at either layout; identical results everywhere. Then: the report gets the 16 vCPU numbers in its header, and James gets the offer of the default-flip branch (`normalizeTimeSeriesDefinitionImpl.cpp:487`, the documentation tables in `StorageTimeSeries.cpp`, the gtest constant, the references). That branch would need no perf XML change: every in-tree XML above creates its table with the default id, so a CI comparison of master against the branch measures exactly this.
- **Fail**, by branch:
  - T1 under 10% at 16 threads: the 2 vCPU gap was mostly contention (system time, page faults). The report keeps the ingest and disk argument only, and the verdict becomes "small; the maintainers' call".
  - A guard slower on B, or E1 above +10% memory: LowCardinality's compact in-memory id matters at scale. The report adds the trade-off and recommends the change only for write-heavy tables.
  - T6 under 3% at 4–8 senders: the −7% belonged to tiny parts from one sender. Drop the ingest claim.
  - E2's C as fast as B on T1 and T2: a setting closes most of the gap without a schema change. The report offers both, setting first.

## Checklist
- [ ] confirm the default at `normalizeTimeSeriesDefinitionImpl.cpp:486-487`; record the build's version and hash
- [ ] A/A pass; record its false positives
- [ ] three sizes × two layouts: `fill-own`, `check`, `own`; record the fill times and the sizes on disk
- [ ] the six XMLs at `MERGED=1`, and T1, G6 again at `MERGED=0`
- [ ] the `MAX_THREADS=4` pass of T1 and G1; the trace_log pass of T1
- [ ] E2 (server C), or a note that the setting had no effect
- [ ] ingest T5, T6 with `SETTLE=1`
- [ ] save the raw outputs under `bench/lc-uuid-id/ab-<date>/`; stop every server
- [ ] update the report verdict and header, and this row of the backlog
