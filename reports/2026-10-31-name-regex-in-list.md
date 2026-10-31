# Metric-name regex → `metric_name IN (...)`: not worth it as specified. The real cost is a row filter on the time ranges table
**Verdict:** Not worth building as asked. The primary key already narrows a regex with a literal prefix (`metric_1.*` reads only `[metric_1, metric_2)`), and rewriting such a regex into an `IN` list makes the handoff's own example **1.5× slower** on master. The cause of that slowdown is a per-row id-range filter on the time ranges table. It is fixed on branch `lilred/perf-time-ranges-id-ranges-index-hint` [unbuilt]: 100-name alternations get ~1.9× faster.
**Task:** 2026-10-31-handoff-rewrite-name-regex-to-a-met · handoff https://github.com/JTCunning/ClickHouse/issues/6 · **Build:** 26.11.1.974, `a7f713612c8` (branch base `26f120e95b5`, no change to the selector between them) · **Date:** 2026-10-31

## What happens (plain language)
- **What the handoff assumed:** a `{__name__=~"…"}` selector that isn't a plain list of names becomes `match(metric_name, '^re$')` and scans the whole tags table.
- **What actually happens:** ClickHouse's primary key analysis does use a regex's literal prefix.
  - `^metric_1.*$` becomes the key range `[metric_1, metric_2)`.
  - In the handoff's dataset every metric in that range matches, so the tags read is already exact.
  - `EXPLAIN indexes = 1` shows 136 of 245 granules for this prefix. A regex with a `(?:…)` group or no prefix gets no range.
- **Where the 1.5M rows come from:** the time ranges table.
  - It is read in full for a regex, because no per-metric id ranges are derived from one.
  - The samples come on top of that.
- **What an `IN` list would change:** today a literal list already gets one id range per metric on the time ranges table.
  - Those ranges are also evaluated as a row filter: two tuple comparisons per listed metric for every row read.
  - With 100+ names this filter, not I/O, dominates the query, so swapping the regex for an `IN` list of its 111 names costs time.

## Numbers
- **Dataset:** a scaled version of the handoff's repro (synthetic):
  - 200,000 series over 200 metrics; `metric_0` has 20,000 series, the others ~905 each
  - 6 labels; 240 samples at 15 s (4 h)
  - TimeSeries schema v11, every inner table merged to one part
- **Window:** the selector reads the last 5 min.
- **Settings:** `max_threads = 2`, local server, 2 vCPU.
- **Timing:** median of 5–6 warm runs (min–max).

**A. Regex vs. the literal `IN` list it would be rewritten to (master, full selector)**

| selector | metrics | median ms | min–max | read_rows | plan+opt ms |
|---|---|---|---|---|---|
| `metric_1` (control) | 1 | 24 | 23–85 | 23,938 | 14 |
| `{__name__=~"metric_1.*"}` (handoff's shape) | 111 | 593–617 | 577–1233 | 730,656 | 160 |
| same, as `IN` of the 111 names (= the proposed rewrite) | 111 | **885** | 879–893 | 722,464 | **456** |
| `{__name__=~"metric_1[0-9]"}` | 10 | 122 | 117–131 | 348,112 | 62 |
| same, as `IN` of the 10 names | 10 | 134 | 130–145 | 200,656 | 76 |
| `{__name__=~".*_19"}` (no literal prefix) | 1 | 37 | 32–43 | 392,578 | 24 |
| same, as `metric_19` | 1 | 26 | 25–38 | 20,546 | 15 |
| `{__name__=~".*19"}` (no literal prefix) | 2 | 42 | 41–52 | 363,841 | 25 |
| same, as `IN` of the 2 names | 2 | 38 | 37–40 | 40,961 | 21 |

Resolving the names first costs ~2 ms through the tags table's primary key. Going through `mergeTreeTextIndex` costs 21 ms, because it reads every token of the index.

So the rewrite gains only for regexes without a usable literal prefix that match few metrics: ~9 ms of 37 ms here. It loses wherever the regex matches many metrics.

**B. Where the `IN` list's time goes (tags lookup subquery alone, 111 names)**

| variant | median ms | min–max | read_rows |
|---|---|---|---|
| master: id ranges as a row filter on the time ranges table | 446 | 424–471 | 298,304 |
| id ranges wrapped in `indexHint` (index only) | 107 | 104–119 | 298,304 |
| no id ranges at all | 62 | 61–66 | 306,496 |

**C. The fix, emulated** (generated SQL with only the time ranges condition replaced; the selector's internal settings applied; interleaved runs; identical results by checksum; identical read_rows and marks)

| case | master | branch (emulated) |
|---|---|---|
| new perf test: 100-name alternation (200 metrics × 1000 series; `max_threads = 4`) | 437 ms (434–483) | 233 ms (221–244) |
| 111-name alternation, dataset above | 855 ms (834–895) | 648 ms (617–671) |
| 10-name alternation | 126 ms | 121 ms |
| one metric | 23 ms | 23 ms |

On the perf-test table the 100-name alternation is 2.5× slower than the equivalent regex on master (437 vs 173 ms). The fix closes most of that gap.

## Why (cause)
- **How the filter is built:** for a literal name or a list of names, `tryGetTimeSeriesIDRanges` derives one id range per metric: `(sipHash64(name), min UUID)` to `(sipHash64(name), max UUID)`. `addTimeRangesTableJoin` then adds `OR` of all those ranges to the `WHERE` of the time ranges subquery.
- **Why it costs so much:** the primary key needs these comparisons to skip granules, but they are evaluated again on every row read. That is 2 × N tuple comparisons per row, and with N = 111 it is ~340 ms of CPU on 200k rows.
- **The fix:**
  - keep the comparisons only as `indexHint` (the same trick the selector already uses on the samples side for whole-metric ranges)
  - filter the rows by `tupleElement(id, 1) IN (hashes)`, one hash lookup per row
  - Because a range spans the whole second id component, that filter is exactly equivalent.
- **Code pointers (`26f120e95b5`):**
  - `src/Storages/StorageTimeSeriesSelector.cpp:517–518` (`addTimeRangesTableJoin`)
  - `src/Storages/TimeSeries/TimeSeriesIDRange.cpp:205` (`makeIDInRangesCondition`) and `:225` (`makeIDOutOfRangesCondition`)
  - Introduced by #121924 (commit `91ed01c747e`, "Read the time ranges table by the id ranges if the metric name is a list of alternatives")
- **What KeyCondition does with regexes:** it serves `match` with a literal prefix and nothing else. A leading group (`^(?:…)`), a leading `.*` or case-insensitive flags get no range.

## Proposed fix
- **What it is:** branch `lilred/perf-time-ranges-id-ranges-index-hint`, a 9-line change in one function (S). It sits upstream of the handoff: it makes literal lists, which Grafana's metric browser and dashboards emit, cheaper on master today. It is also a prerequisite for any future name-resolution rewrite.
- **Tests:** stateless test `05479_time_series_selector_id_ranges_index_hint.sql`; it fails on master. New perf test `tests/performance/promql_selector_name_alternatives.xml`.
- **What it doesn't change:** no format change, no setting, no threshold.
- **Status:** unbuilt. The "after" numbers above are emulated.
- **Compare:** https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred/perf-time-ranges-id-ranges-index-hint
- **PR text:** https://github.com/JTCunning/ClickHouse/blob/lilred/research/prs/time-ranges-id-ranges-index-hint.md

**On the original ask.** If it's still wanted after this fix, limit it to regexes whose literal prefix is empty or much wider than the match (`.*_total`, `(?i)…`, `^(?:a|b).*`).
- Resolve the names with `SELECT DISTINCT metric_name FROM <tags> WHERE match(...)` (~2 ms). It beats the text-index dictionary (21 ms), because the tags table's `metric_name` is LowCardinality and sorted.
- The expected gain is a few ms to ~30% on narrow matches.
- It needs the cap the handoff mentions, and the cap would be a new threshold. That is why I didn't build it.

## Upstream overlap
- **Open `comp-promql` PRs checked:** #127848 (expand groups with literals in regex matchers: complementary, it creates more literal lists that this fix speeds up), #127847 (`ANY` join of the time ranges table: same function, different lines, no conflict in substance), #128177, #118801, #118116. None touches the id-range row filter.
- **Recent commits:** `91ed01c747e` (#121924, the origin of the filter); nothing since on this code.
- **James's fork branches:** none touch `addTimeRangesTableJoin`. The closest are `jtcunning/promql-selector-whole-metric-drop-id-set` and `promql-empty-selector-short-circuit`, both on other parts of the selector.

## How to reproduce
```sql
-- Table and data: the handoff's INSERT with numbers(200000) and `if(number < 20000, 0, (number % 199) + 1)`, T0 = T_END - 14400 (4 h).
-- Then OPTIMIZE TABLE <each .inner_id.*> FINAL.
SELECT count(), sum(length(samples))
FROM timeSeriesSelector(ts, '<selector>', toDateTime64(T_END - 300, 3), toDateTime64(T_END, 3), 'samples')
SETTINGS max_threads = 2, log_comment = '<tag>';
-- 1 cold + 5 warm per selector; system.query_log: query_duration_ms, read_rows, QueryPlanBuildMicroseconds + QueryPlanOptimizeMicroseconds.
-- Emulation: take the "Will execute query:" text from system.text_log (logger StorageTimeSeriesSelector), replace in it
--   or(and(greaterOrEquals(id, tuple(sipHash64('m'), ...)), ...))
-- with
--   indexHint(or(...)), not(notIn(tupleElement(id, 1), tuple(sipHash64('m'), ...)))
-- and run both with join_algorithm='full_sorting_merge', optimize_read_in_order=1,
-- merge_tree_min_bytes_for_concurrent_read=4194304, merge_tree_min_rows_for_concurrent_read=0.
-- Primary key on regex: CREATE TABLE t (m String, id UInt64) ENGINE=MergeTree ORDER BY (m, id); fill with 200 names;
-- EXPLAIN indexes = 1 SELECT count() FROM t WHERE match(m, '^metric_1.*$');   -- Condition: m in ['metric_1', 'metric_2')
```
