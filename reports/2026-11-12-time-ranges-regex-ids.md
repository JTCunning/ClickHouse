# Non-literal `__name__` regex: limit the time ranges read to the matched ids (handoff #17)
**Verdict:** The proposed `id IN (ids from the tags table)` filter works, and it's simple. But it trades one shape against another: it makes the ids query about 3× faster for a regex matching under ~1% of series, is flat at ~10%, and is 17–37% slower above that. That includes +16% on the regex control of the in-tree perf test `promql_selector_name_alternatives.xml`. No branch yet: the regression-free route needs upstream #115482 first. Three options for James are below.
**Task:** 2026-11-05-handoff-limit-the-time-ranges-read-for- · handoff https://github.com/JTCunning/ClickHouse/issues/17 · **Build:** 26.11.1.2075, `1e2fc59b8f1c` (master 2026-11-12) · **Date:** 2026-11-12
**A/B plan:** [ab-plans/time-ranges-regex-ids.md](../ab-plans/time-ranges-regex-ids.md)

## What happens (plain language)
- A selector like `{__name__=~".*cpu.*"}` has no metric id ranges, so the time ranges side of the selector join is `SELECT id, 1 AS matched FROM time_ranges WHERE max_time >= <start> AND min_time <= <end>`.
- That table is ordered by `id` and has no partition key, so the time filter prunes nothing. Every row is read, and the merge join throws away all but the matched ids.
- Adding `id IN (SELECT id FROM tags WHERE <the __name__ matchers>)` lets the primary key skip the granules of the other metrics. A metric's series sit together in that table, because `id` starts with `sipHash64(metric_name)`. For `.*cpu.*`, rows read drop from 2,100,000 to 65,536, exactly what master's literal-name path reads for the same 4 names.
- **The catch:** the set holds one entry per matched series. Building it costs about as much per series as the time-ranges row it saves (about 0.25 µs per id here, plus a second scan of `metric_name` in the tags table). So the set only pays off when the regex is selective. When the matched metrics cover most granules, it prunes nothing and is pure overhead.

## Numbers
**Dataset (`bench/time-ranges-regex-ids/gen.sh`):**
- 2.1M series: 400 metric names × 5,000 series, one sample each in a 1 h window; 3 of the names contain `cpu`.
- Plus 100k series of 20 other names whose only sample is one day earlier, so a wrong time filter would show up.
- Tags: `job` (10 values), `instance` (500), `region` (7). One part per inner table, default v11 TimeSeries table.

**Setup:**
- 2 vCPU, local SSD, `max_threads = 2`, query condition cache off.
- **Ids query:** the query behind `/api/v1/series`, `/api/v1/label/<name>/values?match[]=…` and the tags subquery of every PromQL selector. Master's version is captured from its `Will execute query` debug line; the id set is written in by hand.
- Median of 7 interleaved warm runs. Results are identical in every case (count plus a hash of the ids).

| selector | series matched | master | with the id set | time-ranges granules read |
|---|---|---|---|---|
| `{__name__=~".*cpu.*"}` | 15k (0.7%) | 222 ms | **77 ms (−65%)** | 257 → 8 |
| `{__name__=~".*cpu.*", instance="host-1"}` | 30 | 199 ms | **54 ms (−73%)** | 257 → 8 |
| `{__name__=~"node_cpu.*\|process_cpu.*"}` | 10k | 213 ms | **61 ms (−71%)** | 257 → 4 |
| `{__name__=~"metric_1[0-9]_total"}` | 50k (2.4%) | 250 ms | 144 ms (−42%) | 257 → 17 |
| `{__name__=~"metric_[1-4][0-9]_total"}` | 200k (9.5%) | 552 ms | 554 ms (0%) | 257 → 63 |
| `{__name__=~"metric_[1-9][0-9]_total"}` | 450k (21%) | 1,022 ms | 1,200 ms (+17%) | 257 → 124 |
| `{__name__=~"metric_1.*"}` | 550k (26%) | 1,286 ms | 1,519 ms (+18%) | 257 → 142 |
| `{__name__=~".+"}` | 2M (all) | 4,574 ms | 6,287 ms (+37%); memory 1,079 → 1,211 MiB | 257 → 257 |
| `{job="job-3"}`: no `__name__` matcher, guard | 200k | 950 ms | 925 ms (not applied) | 257 → 257 |

- **End to end through master's own HTTP handlers** (`api_bench.py`, 7 rounds): `/api/v1/label/__name__/values?match[]={__name__=~".*cpu.*"}` takes 208 ms and `/api/v1/series` 215 ms. With the 3 names written as a literal list, master's id-range path takes 55 ms and 64 ms. That is the order of the gain the set gives at this selectivity.
- **In-tree perf test `promql_selector_name_alternatives.xml`** (`perfxml_emulation.py`):
  - Setup: its own fill, 200k series × 20 samples, 4 streams, median of 9.
  - Regex control `m_1[0-9][0-9]` (100 of 200 metrics): master 192 ms, with the id set 222 ms (+16%), read rows 420,992 → 527,488.
  - The set can't prune there: each time-ranges granule holds about 8 metrics.
  - The table function on master matched the captured SQL (196 ms), so the emulation is faithful.

**Rejected variants:**
- **The set built from all the matchers** (the handoff's idea as written): +38% for `{job="job-3"}`, because its subquery re-reads the tags map. Same as the name-only set everywhere else.
- **A set of metrics instead of series** (`tupleElement(id, 1) IN (SELECT tupleElement(id, 1) FROM tags …)`): costs 0–2% at every selectivity but gains nothing. Primary key analysis doesn't see through `tupleElement`, so all 257 of 257 granules are still read.
- **Resolving the regex to the metric names first and taking the literal path** (the way Prometheus's index resolves a regex on a label):
  - The lookup is cheap: 4–18 ms here, since `metric_name` is LowCardinality.
  - `.*cpu.*` end to end: 208 → 55 ms.
  - But +7–8% on broad regexes (110 and 419 names).
  - On a 20,000-name table it fails at 11,111 names with `Query tree is too big. Maximum: 500000` (`max_expanded_ast_elements`).
  - So it needs a cap on the name count, which is a new threshold.

**Side observation, not part of this handoff:** on this box, the 100-name list in the same perf test is slower than the regex over the same metrics: 259 vs 196 ms. That's the list path's per-row conditions at 50% selectivity; the October report [name-regex-in-list](2026-10-31-name-regex-in-list.md) saw the same mechanism. Worth one line in the next A/B run of that XML.

## Why (cause)
- **No ranges for a regex:** `tryGetTimeSeriesIDRanges` (`src/Storages/TimeSeries/TimeSeriesIDRange.cpp`, `tryGetMetricNames`) builds ranges only for `=` or for a regex that `tryGetLiteralAlternatives` turns into a list. Any `.`, `*`, `[` and so on gives none.
- **The time filter alone prunes nothing:** `addTimeRangesTableJoin` (`src/Storages/StorageTimeSeriesSelector.cpp:498-560`) then puts only the time conditions on the time ranges subquery. Default tables have `ORDER BY id` and no partition key there.
- **Where the set's cost goes:** it is a `Set` of `Tuple(UInt64, LowCardinality(UUID))`.
  - Building 2.1M entries takes 0.49 s, including the tags read, at about 63 B per id.
  - The per-row check takes 0.2 s per 2.1M time-range rows.
  - The ordered copy for index analysis is built in place during planning.
- **Pruning is per metric, but master can only prune with per-series sets:** per-metric pruning needs either constant id ranges (the literal path) or a key condition on the first component of `id`. The first is limited by query size; the second isn't supported (`tupleElement` has no monotonicity information). So a per-series set is the only thing today's primary key analysis can prune with.

## Proposed fix: three options, James decides
- **A (recommended): two steps, no regression.**
  1. Upstream [#115482](https://github.com/ClickHouse/ClickHouse/pull/115482) (alexbakharew, `comp-mergetree`, open, last activity 2026-10-19) makes the primary key index serve conditions on the first element of a tuple-typed key column (`coord.1 <= N`, `tupleElement(coord, 1) = 5`) through the monotonic-functions chain. Its tests cover comparisons, not `IN` sets; this use needs an `IN` case.
  2. Then the TimeSeries change is about 20 lines: `tupleElement(id, 1) IN (SELECT tupleElement(id, 1) FROM tags WHERE <__name__ matchers>)` in the time ranges subquery.
  - Effect: the set has at most one entry per metric name (measured overhead 0–2% on broad regexes), and the pruning should match the id set's (8 of 257 granules for `.*cpu.*`). That second part is a prediction that needs a build.
  - Effort: S once #115482 is in. It doesn't depend on the id generator: it uses the first components the tags rows actually have.
- **B: the id set as asked.**
  - Apply it only when there is a `=`/`=~` matcher on `__name__` and no id ranges, and build it from the `__name__` matchers only. That keeps label-only selectors unchanged.
  - Branch-ready in one run: about 25 lines, a stateless test, and a target query in `promql_selector_name_alternatives.xml`.
  - The cost: regexes matching more than ~10% of series get slower, and CI's perf comparison would show the XML's regex control at about +16%. The review bot flags exactly that ("the perf claim contradicts an in-tree benchmark").
  - Also needed: 05493's pruning check uses that regex as its "no pruning" baseline, so it would switch to a selector without a `__name__` matcher.
  - Grafana autocomplete sends broad regexes on the first keystrokes and narrow ones later, so users would see both sides.
- **C: park it** until #115482 moves.

No `FORMAT_VERSION` or state change in any option. Correctness of A and B:
- The left join keeps a series only if a time-ranges row with its id matched. Every tags row that passes the outer `WHERE` also passes the `__name__` matchers, so its id is in the set.
- The set and the outer read take separate snapshots of the tags table. A series inserted between the two can be missed, which looks the same as an insert just after the query. I haven't checked which snapshot comes first.
- Series with ids from another id generator stay time-filtered, unlike on the literal path, because the set holds their real ids.

## Upstream overlap
- [#127847](https://github.com/ClickHouse/ClickHouse/pull/127847) (valerypetrov, open): makes the time ranges join `LEFT ANY`. It changes the join strictness, not the rows read. It touches the same lines of `addTimeRangesTableJoin`; the conflict would be trivial.
- [#127095](https://github.com/ClickHouse/ClickHouse/pull/127095) (James, open): `makeSelectFromTagsTable` calls `applyTimeRangeFilterToSelectIDsQuery`. Options A and B need the matchers and the tags table inside it, so his call gains two arguments, and the compiler flags it at merge. I didn't edit `PrometheusHTTPProtocolAPI.cpp`.
- [#127848](https://github.com/ClickHouse/ClickHouse/pull/127848) (valerypetrov, open): expands regex groups with the literals around them, so some regexes become literal lists with id ranges. Complementary.
- [#129064](https://github.com/ClickHouse/ClickHouse/pull/129064) (James, merged 2026-11-11): the id-range `indexHint`; its test 05493 is the one named under B.
- James's fork branches `prom-label-metadata-column-scan`, `prom-labels-direct-tags`, `promql-empty-selector-short-circuit` and `promql-selector-whole-metric-drop-id-set` don't touch this path.

## How to reproduce
`bench/time-ranges-regex-ids/README.md`. In short: start a server with `server-config.xml`, run `gen.sh`, then `bench.py <clickhouse> 19000 7 out.jsonl` (ids query, master vs id set), `api_bench.py` (HTTP endpoints, regex vs literal list), `time_ranges_rows.py` (rows read from the time ranges table) and `perfxml_emulation.py` (the in-tree regex control). The ids query as master builds it, for `.*cpu.*`:

```sql
SELECT timeSeriesStoreTags(id, tags, '__name__', metric_name) AS series_id
FROM <tags table>
ALL LEFT JOIN (SELECT id, 1 AS matched FROM <time ranges table>
               WHERE max_time >= toDateTime64(1700000000, 3) AND min_time <= toDateTime64(1700003599, 3)
               /* option B adds: */ AND id IN (SELECT id FROM <tags table> WHERE match(metric_name, '^.*cpu.*$'))
              ) AS time_ranges_subquery USING (id)
WHERE match(metric_name, '^.*cpu.*$') AND time_ranges_subquery.matched = 1
SETTINGS join_algorithm = 'full_sorting_merge', optimize_read_in_order = 1
```
