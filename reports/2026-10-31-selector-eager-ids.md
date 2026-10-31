# Resolve the PromQL selector id set eagerly and pass a literal IN (handoff #4)

**Verdict:** not worth building as specified. Running the tags lookup first and handing the samples read a ready id list moves the ~11 ms "plan+opt" cost out of the planning counter but not out of the query: one-series wall time stays at 13 ms, and from about 900 series up a literal list is 2–9× slower. The fixed cost of a narrow selector sits in the tags lookup itself, mostly its join with the time-ranges table (~5 of 13 ms), so fix that instead.
**Task:** 2026-10-31-handoff-resolve-the-promql-selector-id- · handoff https://github.com/JTCunning/ClickHouse/issues/4 · **Build:** 26.11.1.974, master `a7f713612c8` · **Date:** 2026-10-31

## What happens (plain language)
On v11 (bucketed) tables a selector runs as one query: read the samples table `WHERE id IN (<tags lookup>)`. ClickHouse builds the `IN` set *while planning* the samples read, because it needs the set to prune primary-key granules. That's why `QueryPlanBuild + QueryPlanOptimize` shows 11–12 ms for a one-series selector: about 8 ms of it is the tags lookup running, not the planner thinking. If the lookup runs as its own query first, the planning counter drops to ~1.5 ms, but the 8 ms lookup still has to run, so the total stays the same. Passing the ids back as a literal list also costs more than the set does today once there are more than a few hundred ids.

## Numbers
Dataset (synthetic, 1/5 of the handoff's): 200,000 series over 200 metrics (`metric_0` 20,000 series; `metric_1`…`metric_199` ~900 each), 6 labels, 240 samples per series at 15 s (1 h), v11 table with default settings, one part per inner table after `OPTIMIZE FINAL`. 5-minute window. Local server, `max_threads=2`, median of 5 warm runs (cold run dropped), all times in ms.

"Eager" is emulated on the master binary with hand-built SQL: run the generated tags subquery on its own, then run the generated samples query with that subquery replaced by (a) a literal id array, or (b) an in-memory temporary table filled in the same session. (b) behaves like a pre-built set (`FutureSetFromTuple`, the materialized CTE of upstream #118801, or the temp table on James's `jtcunning/promql-empty-selector-short-circuit` branch).

| selector | series | today: wall (min–max) | today: plan+opt | eager + literal array: lookup + samples = total | eager + set (temp table): lookup + samples = total |
|---|---|---|---|---|---|
| `metric_103{pod="pod-100000"}` | 1 | **13** (13–14) | 11.9 | 7 + 6 = **13** | 8 + 5 = **13** |
| `metric_1{job="job3"}` | 89 | **20** (20–32) | 14.1 | 8 + 13 = **21** | 9 + 11 = **20** |
| `metric_1` | 905 | **22** (22–26) | 14.9 | 8 + 38 = **46** | 9 + 12 = **21** |
| `metric_0{job="job3"}` | 1,976 | **97** (94–106) | 21.2 | 10 + 170 = **180** | 14 + 116 = **130** |
| `metric_0` | 20,000 | **108** (106–111) | 30.8 | 13 + 670 = **683** | 27 + 113 = **140** |
| `{job="job3"}` | 19,833 | **813** (795–839) | 74.8 | 43 + 1,618 = **1,661** | 87 + 692 = **779** |

- The handoff's target ("plan+opt of the one-series shape under 5 ms") is met by the samples query alone (1.5–1.8 ms), but wall time doesn't move.
- **Literal arrays scale badly:** the analyzer converts the array element by element (plan build reaches 83 ms at 20k ids), execution is slower too, and peak memory is 8× today's at 20k ids (230 MB vs 27 MB). Typing the literal as `LowCardinality(UUID)` to match the id column made it worse (984 ms at 20k).
- **Even a ready-made set is 30% slower** on `metric_0` and `metric_0{job="job3"}` than today's in-place build. Today the tags lookup reads through the merge join in id order, and that overlaps with the samples read.
- **Where the one-series 13 ms goes:** the tags lookup alone takes 7–8 ms with the time-ranges join and 2–3 ms without it (`metric_name = … AND tags['pod'] = …` straight on the tags table, `timeSeriesStoreTags` included). The time-ranges lookup alone takes 3 ms. A trivial `SELECT 1` takes 0 ms, so per-query overhead isn't the cost.

## Why (cause)
- `makeWhereFilterForSamplesTable` (`src/Storages/StorageTimeSeriesSelector.cpp:722`) emits `id IN (<tags subquery>)`. `FutureSetFromSubquery` builds that set during the samples table's index analysis, which happens inside plan optimization. So "plan+opt" measures the tags lookup, not the planner.
- The tags lookup for one series joins the tags table with a time-ranges subquery (`ALL LEFT JOIN … USING (id)`, `full_sorting_merge`, read in order; see `applySettingsForSelectIDsQuery` at line 1167). Two in-order reads plus a merge join over ~8k rows each cost about 5 ms of fixed setup, even when one row comes out.
- A literal list is the wrong carrier past a few hundred ids: every id becomes an AST node that the analyzer converts one by one, and the query text grows by ~60 bytes per series (1.2 MB at 20k).

## Proposed fix
Report only. Not for this handoff:
- **Don't build the eager + literal form.** Measured on master, it saves at best ~0–1 ms at 1–89 series and is 2–6× slower from ~900 series up. Holding it to a cap would add exactly the kind of threshold the review bot pushes back on, for no measurable gain.
- **The lever for narrow selectors is the time-ranges join** inside the tags lookup: ~5 of 13 ms at one series. Two candidate directions, both to measure first:
  - skip the time-ranges join when the matched set is small: probe `timeranges` by the matched ids instead of joining in order;
  - fold the time-range check into a point lookup per matched id.
  Both belong with handoff #9 (join cost of the tags subquery). Upstream #127847 (`ANY` join) only helps while time-ranges parts are unmerged; it doesn't touch this fixed cost on merged data.
- **The eager step is still the right place for #5** (one lookup per distinct selector in a query) and for the "empty selector → no samples query" short cut. James's `jtcunning/promql-empty-selector-short-circuit` branch already does both via a per-query temporary table. The numbers above suggest that branch should keep the subquery form (`id IN (SELECT … FROM <temp table>)`) rather than inlining literals. Also check it against `metric_0`-style wide metrics, where a pre-built set was 30% slower here.

## Upstream overlap
- #118801 (open, nikitamikhaylov, 2026-10-15): collects ids eagerly in the whole-metric probe and inlines ≤128 ids as a literal, otherwise a materialized CTE. It only covers the pre-v11 probe path, which v11 tables skip (`readImpl`, line 1306), and it predates the time-ranges table. Its 128 cap matches the break-even seen here.
- #118116 (open): skips the re-probe on pre-v11 tables only.
- #127847 (open, 2026-10-26): `LEFT ANY JOIN` for the time-ranges table; it helps with unmerged parts, not this cost.
- James's fork branches: `jtcunning/promql-empty-selector-short-circuit` (eager lookup into a temp table, once per distinct selector; 3.6k commits behind master), `jtcunning/promql-planner-speedup`, `jtcunning/promql-selector-whole-metric-drop-id-set` (pre-v11).
- Master `StorageTimeSeriesSelector.cpp`, last 30 days: only Vitaly Baranov's time-ranges and bucketed-samples work. Its comment at line 1302 already argues against a query per selector on v11.

## How to reproduce
Dataset (the handoff's, scaled 1/5: `numbers(200000)`, `metric_0` for `number < 20000`), on a local server with `enable_time_series_table=1`:
```sql
CREATE TABLE ts ENGINE = TimeSeries;
INSERT INTO ts (metric_name, tags, samples)
WITH cityHash64(number) AS h
SELECT concat('metric_', toString(if(number < 20000, 0, (number % 199) + 1))),
 map('job', concat('job', toString(h % 10)), 'instance', concat('host-', toString(intDiv(h, 10) % 500), ':9100'),
     'cluster', concat('c', toString(intDiv(h, 5000) % 5)), 'namespace', concat('ns', toString(intDiv(h, 25000) % 20)),
     'pod', concat('pod-', toString(number)),
     'mode', ['user','system','idle','iowait','nice','irq','softirq','steal'][(intDiv(h, 500000) % 8) + 1]),
 arrayMap(i -> (toDateTime64(<T0> + i * 15, 3), toFloat64((number % 1000) + i)), range(240))
FROM numbers(200000);
-- then OPTIMIZE TABLE <each .inner_id.* table> FINAL
```
Harness: `bench/selector-eager-ids/bench.py` (today vs literal) and `variants.py` (typed literal, temp table) on this branch, with raw results in the `.jsonl` files next to them. Each script takes the generated SQL from `system.text_log` (`Will execute query:`), cuts out the `in(id, (SELECT …))` subquery, and times the variants from `system.query_log`.
