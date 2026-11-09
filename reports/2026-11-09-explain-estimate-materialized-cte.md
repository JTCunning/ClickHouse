# EXPLAIN ESTIMATE and the PromQL topk() materialized CTE
**Verdict:** not a bug. `EXPLAIN ESTIMATE` counts the shared read once whenever the CTE is materialized. The doubled number comes from running the copied SQL in a session where `enable_materialized_cte` is off (the server default). There the CTE is inlined and the table really is read twice. No fix needed; it's a measurement caveat.
**Task:** 2026-11-01-handoff-explain-estimate-double-counts- · [handoff #13](https://github.com/JTCunning/ClickHouse/issues/13) · **Build:** 26.11.1.1642, git 7b5f2bf328f9 · **Date:** 2026-11-09

## What happens (plain language)
The PromQL entry points (`prometheusQuery*` table functions and the `/api/v1` HTTP handler) run the generated SQL with three settings changed: `enable_materialized_cte=1`, `enable_adaptive_aggregator=0` and `empty_result_for_aggregation_by_empty_set=0`. They apply only when the user hasn't set them. The `AS MATERIALIZED` in the generated SQL is a hint that takes effect only when `enable_materialized_cte=1`. The server default is 0.

So if you copy the SQL out of the log and run `EXPLAIN ESTIMATE` (or the query itself) in a plain session, the analyzer ignores `MATERIALIZED` (with a warning) and inlines the CTE into both consumers. That plan really scans the samples table twice, and the estimate is right for that plan. The real PromQL query reads once, which is why its `SelectedMarks` matches the single-read figure.

## Numbers
Dataset: TimeSeries table (default engine settings), 1 metric, 2,000 series (20 namespaces), 15 s scrape, 24 h, 11.5 M samples; samples inner table 48,000 rows, 357 marks, 7 MiB. Query `topk(10, sum by (namespace, pod) (rate(cpu{job="cadvisor"}[5m])))`, step 900 s.

| case | EXPLAIN ESTIMATE parts / rows / marks (samples table) | real run SelectedMarks (all tables) |
|---|---|---|
| `prometheusQueryRange(topk …)` | 1 / 48,000 / 356 | 358 |
| `prometheusQueryRange(sum by …)` control | 1 / 48,000 / 356 | — |
| `prometheusQueryRange(topk …) SETTINGS enable_materialized_cte=0` | 2 / 96,000 / 712 | — |
| copied SQL, session default (`enable_materialized_cte=0`) | 2 / 96,000 / 712 | **716** (really reads twice) |
| copied SQL, `enable_materialized_cte=1` | 1 / 48,000 / 356 | 358 |

The generic shape behaves the same: on a plain MergeTree table, `WITH c AS MATERIALIZED (…)` with two consumers gives the single-read estimate (391 marks) with the setting on and twice that (782) with it off. A plain, non-materialized CTE with two consumers is also 782, and that's correct because it does read twice.

The handoff's figures fit this exactly. Its estimate (Parts 2, 273,070 marks) is twice the control's (Parts 1, 136,535). Its `EXPLAIN indexes = 1` shows no `MaterializingCTEs` step: both join sides carry their own full `ReadFromMergeTree` subtree, so it was taken with materialization off. The real run (`MaterializingCTE` processor present, `SelectedMarks` 136,543) had it on.

## Why (cause)
- `QueryPlan::explainEstimate` (`src/Processors/QueryPlan/QueryPlan.cpp:1188`) walks the plan tree and adds up every `ReadFromMergeTree` it reaches. A materialized CTE's plan is attached once, as a child of the `MaterializingCTEs` step (`Optimizations/addPlansForMaterializingCTEs.cpp`), and its consumers read `ReadFromMemoryStorage`. So the walk sees the read once. The plan is a tree, so a visited-node set would change nothing.
- `StoragePrometheusQuery::applySettingsForPrometheusQuery` (`src/Storages/StoragePrometheusQuery.cpp:190`) turns materialization on for the generated query only. It's also called from `PrometheusHTTPProtocolAPI.cpp:260`.
- `SQLSubquery.h:22` documents that the mark is only a hint.

## Proposed fix
Report only: no code change. What this means in practice:
- When you analyse or time generated PromQL SQL outside the PromQL entry points, add `SETTINGS enable_materialized_cte = 1, enable_adaptive_aggregator = 0, empty_result_for_aggregation_by_empty_set = 0` to reproduce the plan PromQL actually runs. This matters beyond `topk`: `or` also materializes a shared side (`applyBinaryOperatorOr.cpp`). The adaptive-aggregator setting (added 2026-11-02, `5b3750394f1f`) changes grid-aggregation timing too. Measurements of copied SQL made without these settings describe a different plan.
- Better still, run `EXPLAIN ESTIMATE SELECT * FROM prometheusQueryRange('<table>', '<promql>', start, end, step)`. It applies the settings itself and returns the single-read estimate.
- Upstream #118853 (open) adds `force_materialized_cte`, which turns the silent fallback into an error. That would have made this visible at once.

## Upstream overlap
- Open PRs that touch `EXPLAIN ESTIMATE` (#102398 adds byte columns, #121815, #124320) don't change how it counts shared reads.
- Materialized-CTE PRs: #118853 (`force_materialized_cte`), #123596, #124871, #121688. None changes the default of `enable_materialized_cte`.
- Nothing to fix, so there's no branch.

## How to reproduce
```sql
-- clickhouse local, one session (master 7b5f2bf328f9)
SET allow_experimental_time_series_table = 1, session_timezone = 'UTC';
CREATE TABLE prom ENGINE = TimeSeries;
INSERT INTO prom (metric_name, tags, samples)
SELECT 'cpu', map('namespace', toString(number % 20), 'pod', toString(number), 'job', 'cadvisor'),
       arrayMap(i -> (toDateTime64(1704067200 + i * 15, 3), toFloat64(i * (number % 13 + 1))), range(5760))
FROM numbers(2000);

EXPLAIN ESTIMATE SELECT * FROM prometheusQueryRange('prom',
    'topk(10, sum by (namespace, pod) (rate(cpu{job="cadvisor"}[5m])))', 1704067800, 1704153585, 900);  -- 356 marks
EXPLAIN ESTIMATE SELECT * FROM prometheusQueryRange('prom',
    'topk(10, sum by (namespace, pod) (rate(cpu{job="cadvisor"}[5m])))', 1704067800, 1704153585, 900)
    SETTINGS enable_materialized_cte = 0;                                                             -- 712 marks
-- The copied SQL (the "Will execute query" line at send_logs_level='debug'):
-- EXPLAIN ESTIMATE <sql>;                                   -- 712 marks, and a real run selects 716
-- EXPLAIN ESTIMATE <sql> SETTINGS enable_materialized_cte=1; -- 356 marks, and a real run selects 358

-- Generic, without PromQL:
CREATE TABLE t (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
INSERT INTO t SELECT number, number % 7 FROM numbers(100000);
EXPLAIN ESTIMATE WITH c AS MATERIALIZED (SELECT k, v FROM t WHERE k < 50000)
SELECT count() FROM c AS a JOIN c AS b ON a.k = b.k SETTINGS enable_materialized_cte = 1;  -- 391 marks
EXPLAIN ESTIMATE WITH c AS MATERIALIZED (SELECT k, v FROM t WHERE k < 50000)
SELECT count() FROM c AS a JOIN c AS b ON a.k = b.k;                                       -- 782 marks
```
Real-run `SelectedMarks` came from `clickhouse local --print-profile-events --profile-events-delay-ms=-1`. Side note, not investigated: once, reopening a `clickhouse local --path` that holds a TimeSeries table failed with "Tags inner table … doesn't exist" (the first reopen had worked). Running everything in one session avoids it.
