# selector-set-cache (handoff #5, second pass)

Measures repeated PromQL selectors without a build. Report: `reports/2026-11-01-promql-shared-selector-sets.md`.

- `config.xml`: local server on 127.0.0.1:19000 with query_log and text_log (trace). For the HTTP API check, add a `<prometheus>` block with a `query` handler on `/api/v1/query` (port 19093).
- `bench.py <label> [runs]`: times PromQL shapes (`x`, `x + x`, `x + x + x`, offset pair, wide selector) on table `ts`; counts set builds from `Creating set, key:` trace lines. Output: `results.jsonl`.
- `emulate.py`: takes a selector's generated SQL from text_log and times the whole SQL, the tags lookup alone, the SQL with the lookup replaced by its literal ids (a cache hit minus planning), and EXPLAIN PLAN of the lookup. Output: `emulate.jsonl`.
- `perf_xml.py <label>`: runs the `<query>` lines of `tests/performance/promql_repeated_selector.xml` against database `perf` (fill it with the test's create/fill queries first). Output: `perf_xml.jsonl`. Run with `branch` on a build of `lilred/perf-promql-shared-selector-sets` for the A/B column.

Data for `ts`: 100,000 series over 50 metrics, `job` (10), `instance` (500), `pod` (unique), 240 samples at 15 s ending at the last 15-minute boundary (`t_end`), loaded in 4 batches of 25,000 with `max_insert_block_size = 4096`.

Gotcha: run `clickhouse client -q "INSERT ... VALUES ..."` with `< /dev/null`, or it waits on stdin.
