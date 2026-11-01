# lc-uuid-id (scout: what LowCardinality(UUID) in the TimeSeries id buys on reads)

A/B of the default v11 id `Tuple(UInt64, LowCardinality(UUID))` vs `Tuple(UInt64, UUID)` on a prebuilt master server
(127.0.0.1:19100, `enable_time_series_table=1`, query_log + text_log + trace_log; config as in `bench/drop-name-exact-selector`).

Dataset (own cases):
```sql
CREATE TABLE ts_lc ENGINE = TimeSeries;
CREATE TABLE ts_plain ENGINE = TimeSeries TAGS INNER COLUMNS (id Tuple(UInt64, UUID));  -- propagates to all inner tables
-- for t in ts_lc, ts_plain:
INSERT INTO <t> (metric_name, tags, samples)
WITH cityHash64(number) AS h
SELECT concat('metric_', toString(if(number < 100000, 0, (number % 49) + 1))),
       map('job', concat('job', toString(h % 10)), 'instance', concat('host-', toString(intDiv(h, 10) % 500), ':9100'),
           'cluster', concat('c', toString(intDiv(h, 5000) % 5)), 'namespace', concat('ns', toString(intDiv(h, 25000) % 20)),
           'pod', concat('pod-', toString(number)),
           'mode', ['user','system','idle','iowait','nice','irq','softirq','steal'][(intDiv(h, 500000) % 8) + 1]),
       arrayMap(i -> (toDateTime64(T0 + i * 30, 3), toFloat64((number % 1000) + i * 1.5)), range(480))
FROM numbers(150000) SETTINGS max_insert_block_size = 16384, max_threads = 2, max_block_size = 16384;
-- T_END = now rounded down to 900 s (file t_end), T0 = T_END - 14400; then OPTIMIZE TABLE <each .inner_id.* table> FINAL
```

- `python3 bench.py own`: own cases on ts_lc / ts_plain -> `run1.jsonl`
- `python3 bench.py perf`: the in-tree perf tests that create TimeSeries tables (`promql_selector_id_range_row_cost`,
  `promql_selector_pk_range`, `promql_set_operator_presence_mask`, `promql_topk`, the TimeSeries part of `promql_tags_transform`),
  each created twice (default and plain id), filled, `OPTIMIZE ... FINAL`, then every query A/B -> `run2.jsonl`
- `python3 pe.py`: re-creates `promql_selector_pk_range` for both and prints the ProfileEvents that differ.

Each case: 1 discarded warm-up, then RUNS (5) interleaved runs per variant, `max_threads = 2`; medians from `system.query_log`.
