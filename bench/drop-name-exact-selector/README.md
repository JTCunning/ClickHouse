# drop-name-exact-selector (handoff #8, second pass)
Emulates `lilred/perf-promql-drop-name-exact-selector` on a prebuilt master server (no build).
`bench.py` runs each case three ways, interleaved: `master` (prometheusQuery as shipped), `handm` (master's generated SQL from the
`Will execute query` log line, run as-is) and `branch` (the same SQL with the `__name__` drop step as a plain projection).
`run1.jsonl` holds the results, `sql_*_branch.sql` the emulated branch SQL, and `t_end` the T_END the data was loaded with.

Dataset (same as `bench/skip-tags-label-free`): a local server on 127.0.0.1:19000 with `enable_time_series_table=1`, then
```sql
CREATE TABLE ts ENGINE = TimeSeries;
INSERT INTO ts (metric_name, tags, samples)
WITH cityHash64(number) AS h
SELECT concat('metric_', toString(if(number < 100000, 0, (number % 49) + 1))),
       map('job', concat('job', toString(h % 10)), 'instance', concat('host-', toString(intDiv(h, 10) % 500), ':9100'),
           'cluster', concat('c', toString(intDiv(h, 5000) % 5)), 'namespace', concat('ns', toString(intDiv(h, 25000) % 20)),
           'pod', concat('pod-', toString(number)),
           'mode', ['user','system','idle','iowait','nice','irq','softirq','steal'][(intDiv(h, 500000) % 8) + 1]),
       arrayMap(i -> (toDateTime64(T0 + i * 15, 3), toFloat64((number % 1000) + i)), range(40))
FROM numbers(200000) SETTINGS max_insert_block_size = 65536, max_threads = 2;
-- T_END = now rounded down to 900 s, T0 = T_END - 600; then OPTIMIZE TABLE <each .inner_id.* table> FINAL
```
Run `python3 bench.py > run.jsonl` in this directory.
