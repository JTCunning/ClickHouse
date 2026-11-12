# Handoff #17: time ranges read for a non-literal `__name__` regex

Report: [../../reports/2026-11-12-time-ranges-regex-ids.md](../../reports/2026-11-12-time-ranges-regex-ids.md).
All data is synthetic. Master binary 26.11.1.2075 (1e2fc59b8f1c), 2 vCPU / 4 GiB, local disk.

1. Start a server: `clickhouse server --config-file=server-config.xml` from an empty work directory
   (TCP 19000, HTTP 18123, Prometheus API on 19093: `/api/v1` = table `ts`, `/ts2/api/v1` = table `ts2`).
2. `gen.sh <clickhouse> 19000`: table `ts`, 2.1M series (about 25 s).
3. `bench.py <clickhouse> 19000 7 out.jsonl`: the ids query as master builds it vs the same SQL with the id set
   (and the rejected variants; `VARIANTS=` and `CASESET=selectivity` pick subsets).
4. `api_bench.py <clickhouse> 19000 19093 7 out.jsonl`: `/api/v1/label/__name__/values` and `/api/v1/series` through master's
   own code, regex vs the literal list of the names it matches.
5. `time_ranges_rows.py`: rows read from the time ranges table alone.
6. The in-tree perf test's regex control: create and fill `promql_name_alternatives_ts` as in
   `tests/performance/promql_selector_name_alternatives.xml`, capture master's SQL from the `Will execute query` debug line
   (`perfxml-regex-master.sql`; `perfxml-regex-idset.sql` adds the id set), then `perfxml_emulation.py <A.sql> <B.sql> 9`.
   The inner table names in the captured SQL carry the UUID of the table: re-capture after a new fill.
7. `gen_names.sh` + `names_scaling.py`: the literal path with 11,111 and 20,000 names.
