# Lab: metric-level filter on the TimeSeries time ranges table (handoff #17, option A)

Report: [../../reports/2026-11-14-time-ranges-metric-filter.md](../../reports/2026-11-14-time-ranges-metric-filter.md) · A/B plan: [../../ab-plans/time-ranges-metric-filter.md](../../ab-plans/time-ranges-metric-filter.md)

1. Start a server from an empty work directory with the config of the earlier lab, by absolute path:
   `clickhouse server --config-file=$PWD/server-config.xml` (copy `../time-ranges-regex-ids/server-config.xml` first; TCP 19000, HTTP 18123, `text_log` at debug).
2. `python3 -I emulate.py 15`: fills the table of `tests/performance/promql_selector_name_alternatives.xml` once (about 30 s here), captures master's generated SQL for `.*_199` and `m_1[0-9][0-9]` from the selector's `Will execute query` debug line, and times A (master), `set` (plus the branch's filter), `set in indexHint`, `hint` (an `indexHint` over the matched metrics' id ranges) and `B~` (set + hint, the emulated branch with #115482), interleaved. It asserts that every variant returns the same result.
3. Raw output: `emulate-run1.txt` (9 rounds, before the `hint` and `set in indexHint` variants were added), `emulate-run2.txt` (15 rounds), `emulate-run3.txt` (15 rounds).

The `hint` emulation of the pruning costs about 58 ms by itself on the broad regex (100 OR'ed ranges); the branch has no such hint, so read the broad case's prediction from the `set` column.
