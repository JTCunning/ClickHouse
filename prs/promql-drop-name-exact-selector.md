# PromQL: skip the duplicate series check when dropping the metric name of one metric

Branch: `lilred-perf-promql-drop-name-exact-selector` @ `dadc843ffb74` (base master `c189bd99fbda`; merges cleanly with master `35fed92ffa43`, 2026-11-06; numbers from the build of this head) · compare: https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred-perf-promql-drop-name-exact-selector

<!-- upstream:start (the PR body is everything between these markers; the title is the heading above) -->
Related: https://github.com/ClickHouse/ClickHouse/pull/121069
Related: https://github.com/ClickHouse/ClickHouse/pull/126763
Related: https://github.com/ClickHouse/ClickHouse/pull/123419

### Changelog category (leave one):
- Experimental Feature

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
PromQL functions and operators applied to a selector of one metric, such as `sum(rate(http_requests_total[5m]))` or `abs(m)`, no longer regroup all series to drop the metric name. Such queries over 100k series are up to 27% faster.

### Description

Functions and operators such as `rate`, `abs`, `clamp_max`, `x * 2` and `histogram_quantile` drop `__name__`. `dropMetricName` does that with a `GROUP BY timeSeriesRemoveTag(group, '__name__')` over every series plus the per-step duplicate check `timeSeriesThrowDuplicateSeriesIf(...)`, because two series that differ only by name get the same tags after the drop.

Now, when the only vector argument is a selector with an equality matcher on `__name__` (`m`, `{__name__="m"}`, with `offset`, `@` or as a subquery), all its series share one name, so the drop can't create duplicates and `dropMetricName` emits a plain projection: no GROUP BY, no check. Several names, regular expressions, aggregations and operators over two vectors keep the existing SQL. One file plus the precondition in `dropMetricName.h`; no setting or format change.

Release build (clang 21), AMD EPYC 9R14, servers pinned to 16 cores; performance comparison with two servers on one host, 13 runs per query, base `c189bd99fbda`, 100k series.

| query | master | this PR | change |
|---|---|---|---|
| `sum(rate(m[5m]))` | 152 ms | 115 ms | -24.2% |
| `sum(abs(m))` | 153 ms | 111 ms | -27.5% |
| `sum(rate(m[1m]))`, range, 36 steps | 179 ms | 136 ms | -24.4% |
| `sum by (job) (rate(m[5m]))` | 167 ms | 147 ms | -12.3% |

Not slower: the same selector as a regular expression `{__name__=~"m"}` (control, keeps the check) +1.9% (not significant); `promql_selector_pk_range.xml` within ±0.8% except `sum(rate(pkr_metric[30m]))`, which takes the shortcut, -4.2%.

Tests: `05482_promql_drop_metric_name_single_metric` checks every one-name form (`m`, `{__name__="m"}`, `offset`, `@`, subquery, `abs`, `clamp_max`, unary minus, `* 2`, `timestamp`, `histogram_quantile`, `sum(rate)`) against master's results, and that selectors over several names still merge series, or raise `CANNOT_EXECUTE_PROMQL_QUERY` when they collide at one step. Its last two lines count `Aggregating` steps in `EXPLAIN` against the regex form: `-1` here and `0` on master, so they fail if the shortcut is not taken. New perf test `promql_drop_metric_name_single_metric.xml`. This is the narrow part of #123419; #121069 applies the same rule to range functions only and #126763 defers the drop more widely, so only one of them should land.
<!-- upstream:end -->

Full A/B (2026-11-05): A = `c189bd99fbda`, B = `dadc843ffb74`; AMD EPYC 9R14 (64 cores, 123 GiB), servers pinned to cores 0-15; clang++-21 release, `--reconfigure` so GIT_HASH matches HEAD; perf.py from B's tree against two servers on one host, 13 runs per query.

| perf test | query | A median | B median | change | significant |
|---|---|---|---|---|---|
| promql_drop_metric_name_single_metric | `sum(rate(drop_metric[5m]))` | 0.152 s | 0.115 s | -24.2% | faster |
| promql_drop_metric_name_single_metric | `sum by (job) (rate(drop_metric[5m]))` | 0.167 s | 0.147 s | -12.3% | faster |
| promql_drop_metric_name_single_metric | `sum(rate(drop_metric[1m]))` range 36 steps | 0.179 s | 0.136 s | -24.4% | faster |
| promql_drop_metric_name_single_metric | `sum(abs(drop_metric))` | 0.153 s | 0.111 s | -27.5% | faster |
| promql_drop_metric_name_single_metric | `sum(rate({__name__=~"drop_metric"}[5m]))` control | 0.150 s | 0.153 s | +1.9% | same |
| promql_selector_pk_range | `pkr_metric` | 0.278 s | 0.277 s | -0.2% | same |
| promql_selector_pk_range | `sum(pkr_metric{dc=~".*"})` | 0.268 s | 0.266 s | -0.8% | same |
| promql_selector_pk_range | `sum(rate(pkr_metric[30m]))` | 0.290 s | 0.278 s | -4.2% | faster |
| promql_selector_pk_range | `sum(pkr_metric{dc="0"})` | 0.138 s | 0.137 s | -0.7% | same |

Tests: 05482 passes on B; on A it differs only on the two `EXPLAIN` lines (`0` / `0` instead of `-1` / `-1`), so every result line matches master. No A/A figure in this run; the control and the three unaffected pk_range queries (±0.8%) bound the noise.

<details>
<summary>Self-review (preflight) and notes for James</summary>

- Round 1 (independent run of `.claude/skills/review/SKILL.md`): ⚠️ Request changes. One Major: the shortcut's caller precondition was undocumented, so it's now written in `dropMetricName.h`. Two test asks: the `@` modifier (added) and the build-only items. Nits: histogram_quantile scope (clarified) and the duplicated rule in `pushDownLabelMatchers.cpp` (kept separate; cross-reference comment added).
- Round 2: ⚠️ Request changes **only** for build-dependent evidence: compile plus clang-tidy, the two predicted `-1` lines of 05482, and measured branch numbers. The reviewer would approve once those come back as expected.
- Round 3 (2026-11-06, final text with the build numbers): ✅ Approve. Text fixes applied: entry "up to 27%" (sum by is -12.3%), 36 steps not 37, no "shortcut disabled" switch. Open: run 05482 once on the tree merged with current master (the offset/@ lines go through master's reworked `getRangeAggregationRange`; expected unchanged). Nit not taken (would need a rebuild): a negative case `abs(m{host="h1"} or n)` that reaches the walker's `default` branch.

- 2026-11-06: changelog category switched to `Experimental Feature` (James's convention for TimeSeries/PromQL); text rewritten to the template order, about 2k characters, no sub-headings.
- Overlap: #121069 (same rule, range functions only, fused into the range GROUP BY, still open, updated 2026-11-02) and #126763 (James, wider, open). Land one. See the report `reports/2026-11-01-drop-name-exact-selector.md`.
- Master since base: `applyOffset`, `applyMathBinaryOperator`, `applyComparisonOperator`, `fixedAtModifier`, `applyFunctionOverRange`, `applyFunctionPredictLinear`, `applyFunctionQuantileOverTime` and `SQLQueryPiece.h` changed (#122977, #122781); none touches `dropMetricName` or the parse-tree nodes the rule walks. `git merge-tree` vs `35fed92ffa43` is clean.
- Base: upstream `c189bd99fbda`. Commits `99c452329b87`, `dadc843ffb74`.

</details>
