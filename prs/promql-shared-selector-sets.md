# Build the id set of a repeated PromQL selector once per query

Branch: `lilred-perf-promql-shared-selector-sets` @ `b56cfb0261aa` (rebased 2026-11-04 onto master `fd328d4baedf`, plus one review fix; the numbers below are from `15ec8bc23e76` on `c189bd99fbda`, confirm run pending) · compare: https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred-perf-promql-shared-selector-sets

<!-- upstream:start (the PR body is everything between these markers; the title is the heading above) -->
Related: https://github.com/ClickHouse/ClickHouse/pull/118801

### Changelog category (leave one):
- Performance Improvement

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
A PromQL query that repeats a selector with the same label matchers and time range, such as `x + x`, looks up the matching series in the tags table of a `TimeSeries` table once instead of once per occurrence. `x + x` is 17% faster and `x + x + x` 21% faster in the new performance test.

### Description

Each `timeSeriesSelector` reads the samples with `id IN (<tags lookup>)`, and on a table with bucketed samples the primary-key analysis builds that set in place while the query is planned. When a query repeats a selector (`x + x`, `x / ignoring(pod) group_left x`), every occurrence runs the same lookup again: the set keys are equal, but each selector is planned by its own interpreter with its own prepared sets.

Now one PromQL evaluation (the `prometheusQuery*` table functions and the HTTP API, through `applySettingsForPrometheusQuery`) gets a `PreparedSetsCache`, the cache mutations already use, and `FutureSetFromSubquery::buildOrderedSetInplace` adopts a set already built under the same key (`findBuilt`). All sets of one query are built in one pipeline, so this cache never waits for a set in flight (a new option), and it replaces a waiting mutation cache in the PromQL query's own context; a distributed plan gets no cache. Selectors used once, or with another time range (`offset`), build their own sets as before. No setting or format change.

Release build (clang 21), AMD EPYC 9R14, servers pinned to 16 cores; performance comparison with two servers on one host, 13 runs per query, base `c189bd99fbda`.

| query | master | this PR | change |
|---|---|---|---|
| `x + x`, x = `metric_1{pod="pod-1"}` | 41.9 ms | 34.9 ms | -16.9% |
| `x + x + x` | 61.0 ms | 48.1 ms | -21.1% |
| `y + y`, y = `metric_1{job="job3"}` (~200 series) | 55.0 ms | 46.8 ms | -15.0% |

That is 6.5-8.2 ms saved per repeated occurrence, as in an earlier run on the same host (5.2-6.8 ms). Not slower: `rate(x[5m]) / rate(x[5m] offset 10m)` +1.5% and `y` alone -0.3% (both not significant), all 4 queries of `promql_selector_pk_range.xml` -2.2 to -2.8% (not significant); `x` alone +3.8% is marked unstable (p = 0.88).

Tests: `05481_promql_repeated_selector_shares_id_set` checks the results and `SetsBuiltFromSubquery` of `x + x`, `x + x + x`, `x - x offset 1m` and `x + ignoring(pod) y` (1, 1, 2, 2; master prints 2, 3, 2, 2), and runs `x + x + x` with the in-place build off at `max_threads` 1 and 2, the path that must not wait; an HTTP API integration case expects one set built; gtests cover the no-wait lookup and skipping abandoned builds. The mutation tests `02581_*` and `02863_*` pass. New perf test `promql_repeated_selector.xml`: no existing `promql_*` query repeats a selector. #118801 shares one tags scan per metric through `MaterializedCTE`; this PR is smaller and independent of it.
<!-- upstream:end -->

Full A/B (run 2, from the lil-red-outbox results comment 2026-11-02): A = `c189bd99fbda`, B = `15ec8bc23e76`; perf.py from B's tree, `compare_perf_tsv.py` defaults (3% floor, alpha 0.05).

| perf test | query | A | B | change | status |
|---|---|---|---|---|---|
| promql_repeated_selector | `metric_1{pod="pod-1"}` (control) | 17.44 | 18.10 | +3.8% | unstable, p = 0.88 |
| promql_repeated_selector | `x + x` | 41.93 | 34.85 | -16.9% | faster, p = 0.021 |
| promql_repeated_selector | `x + x + x` | 61.04 | 48.14 | -21.1% | faster, p < 0.001 |
| promql_repeated_selector | `rate(x[5m]) / rate(x[5m] offset 10m)` | 41.45 | 42.06 | +1.5% | same |
| promql_repeated_selector | `metric_1{job="job3"}` | 24.67 | 24.58 | -0.3% | same |
| promql_repeated_selector | `y + y` | 55.03 | 46.79 | -15.0% | faster, p < 0.001 |
| promql_selector_pk_range | 4 queries | 138.5-287.1 | 135.4-280.1 | -2.2 to -2.8% | same |

Not run on a build yet: gtests `PreparedSetsCache.*` (release config has `ENABLE_TESTS=OFF`; upstream CI runs them). Rebase note: master's `applySettingsForPrometheusQuery` (shared by both entry points) now creates the cache, instead of the two copies the branch had. Review round 4 (b56cfb0): always install the PromQL cache (before, a PromQL query inside a mutation kept the mutation's waiting cache), and 05481's counted queries pin `use_index_for_in_with_subqueries = 1, use_query_condition_cache = 0`.

<details>
<summary>Self-review (lil red, not for upstream)</summary>

Three rounds against `.claude/skills/review/SKILL.md` at master `c189bd99fbda`.
- Round 1, ⚠️ Request changes. Major: with the cache on an ordinary query context, the old blocking wait in `CreatingSetsTransform` could stall the one executor thread while another transform of the same pipeline held the promise (e.g. `max_threads = 1`, `use_index_for_in_with_subqueries = 0`). Fixed with the no-wait cache option. Also: the distributed-plan question (fixed: no cache), the mutation side effect (now stated), and HTTP API coverage (added).
- Round 2, ⚠️ Request changes. Add a gtest for the no-wait option (added; the cache's code and all 7 gtest cases were checked verbatim in a standalone g++ model, and a mutant without the no-wait branch fails); nit: `MergeTask` is TTL-delete merges (fixed).
- Round 3, ⚠️ Request changes, **only for the build-dependent evidence**: the branch column, and running 05481 (expected 1, 1, 2, 2), `test_repeated_selector_builds_id_set_once` (expected 1) and the gtest on a build. Reviewer: "If the numbers show the expected gain and the controls are unchanged, this can be approved without further code changes."

- Round 4 (2026-11-04, after the rebase onto `fd328d4baedf`), ⚠️ Request changes. Major: the `!getPreparedSetsCache()` guard kept a mutation's waiting cache for a PromQL query inside a mutation (same-key sets in one pipeline could wait on each other). Fixed in b56cfb0: always install the non-waiting cache in the PromQL context copy. Test: pinned the settings the 05481 counts depend on (CI randomizes `use_query_condition_cache`; `use_lightweight_primary_key_index_analysis` is also randomized and left to the flaky check). Open: confirm build; nit on `spill_state` not copied on adoption (same as the existing cache-hit path), left as is.

</details>
