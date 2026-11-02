# [unbuilt] Build the id set of a repeated PromQL selector once per query

Branch: `lilred-perf-promql-shared-selector-sets` · compare: https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred-perf-promql-shared-selector-sets

<!-- upstream:start (the PR body is everything between these markers; the title is the heading above without [unbuilt]) -->

Each `timeSeriesSelector` in a PromQL query reads the samples table with `id IN (<tags lookup>)`, and on a table with bucketed samples (version 11) the primary-key analysis builds that set in place while the query is planned. When a query repeats a selector with the same matchers and time range (`x + x`, `x / ignoring(pod) group_left x`, `x > 0 and x < 10`), each occurrence runs the same tags lookup again. The set key is already the hash of the subquery, so the two keys are equal. They are not shared because each selector is planned by its own interpreter, with its own prepared sets.

This PR gives one PromQL evaluation (the `prometheusQuery` / `prometheusQueryRange` table functions and the Prometheus HTTP API) a `PreparedSetsCache`, the per-query set cache that mutations already use to share sets between tasks. `FutureSetFromSubquery::buildOrderedSetInplace` now adopts an ordered set already built under the same key (`PreparedSetsCache::findBuilt`) instead of running the subquery. The existing cache lookup in `CreatingSetsTransform` is not enough on its own: it runs only after the subquery has produced its first block, which for a selector of a few series is nearly the whole lookup.

All the sets of one PromQL query are built in one pipeline, so its cache is created with a new `PreparedSetsCache` option that never waits for a set still being built: a lookup in flight gets a promise to build the set once more. Waiting there, as the mutation cache does, could block the executor thread that the building subquery needs (for example with `max_threads = 1` and `use_index_for_in_with_subqueries = 0`). A distributed plan (`make_distributed_plan`) gets no cache, since it ships the values of each set to its workers.

Nothing changes for a selector that occurs once: it misses the cache with one hash-map lookup. Selectors with different matchers or time ranges (an `offset`) have different keys and still build their own sets. `EXPLAIN` runs nothing new, and no serialized state or setting is added.

Outside PromQL, the cache is set only for mutations (`MutatePlainMergeTreeTask`) and TTL-delete merges (`MergeTask`), which keep waiting for sets in flight as before. The new adoption in `buildOrderedSetInplace` applies to them too: a mutation task whose primary-key analysis needs a set already built by another task of the same mutation now adopts it instead of running the subquery again, which is the sharing that cache is there for. Existing coverage: `02581_share_big_sets_between_mutation_tasks`, `02581_share_big_sets_between_multiple_mutations_tasks_long`, `02863_mutation_where_in_set_result_cache_pipeline_stuck_bug`.

### Evidence
Host: 2 vCPU / 4 GiB VM, `max_threads = 2`. Base: master build `a7f713612c80`. Workload: the new `tests/performance/promql_repeated_selector.xml` (100,000 series over 50 metrics, 240 samples each, v11 table with default settings), instant query, median of 10 warm runs after one cold run.

| case | master (median of 10) | min–max | sets built | branch (median of 10) |
|---|---|---|---|---|
| `metric_1{pod="pod-1"}` (control) | 21.5 ms | 20–49 | 1 | pending build |
| `x + x`, x = `metric_1{pod="pod-1"}` | 45.0 ms | 42–79 | 2 | pending build |
| `x + x + x` | 67.5 ms | 63–109 | 3 | pending build |
| `rate(x[5m]) / rate(x[5m] offset 10m)` (control, two keys) | 49.0 ms | 42–57 | 2 | pending build |
| `metric_1{job="job3"}`, ~200 series (control) | 39.0 ms | 36–68 | 1 | pending build |
| `y + y`, y = `metric_1{job="job3"}` | 80.0 ms | 74–108 | 2 | pending build |

What a cache hit saves, measured on master by running a selector's generated SQL (100,000-series dataset, median of 10): the one-series selector takes 26.5 ms, its tags lookup alone 14 ms, and the same SQL with the lookup replaced by its literal result 6 ms. The subquery is still analysed and planned on a hit (EXPLAIN PLAN of it: ~5 ms). Expected: the cost of one tags lookup off each repeated occurrence, and no change on the controls. A first A/B run on a 64-vCPU host saved 5.2–6.8 ms per repeat (`x + x` 41.6 → 35.4 ms, −15%; `x + x + x` 59.1 → 48.6 ms, −18%); the 14 ms "lookup alone" figure above includes per-query overhead that a hit does not remove.

Commands (branch column):
```
clickhouse server  # 127.0.0.1, query_log on
# fill: the <create_query> and <fill_query> of tests/performance/promql_repeated_selector.xml in database perf
python3 perf_xml.py branch   # research bench/selector-set-cache: each <query>, 1 cold + 10 warm runs, median from system.query_log
```

### Tests
- `05481_promql_repeated_selector_shares_id_set`: the results of `x + x`, `x + x + x`, `x - x offset 1m` and `x + ignoring(pod) y`, and `ProfileEvents['SetsBuiltFromSubquery']` for each: 1, 1, 2, 2. Master prints 2, 3, 2, 2, so the test fails without the change. It also runs `x + x + x` with `use_index_for_in_with_subqueries = 0` at `max_threads` 1 and 2, where the three sets are built in the pipeline and must not wait for each other (results only: how many builds overlap depends on scheduling).
- `test_prometheus_protocols/test_query_api.py::test_repeated_selector_builds_id_set_once`: `foo{shape="square"} + foo{shape="square"}` through `/api/v1/query`, one set built. Master returns the same response with 2.
- `gtest_prepared_sets_cache`: `NonWaitingCacheBuildsAgainWhileInFlight` (a second lookup of a set in flight gets a promise, the entry stays with the first builder, a later lookup gets the built set) and `FindBuiltSkipsAbandonedAndFailedBuilds`.
- New perf test `tests/performance/promql_repeated_selector.xml`: repeated one-series and ~200-series selectors, plus single-use and `offset` controls. No existing `tests/performance/promql_*` query repeats a selector, so none of them should change.

Related: #118801 (a broader approach: one tags scan per metric through `MaterializedCTE`). This PR is independent of it and smaller.

### Changelog category (leave one):
- Performance Improvement

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
A PromQL query that repeats a selector with the same label matchers and time range, such as `x + x`, looks up the matching series in the tags table of a `TimeSeries` table once instead of once per occurrence.

<!-- upstream:end -->

<details>
<summary>Self-review (lil red, not for upstream)</summary>

Three rounds against `.claude/skills/review/SKILL.md` at master `c189bd99fbda`.
- Round 1, ⚠️ Request changes. Major: with the cache on an ordinary query context, the old blocking wait in `CreatingSetsTransform` could stall the one executor thread while another transform of the same pipeline held the promise (e.g. `max_threads = 1`, `use_index_for_in_with_subqueries = 0`). Fixed with the no-wait cache option. Also: the distributed-plan question (fixed: no cache), the mutation side effect (now stated), and HTTP API coverage (added).
- Round 2, ⚠️ Request changes. Add a gtest for the no-wait option (added; the cache's code and all 7 gtest cases were checked verbatim in a standalone g++ model, and a mutant without the no-wait branch fails); nit: `MergeTask` is TTL-delete merges (fixed).
- Round 3, ⚠️ Request changes, **only for the build-dependent evidence**: the branch column, and running 05481 (expected 1, 1, 2, 2), `test_repeated_selector_builds_id_set_once` (expected 1) and the gtest on a build. Reviewer: "If the numbers show the expected gain and the controls are unchanged, this can be approved without further code changes."

</details>
