# [unbuilt] Read the `TimeSeries` time ranges table by metric for a regex on the metric name

Branch: `lilred-perf-time-ranges-metric-filter` @ `22340b032da2` (base `83b6da032407`; 1 commit) · compare: https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred-perf-time-ranges-metric-filter · handoff: https://github.com/JTCunning/ClickHouse/issues/17 · **depends on upstream #115482** (open, conflicts with master since 2026-10)

<!-- upstream:start (the PR body is everything between these markers; the title is the heading above) -->
Related: https://github.com/ClickHouse/ClickHouse/pull/115482
Related: https://github.com/ClickHouse/ClickHouse/pull/129064

### Changelog category (leave one):
- Experimental Feature

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
Speed up `TimeSeries` selectors whose metric name regular expression isn't a list of literal names, such as `{__name__=~".*cpu.*"}`: the time ranges table is read only over the metrics the selector matches instead of in full.

### Description

A selector with a literal metric name or a list of names reads the time ranges table by the id ranges of its metrics (#129064). A regex such as `.*cpu.*` has no id ranges, so the time ranges subquery of the selector's ids query (the query behind every selector, `/api/v1/series` and `/api/v1/label/<name>/values?match[]=`) reads the whole table: the time filter can't prune it, because the table is ordered by `id`.

Now that subquery also gets `tupleElement(id, 1) IN (SELECT tupleElement(id, 1) FROM <tags table> WHERE <matchers on __name__>)` when the selector has a `=` or `=~` matcher on `__name__` and no id ranges, and the id generator computes the first component of `id` from the metric name only (the default `tuple(sipHash64(metric_name), ...)`). The set has at most one element per selected metric. With #115482, which lets the primary key use conditions on the first element of a tuple-typed key column (`IN` goes through the same monotonic chain as comparisons), the time ranges table is read only over the granules of the selected metrics. The set holds the first components the tags table actually stores, so series written with another id generator are kept. Unchanged: names and lists of names (id ranges), selectors with no or only negative matchers on `__name__`, ids that aren't tuples, generators whose first component depends on the tags, and `LowCardinality` or `Nullable` first components. The set uses the same `__name__` conditions as the outer filter, so the selected series are exactly today's. Without #115482 the filter prunes nothing: this PR stays a draft until #115482 is merged.

Setup: release build pending; A/B of the branch on top of #115482 vs master + #115482, `max_threads = 4`, interleaved rounds, the fill of `promql_selector_name_alternatives.xml`. The master column so far is master `1e2fc59b8f1c` on 2 vCPU, median of 15 rounds; #115482 doesn't change these two queries.

| case | master + #115482 | branch |
|---|---|---|
| `{__name__=~".*_199"}` (1 of 200 metrics), new query in `promql_selector_name_alternatives.xml` | 23 ms | pending build |
| `{__name__=~"m_1[0-9][0-9]"}` (100 of 200 metrics), the file's regex control | 169 ms | pending build |

Correctness: the selected samples are identical to master's for every selector in the new test, including a series whose id comes from another generator. Tests: `05768_time_series_selector_regex_time_ranges_metric_filter` pins where the filter appears (dropping the id-ranges check adds it to the list selector; dropping the positive-matcher check adds it to `{__name__!~"ba.", env="prod"}`; dropping the generator check adds it to the tags-hash table and breaks the `UInt64` id table) and that the regex skips the 10,000 time ranges of an unselected metric as the list does (needs #115482). `05493` used such a regex as its no-pruning baseline; it now uses `{env="prod"}`, and its regex line counts the new filter.

The primary key analysis is @ alexbakharew's #115482. #127095 also calls `applyTimeRangeFilterToSelectIDsQuery`; its call gains the `metric_filter` argument (`nullptr` keeps today's SQL).
<!-- upstream:end -->

<details><summary>Emulation, method and self-review (not for upstream)</summary>

**Branch column pending build.** It needs master with #115482 rebased onto it (it conflicts in `KeyCondition.cpp`/`.h` since 2026-10; 412 commits to that file since its base `bb113c3849bb`). The master column above is master `1e2fc59b8f1c` without #115482; #115482 alone doesn't change these queries (no condition on `tupleElement(id, 1)` in them).

**Emulation on master** (`bench/time-ranges-metric-filter/emulate.py`, master `1e2fc59b8f1c`, 2 vCPU, `max_threads = 4`, `max_threads_min_free_memory_per_thread = 0`, 15 interleaved rounds, medians; run 1 had 9 rounds; raw output `emulate-run{1,2,3}.txt`). Master's generated SQL from its `Will execute query` line, then: `set` = plus the branch's filter (master can't prune with it), `hint` = plus an `indexHint` over the id ranges of the matched metrics (what #115482 should skip), `B~` = both.

| case | A (master) | set | hint | B~ (emulated branch) | read_rows A → B~ |
|---|---|---|---|---|---|
| `.*_199` (1 of 200) | 23 ms (run 1: 26) | 23 (26) | 17 | **18 (18): −22% (−31%)** | 247,040 → 83,200 |
| `m_1[0-9][0-9]` (100 of 200) | 169 (167) | **188 (177): +11% (+6%)** | 227 | 228 (221) | 420,992 → 527,488 |

- The `hint` column for the broad regex is an artifact: a 100-range `indexHint` costs ~58 ms here with no pruning, and the branch has no such hint (its primary key check is a lookup in a 100-element set). So the branch's predicted cost on the broad regex is the `set` column: **+6–11%**. That is above what the 2026-11-12 report measured for the metric-level set on its 2.1M-series dataset (0–2%). The set build alone takes 2–3 ms (`SELECT ... IN (SELECT tupleElement(id, 1) ...)` timed by itself), so most of the extra 10–19 ms is elsewhere in the plan, not yet attributed (needs `processors_profile_log`; see the A/B plan).
- On the 2.1M-series dataset of the 2026-11-12 report, the per-series set (which skips the same granules) took `.*cpu.*` from 222 to 77 ms; the metric-level set should get about that with a smaller set.
- Results identical in every variant (assert in `emulate.py`). The new test on master prints the expected `.reference` except its two branch lines (`05768_regex 1` and the final `1 1`); 05493 with the new baseline prints its `.reference` on master except `05493_regex 0 1` (branch-only).

**Preflight round 1** (independent review with `.claude/skills/review/SKILL.md` at `83b6da032407`): ⚠️ Request changes, no Blockers.
- Major, pre-existing: `matcherToAST` anchors `=~"a|b"` as `^a|b$`, which matches `ax` and `xb` (PromQL: `^(?:a|b)$`). Confirmed on master `1e2fc59b8f1c`: `{__name__=~"fo.|ba[r]"}` returns `fooo` and `xbar` too. It affects every label matcher, not only `__name__`, and this branch doesn't change it (its set uses the same condition as the outer filter). Reported to James as a separate correctness fix; not in this branch (one mechanism per PR).
- Major, inherent: depends on #115482 (draft until it merges; the last line of 05768 is `0 1` without it).
- Major: broad-regex cost. Measured above (+6–11% emulated on the XML's control). The suggested `indexHint(... IN ...)` form costs the same here (run 3: +6% vs +7%), so the row-level check isn't the cost; kept the plain form. No threshold added (it would be a new magic number); the A/B decides, and James decides on the trade-off.
- Nit, fixed: skip `LowCardinality`/`Nullable` first components (#115482 can't use them). Nit, fixed: changelog entry wording.
- Round 2 not run yet: the next code change is the rebase onto #115482, and the preflight runs again then.
</details>
