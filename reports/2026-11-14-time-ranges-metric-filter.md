# Option A for handoff #17: read the time ranges table by metric for a `__name__` regex
**Verdict:** Branch ready, but blocked on upstream #115482, which is idle and now conflicts with master. Emulated on master, the metric-level filter makes a narrow regex's selector 22–31% faster (1 of 200 metrics). It also costs 6–11% on the in-tree broad-regex control, not the 0–2% the 2026-11-12 report predicted.
**Task:** 2026-11-05-handoff-limit-the-time-ranges-read-for- · handoff https://github.com/JTCunning/ClickHouse/issues/17 · **Build:** 26.11.1.2253, `1e2fc59b8f1c` (master 2026-11-12) · **Date:** 2026-11-14
**A/B plan:** [ab-plans/time-ranges-metric-filter.md](../ab-plans/time-ranges-metric-filter.md) · **Branch:** `lilred-perf-time-ranges-metric-filter` @ `22340b032da2` · **PR text:** [prs/time-ranges-metric-filter.md](../prs/time-ranges-metric-filter.md) · **Before:** [2026-11-12 report](2026-11-12-time-ranges-regex-ids.md)

## What happens (plain language)
- Option A has two parts. First, upstream #115482 teaches primary key analysis that a condition on the first element of a tuple key (`id.1`) can skip granules. Second, the selector adds a set of the matched metrics' first id components to its time ranges subquery.
- **#115482 status (2026-11-14):** open, last activity 2026-10-19. Its reviewer nihalzp asked for the split on 2026-09-18, the author split it on 2026-09-24, and the bot unassigned nihalzp for inactivity on 2026-10-19. It no longer merges with master: `KeyCondition.cpp` and `.h` conflict, after 412 commits to that file since the PR's base (2026-08-30). The function it extends has been renamed (`isKeyPossiblyWrappedByMonotonicFunctions` → `tryMatchKeyColumnThroughMonotonicChain`).
- **Does it cover `IN` sets?** Yes, by reading the code. On master, an `IN` set's key mapping goes through the same function (`tryAnalyzePredicateExpressionForSetIndex`), and `MergeTreeSetIndex::checkInRange` applies the mapping's function chain to each granule's key range (`applyMonotonicFunctionsChainToRange`). So the PR's "first element is monotonic" link should also serve `tupleElement(id, 1) IN (subquery)` once it's rebased. Its tests only cover comparisons; an `IN` case is the one to add.
- **The branch** adds `tupleElement(id, 1) IN (SELECT tupleElement(id, 1) FROM <tags> WHERE <__name__ matchers>)`. It applies only when the selector has a `=`/`=~` matcher on `__name__`, has no id ranges, and the id generator's first component depends only on the metric name. The set holds one entry per matched metric.

## Numbers
**Dataset:** the fill of `tests/performance/promql_selector_name_alternatives.xml`: 200 metrics × 1,000 series, 20 samples each, `instance` (1,000 values) and `dc` (8). One part per inner table.
**Setup:** 2 vCPU, `max_threads = 4`. Master's generated SQL is captured from its debug line and run as is (A); with the branch's set (set, which master can't prune with); and with the set plus an `indexHint` over the matched metrics' id ranges (B~, standing in for #115482's pruning). 15 interleaved rounds, medians. Results are identical in every variant.

| selector | A (master) | set only | B~ (emulated branch) | time-ranges + tags rows read, A → B~ |
|---|---|---|---|---|
| `{__name__=~".*_199"}`: 1 of 200 metrics | 23 ms (run 1: 26) | 23 | **18 ms: −22% (run 1: −31%)** | 247k → 83k |
| `{__name__=~"m_1[0-9][0-9]"}`: 100 of 200, the XML's control | 169 (167) | **188: +11% (run 1: +6%, run 3: +7%)** | 228* | 421k → 527k |

- *For the broad regex, B~ is dominated by the emulation itself: a 100-range `indexHint` costs ~58 ms here with no pruning. The branch has no such hint, so its predicted cost is the "set only" column, +6–11%.
- The set build alone takes 2–3 ms (timed by itself). Putting the set inside `indexHint`, so no row is checked against it, costs the same (+6%, run 3). So most of the extra ~10–19 ms is somewhere else in the plan and isn't attributed yet. That's the first thing the A/B should profile.
- **Correction to the 2026-11-12 report:** it measured the metric-level set at 0–2% on its own 2.1M-series dataset and said option A would have "no regression". On the in-tree XML's smaller table it's +6–11%. That is still under option B's +16% on the same control, but it isn't zero.
- On the 2026-11-12 dataset (2.1M series), the per-series set skipped the same granules and took `.*cpu.*` (0.7% of series) from 222 to 77 ms. The metric-level set should reach about the same once #115482 is in.

## Why (cause)
- No id ranges for a regex: `tryGetTimeSeriesIDRanges` (`src/Storages/TimeSeries/TimeSeriesIDRange.cpp`) only covers literal names. With no ranges, `addTimeRangesTableJoin` (`src/Storages/StorageTimeSeriesSelector.cpp`) gives the time ranges subquery only time conditions, and the table, ordered by `id`, is read in full.
- A set of first components can only skip granules if primary key analysis maps the granule's `id` range to a range of `id.1`. Master can't do that (`tupleElement` declares no monotonicity), and #115482 adds exactly that.

## Proposed fix
- The branch: `tryMakeTimeRangesMetricFilter` plus `hasMetricNameFirstComponent`, about 70 lines with comments. It passes the filter through `applyTimeRangeFilterToSelectIDsQuery`.
- Tests:
  - new `05768`: correctness against the list of names (including a series from another id generator), where the filter appears and where it doesn't, and rows skipped (needs #115482)
  - `05493`: its no-pruning baseline becomes `{env="prod"}`
  - a narrow-regex query added to `promql_selector_name_alternatives.xml`
- No format or state change. Effort S, but blocked on #115482: someone has to rebase it and add an `IN` test.
- **Open risk:** the +6–11% on the broad control. A row-level vs `indexHint` form doesn't change it. Avoiding it would take a selectivity threshold (a new magic number), or finding and removing the unattributed plan cost.

## Side finding (correctness, not part of this handoff)
`matcherToAST` anchors a regex as `'^' + value + '$'`, so `=~"fo.|ba[r]"` becomes `^fo.|ba[r]$`. That pattern matches any name that starts with `fo.` or ends with `bar`. Prometheus anchors the whole expression (`^(?:fo.|ba[r])$`).
- On master `1e2fc59b8f1c`, `{__name__=~"fo.|ba[r]"}` over metrics `foo`, `fooo`, `xbar` and `bar` returns all four; Prometheus returns 2.
- It applies to every label matcher with `|` in a regex that isn't a list of literals.
- The fix is a one-line change to `^(?:…)$` plus a test, as a separate PR.

## Upstream overlap
- [#115482](https://github.com/ClickHouse/ClickHouse/pull/115482): the dependency (above).
- [#127847](https://github.com/ClickHouse/ClickHouse/pull/127847): merged 2026-11-12 (`LEFT ANY` join). The branch is based on it.
- [#127095](https://github.com/ClickHouse/ClickHouse/pull/127095) (James, open): also calls `applyTimeRangeFilterToSelectIDsQuery`, so its call gains one argument.
- [#127848](https://github.com/ClickHouse/ClickHouse/pull/127848) (open): turns more regexes into literal lists, which take the id-range path instead. Complementary.
- James's fork branches: none on this path (checked 2026-11-14; the only `time-range*` branches are #129064's, which is merged).

## How to reproduce
- `bench/time-ranges-metric-filter/README.md` covers it. Start a lab server with `../time-ranges-regex-ids/server-config.xml`, then run `emulate.py 15`. It fills the XML's table, captures master's SQL, and prints the table above.
- Tests on master: `clickhouse client --database db --param_CLICKHOUSE_DATABASE=db -n --queries-file 05768_….sql` prints the `.reference` except its two branch lines.
