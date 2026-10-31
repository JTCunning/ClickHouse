# [unbuilt] Grow the timeSeries*ToGrid bucket map before a colliding wrapping index

Branch: `lilred/perf-grid-gap-wraparound` · Compare: https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred/perf-grid-gap-wraparound

The bucket map of the `timeSeries*ToGrid` states is a `HashMap` with `TrivialHash` and linear probing, so bucket index `k` lives in cell `k % bufSize`. Since #124773 the map fills up to 3/4 before it grows. That is collision-free while every index is below the buffer size, but on a series with a gap in the middle of the grid the indexes after the gap wrap around onto the cells of the first run of buckets, and while the map is between 1/2 and 3/4 full every insert of such an index walks the whole run (about 3,100 probes per bucket after the gap in the case below). This PR doubles the map before inserting a missing index that wraps and collides with another index, when the map is more than half full, so colliding indexes grow the map exactly as they did with the 1/2 limit before #124773. Maps without such collisions (contiguous series, including those that start late in the grid) keep the 3/4 limit and its memory saving. The add, batch add, merge and deserialize paths all go through the same helper; the serialized state is unchanged.

### Evidence
Dataset: 200 series, one sample per minute, grid `(0, 604800, 60, 60)` (7 days, 10081 points). `contiguous` = minutes [0, 6200); `gap` = minutes [0, 3100) and [6981, 10081), i.e. the same 6,200 samples per series; `gap, no wrap` = minutes [0, 3100) and [5092, 8192), the same count and gap size, but no index reaches 8192; `late start` = minutes [4000, 10081). `max_threads = 1`, median of 7 warm runs (min–max).

| case | host | base sha | master | branch |
|---|---|---|---|---|
| `timeSeriesLastToGrid`, contiguous | 2 vCPU x86_64 VM | a7f71361 | 0.127 s (0.122–0.150) | pending build |
| `timeSeriesLastToGrid`, gap | 2 vCPU x86_64 VM | a7f71361 | 0.774 s (0.771–0.803) | pending build |
| `timeSeriesLastToGrid`, gap, no wrap | 2 vCPU x86_64 VM | a7f71361 | 0.126 s (0.124–0.150) | pending build |
| `timeSeriesLastToGrid`, late start | 2 vCPU x86_64 VM | a7f71361 | 0.099 s (0.099–0.123) | pending build |
| `timeSeriesRateToGrid`, contiguous | 2 vCPU x86_64 VM | a7f71361 | 0.156 s (0.150–0.183) | pending build |
| `timeSeriesRateToGrid`, gap | 2 vCPU x86_64 VM | a7f71361 | 0.645 s (0.640–0.670) | pending build |

The "gap, no wrap" row isolates the cause: the same samples and the same gap cost nothing extra when no index wraps.

A model of the table (linear probing, identity hash, ascending inserts; probes per series / final cells) predicts the branch column:

| shape (minutes) | master (3/4) | 1/2 limit (before #124773) | branch |
|---|---|---|---|
| [0, 6200) | 0 / 16384 | 0 / 16384 | 0 / 16384 |
| [0, 3100) + [6981, 10081) | 5.68M / 16384 | 0 / 16384 | 0 / 16384 |
| [0, 6000) + [10000, 10081) | 0.34M / 8192 | 0 / 16384 | 0 / 16384 |
| [4000, 10081) | 0 / 8192 | 0 / 16384 | 0 / 8192 |
| every 2nd minute | 2,988 / 8192 | 0 / 16384 | 0 / 16384 |
| [0, 1500) + [8581, 10081) | 1.67M / 4096 | 1.67M / 8192 | 1.67M / 8192 |
| [0, 2048) + [8192, 10081) | 3.86M / 8192 | 3.86M / 8192 | 3.86M / 8192 |

The last two rows are gap shapes that the 1/2 limit did not handle either; this PR does not change them (see "Not covered").

Branch column pending build; commands:
```
clickhouse local --path ./db --enable_time_series_aggregate_functions=1 --max_threads=1 --time -n -q "
  SELECT sum(length(r)) FROM (SELECT id, timeSeriesLastToGrid(0, 604800, 60, 60)(ts, v) r FROM g.s WHERE case='gap_mid' GROUP BY id) FORMAT Null;"   # x8, drop the first
```
and `tests/performance/timeseries_to_grid_gap.xml`, `timeseries_to_grid_bucketing.xml`, `timeseries_to_grid_aggregation_functions.xml`, `timeseries_to_grid_deserialization.xml` in the CI performance comparison. Indexes below the buffer size (all ascending series that start at the grid start, i.e. the sorted cases of `timeseries_to_grid_bucketing.xml`) cost one comparison more per bucket lookup.

Memory: the map grows earlier only when a wrapping index collides, by one doubling at a time from above 1/2 full, so its load factor never drops below 1/4 and its size is at most what the 1/2 limit gave. One common shape does give back the #124773 saving: a series with a sample in every 2nd bucket (step smaller than the scrape interval) ends at the 1/2-limit size (16384 cells instead of 8192 in the model) and saves about 3K probes per series.

### Not covered
Gaps whose wrapping indexes collide while the map is at most half full (the last two rows of the model) stay slow, as they were before #124773. Fixing those needs a different bucket layout (for example the dense array of #124777) or a different hash, which is beyond this change.

### Tests
- `05474_timeseries_to_grid_gap_wraparound`: four shapes (wrap at 1/2–3/4 fill, wrap below 1/2 fill, wrap at 3/4 fill, late start without collisions) through the add, batch add, merge and deserialize paths, each checked against the expected grid built without the aggregate functions, plus `timeSeriesRateToGrid` counts. Results are unchanged by this PR, so the test guards the new growth path's correctness; the performance test carries the speed evidence.
- `tests/performance/timeseries_to_grid_gap.xml`: the contiguous, gap and late-start cases above.

Closes: https://github.com/ClickHouse/ClickHouse/issues/125759
Related: https://github.com/ClickHouse/ClickHouse/pull/124773
Related: https://github.com/ClickHouse/ClickHouse/pull/124777

### Changelog category (leave one):
- Performance Improvement

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
Fixed a performance regression in 26.10: the `timeSeries*ToGrid` aggregate functions (and the PromQL range functions built on them) were up to 6× slower on a series with a gap in the middle of the time range than on a contiguous series with the same number of samples.

<details><summary>Self-review (clickhouse-gh rules, run locally)</summary>

Two rounds. Round 1 raised two Majors: the first rule missed collisions it could have caught, and late-start series lost the 3/4 memory saving. Both were fixed by growing only on a real collision, and the claim was narrowed. Round 2 found no Blockers or Majors. It returned "Request changes" only because the branch column needs a build; the code and the text were otherwise approved. Remaining Nits: the stride-2 memory note (now in the Memory paragraph) and a possible 26.10 backport label (#124773 is in v26.10.2.103-stable).
</details>
