# Grow the timeSeries*ToGrid bucket map before inserting a colliding key

Branch: `lilred-perf-grid-gap-wraparound` @ `e6d27f5cc1d4` (base master `52529f0c7115`; merges cleanly with master `66b092b52662`, 2026-11-07; the numbers below are from the build of the first head `46291048116b`; `0113a7c96c12` and `e6d27f5cc1d4` change the rule and need run 2, see the A/B plan) · compare: https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred-perf-grid-gap-wraparound

<!-- upstream:start (the PR body is everything between these markers; the title is the heading above) -->
Closes: https://github.com/ClickHouse/ClickHouse/issues/125759
Related: https://github.com/ClickHouse/ClickHouse/pull/124773
Related: https://github.com/ClickHouse/ClickHouse/pull/124777

### Changelog category (leave one):
- Experimental Feature

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
Improved the performance of the experimental `timeSeries*ToGrid` aggregate functions (and the PromQL range functions that use them) on series with a gap in the middle of the grid: up to 8.8× faster when the samples after the gap would otherwise collide with the samples before it.

### Description

The bucket map of the `timeSeries*ToGrid` states is a `HashMap` with `TrivialHash` and linear probing, so bucket index `k` lives in cell `k % bufSize`. Since #124773 the map fills to 3/4 before it grows. On a series with a gap in the middle of the grid, the indexes after the gap wrap onto the cells of the run before it, and while the map is between 1/2 and 3/4 full every insert of a key whose cell the other run holds walks that run (about 3,100 probes per bucket in the case below), whichever run arrives first.

Now a missing key whose home cell and the cell after it hold other keys (the start of a run, not the one-cell collisions of a sample in every 2nd bucket) doubles the map first when it is more than half full, unless the key itself would still wrap after the doubling. Such collisions grow the map at the pre-#124773 1/2 limit and never beyond that size; maps without them (contiguous series, late starts, every 2nd bucket) keep the 3/4 limit. The add, batch add, merge and deserialize paths share one helper; no setting or serialized-state change. When the run after the gap arrives first, inserts cost what they cost with the 1/2 limit (about 3× fewer probes than master in the case below, not 8.8×). Gaps that wrap onto a map below half full were slow before #124773 too and stay so; in time order they keep their size, in reverse order or merged into the later run the map doubles once (the 1/2-limit size). They need a different layout such as #124777.

Release build (clang 21), AMD EPYC 9R14, servers pinned to 16 cores; performance comparison with two servers on one host, 13 runs per query, base `52529f0c7115`; 200 series, one sample per minute, 7-day grid at 60 s, `max_threads = 1`.

| query | master | this PR | change |
|---|---|---|---|
| `timeSeriesLastToGrid`, contiguous | 99 ms | 99 ms | +0.4% |
| `timeSeriesLastToGrid`, gap | 877 ms | 99 ms | 8.8× faster |
| `timeSeriesRateToGrid`, contiguous | 144 ms | 145 ms | +0.8% |
| `timeSeriesRateToGrid`, gap | 945 ms | 145 ms | 6.5× faster |

Not slower: contiguous (above) and late-start series (−0.02%), all not significant.

Tests: `05474_timeseries_to_grid_gap_wraparound` checks four shapes (wrap at 1/2–3/4 fill, wrap below 1/2 fill, wrap at 3/4 fill, late start) plus a sample every 2nd minute, through add, batch add in time and reverse order, merge in both orders and deserialize, against a grid built without the aggregate functions. Results do not change, so master passes it too; the new perf test `timeseries_to_grid_gap.xml` (gap, later-run-first merge, sparse gap, every 2nd minute, four samples per bucket, controls) carries the speed evidence.
<!-- upstream:end -->

Full A/B (2026-11-05): A = `52529f0c7115`, B = `46291048116b`; AMD EPYC 9R14 (64 cores, 123 GiB, Ubuntu 24.04.5); clang++-21 release, `--reconfigure` so GIT_HASH matches HEAD; perf.py from B's tree against two servers on one host, pinned to cores 0-15, 13 runs per query.

| perf test | query | A median | B median | change | significant |
|---|---|---|---|---|---|
| timeseries_to_grid_gap | timeSeriesLastToGrid contiguous | 0.0986 s | 0.0990 s | +0.4% | same |
| timeseries_to_grid_gap | timeSeriesLastToGrid gap | 0.877 s | 0.0994 s | -88.7% (8.8x) | faster |
| timeseries_to_grid_gap | timeSeriesLastToGrid late_start | 0.0869 s | 0.0869 s | -0.02% | same |
| timeseries_to_grid_gap | timeSeriesRateToGrid contiguous | 0.144 s | 0.145 s | +0.8% | same |
| timeseries_to_grid_gap | timeSeriesRateToGrid gap | 0.945 s | 0.145 s | -84.6% (6.5x) | faster |

Tests: 05474 passes on A and B (results unchanged, as expected). No A/A figure in this run; the three unaffected queries (±0.8%) bound the noise. The other `timeseries_to_grid_*.xml` files were not in this run; CI's performance comparison covers them (the sorted cases of `timeseries_to_grid_bucketing.xml` pay one extra comparison per bucket lookup).

Model of the table (linear probing, identity hash, ascending inserts; probes per series / final cells), kept from the unbuilt draft:

| shape (minutes) | master (3/4) | 1/2 limit (before #124773) | branch |
|---|---|---|---|
| [0, 6200) | 0 / 16384 | 0 / 16384 | 0 / 16384 |
| [0, 3100) + [6981, 10081) | 5.68M / 16384 | 0 / 16384 | 0 / 16384 |
| [0, 6000) + [10000, 10081) | 0.34M / 8192 | 0 / 16384 | 0 / 16384 |
| [4000, 10081) | 0 / 8192 | 0 / 16384 | 0 / 8192 |
| every 2nd minute | 2,988 / 8192 | 0 / 16384 | 0 / 16384 |
| [0, 1500) + [8581, 10081) | 1.67M / 4096 | 1.67M / 8192 | 1.67M / 8192 |
| [0, 2048) + [8192, 10081) | 3.86M / 8192 | 3.86M / 8192 | 3.86M / 8192 |

<details>
<summary>Self-review (preflight) and notes for James</summary>

- Rounds 1–2 (2026-10-31, unbuilt): round 1 raised two Majors (the first rule missed collisions it could catch; late-start series lost the 3/4 saving), both fixed by growing only on a real collision. Round 2: no Blockers or Majors; "Request changes" only for the missing branch numbers.
- Round 3 (2026-11-07, on `46291048116b` with your run-1 numbers): ⚠️ Request changes, two Majors the run didn't cover. (1) Arrival order: the rule only grew for keys at or above the buffer size, so when the run after the gap comes first (reverse order, or a `-Merge` into a state holding the later run, which a multi-threaded GROUP BY can produce) the low keys walked the whole run as on master. Confirmed on a master binary: that merge is as slow as the gap case. (2) A sparse gap doubled the map for no gain. Fixed in `0113a7c96c12`; nits fixed too (Rate window 120 s in the perf test, where 60 s made every point NULL; a note at the map alias).
- Round 4 (on `0113a7c96c12`): ⚠️ Request changes. Major: a sample in every 2nd bucket (scrape interval = 2 × step) doubled the map vs master to save ~3K probes per series; the first head had this too. Fixed in `e6d27f5cc1d4` (grow only when the next cell is taken as well). Major 2 (a sparse gap inserted in reverse or merged into the later run doubles once, same speed) is disclosed in the text instead of adding a `bucket_count` bound, which would bring back master's probing for reverse-order gaps on grids longer than 2× the map. Round cap reached; no round 5. Run 2 (memory per query) is the check.
- 2026-11-07: rewritten to the template order (links, changelog, description; about 2k characters, no sub-headings); category switched to `Experimental Feature` (your TimeSeries/PromQL convention; the draft said Performance Improvement). The entry now says "up to 8.8× faster" (your build), not the draft's "up to 6× slower" (my 2 vCPU emulation; the gap was 8.9× slower than contiguous on your host).
- For you to decide: #124773 is in v26.10.2.103-stable, so this fixes a 26.10 slowdown; a backport label (`pr-must-backport` or `v26.10-must-backport`) is your call.
- Memory cost (in the model): a series with a sample in every 2nd bucket ends at the 1/2-limit size (16384 cells instead of 8192). Not measured.
- Perf XML: extending `timeseries_to_grid_bucketing.xml` instead of adding `timeseries_to_grid_gap.xml` is an option; moving it now needs a new build and run, so I kept the new file.
- Test number 05474 is used by six other tests on master (numbers aren't unique there); the name is unique.
- Overlap (checked 2026-11-07): #125759 still open, no other upstream PR references it; #124777 (dense array, open, updated 2026-11-02) is the wider alternative, complementary.
- Commits: `46291048116b` on base `52529f0c7115`; its commit carries a `Co-Authored-By` trailer from before your no-trailer rule (you squash).

</details>
