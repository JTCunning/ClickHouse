# Grow the timeSeries*ToGrid bucket map before inserting a colliding key

**Status 2026-11-10: not ready to submit.** Run 2 failed its pass rule, and review rounds 5–7 found three more shapes where the rule misfires; the round-7 Majors on `84bdb18276cb` are open (see the self-review below). Waiting for James's decision on issue #10: park in favour of #124777, or one more iteration.

Branch: `lilred-perf-grid-gap-wraparound` @ `84bdb18276cb` (base master `52529f0c7115`; merges cleanly with master `8f3f96290d5b`, 2026-11-10). The numbers below are run 2 (James's build of `e6d27f5cc1d4`). Since then: `8544ce7267c8` (comments, merge order pinned in the tests, early-first merge query) and `84bdb18276cb` (the rule grows only when the doubling separates the two keys; a model puts every run-2 case where it was, and fixes the sparse gap's reverse-order and later-run-first doubling). Run 3 re-measures everything on `84bdb18276cb`, the bucketing guard on byte-identical data · compare: https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred-perf-grid-gap-wraparound

<!-- upstream:start (the PR body is everything between these markers; the title is the heading above) -->
Closes: https://github.com/ClickHouse/ClickHouse/issues/125759
Related: https://github.com/ClickHouse/ClickHouse/pull/124773
Related: https://github.com/ClickHouse/ClickHouse/pull/124777

### Changelog category (leave one):
- Experimental Feature

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
Improved the performance of the experimental `timeSeries*ToGrid` aggregate functions (and the PromQL range functions that use them) on series that cover a large part of the grid but have a gap in the middle: up to 8.4× faster.

### Description

The bucket map of the `timeSeries*ToGrid` states is a `HashMap` with `TrivialHash` and linear probing, so bucket index `k` lives in cell `k % bufSize`. Since #124773 the map fills to 3/4 before it grows. On a series with a gap in the middle of the grid, the indexes after the gap wrap onto the cells of the run before it, and while the map is between 1/2 and 3/4 full every insert of a key whose cell the other run holds walks that run (about 3,100 probes per bucket in the case below), whichever run arrives first. A multi-threaded `GROUP BY` hits the same walk when it merges the two halves of a series split between reader threads.

Now a missing key whose home cell and the cell after it hold other keys (the start of a run, not the one-cell collisions of a sample in every 2nd bucket) doubles the map first when it is more than half full and the doubling moves it and the key in its home cell to different halves, so that they stop colliding. Such collisions grow the map at the pre-#124773 1/2 limit and never beyond that size; maps without them, and runs a doubling would leave on the same cells, keep the 3/4 limit. The add, batch add, merge and deserialize paths share one helper; no setting or serialized-state change. Limits: when two states of the series are merged, the first ~1,000 inserts of the second land in a map below half full, so the merge gets about 2× faster (as with the 1/2 limit), not 8×. Keys at least twice the map size keep master's growth (growing for them would also grow maps whose keys are scattered over the grid, such as the partial states of a `GROUP BY` over many parts), so a gap that ends far past the map size stays as slow as on master: a sparse series, as before #124773, and a long grid such as 30 days at 60 s, which the 1/2 limit grew out of. A layout such as #124777 covers both.

Release build (clang 21), AMD EPYC 9R14, two servers on one host pinned to 16 cores, performance comparison with 13 runs per query, base `52529f0c7115`; 200 series, one sample per minute, 7-day grid at 60 s, `max_threads = 1`.

| query | master | this PR | change |
|---|---|---|---|
| `timeSeriesLastToGrid`, gap | 880 ms | 104 ms | 8.4× faster |
| `timeSeriesRateToGrid`, gap | 969 ms | 171 ms | 5.7× faster |
| `timeSeriesLastToGridMerge`, two runs (order not pinned) | 928 ms | 481 ms | 1.9× faster |
| `timeSeriesLastToGrid`, contiguous | 103 ms | 101 ms | −1.4% |

Not slower: contiguous, late-start, sparse-gap, every-2nd-minute and four-per-bucket series are within ±2.3% and not significant, with the same `memory_usage`. `timeseries_to_grid_aggregation_functions.xml`: 21 of 35 queries 3–12% faster (its series are split between reader threads), none slower. `timeseries_to_grid_bucketing.xml`: pending (its fill leaves several parts that each span every id, so it is compared on byte-identical copies of one fill).

Results are identical to master. Tests: `05474_timeseries_to_grid_gap_wraparound` checks four shapes plus a sample every 2nd minute through add, batch add in time and reverse order, merge in both orders and deserialize, against a grid built without the aggregate functions. Results do not change, so master passes it too; `timeseries_to_grid_gap.xml` carries the speed evidence.
<!-- upstream:end -->

Full A/B, run 2 (2026-11-10): A = `52529f0c7115` (the run-1 A binary), B = `e6d27f5cc1d4`; AMD EPYC 9R14 (64 cores, 123 GiB, Ubuntu 24.04.5); clang++-21 release, `--reconfigure` so GIT_HASH matches HEAD; perf.py from B's tree against two servers on one host, pinned to cores 0-15, 13 runs per query; memory = median `memory_usage` from `system.query_log` (14 runs incl. prewarm).

| case | query | A median | B median | change | significant | A memory | B memory |
|---|---|---|---|---|---|---|---|
| T1 | `timeSeriesLastToGrid`, gap | 0.880 s | 0.104 s | -88.1% (8.4x) | faster (p 5.4e-24) | 101.2 MiB | 101.2 MiB |
| T2 | `timeSeriesRateToGrid` (120 s window), gap | 0.969 s | 0.171 s | -82.3% (5.7x) | faster (p 1.2e-34) | 126.2 MiB | 126.2 MiB |
| T3 | `timeSeriesLastToGridMerge`, later run first | 0.928 s | 0.481 s | -48.1% (1.93x) | faster (p 5.3e-48) | 106.1 MiB | 106.1 MiB |
| G1 | `timeSeriesLastToGrid`, contiguous | 0.103 s | 0.101 s | -1.4% | no | 101.2 MiB | 101.2 MiB |
| G2 | `timeSeriesRateToGrid`, contiguous | 0.170 s | 0.171 s | +0.4% | no | 126.2 MiB | 126.2 MiB |
| G3 | `timeSeriesLastToGrid`, late_start | 0.088 s | 0.090 s | +1.6% | no | 51.9 MiB | 51.9 MiB |
| G4 | `timeSeriesLastToGrid`, gap_sparse | 0.283 s | 0.285 s | +0.7% | no | 41.3 MiB | 41.3 MiB |
| G6 | `timeSeriesLastToGrid`, every_2nd | 0.079 s | 0.081 s | +1.8% | no | 51.9 MiB | 51.9 MiB |
| G7 | `timeSeriesLastToGrid`, dense | 0.175 s | 0.179 s | +2.3% | no (p 0.049) | 102.0 MiB | 102.0 MiB |
| G5 | `timeseries_to_grid_aggregation_functions.xml`, 35 queries | | | -12.0% to +0.7% | 21 faster, 14 same, none slower | | |
| G5 | `timeseries_to_grid_bucketing.xml`, 7 queries (each server filled on its own) | | | -5.7% to +22.9% | 5 slower, 1 faster, 1 same | | |

The bucketing result is not used as evidence either way. Its fill is the layout draw that #122319 describes for `timeseries_to_grid_aggregation_functions.xml` (several parts that each span every id; which timestamps land in which part differs per server and per fill), so the two servers did not read the same layout (A/A below). A model of the bucket map on the same layouts puts the branch at −7% to +1% of master's bucket-map time, which is a small share of those queries. Run 3 compares A and B on byte-identical copies of one fill, so the multi-part layout (and its cross-thread merges, which this PR changes) stays in the test.

A/A on one master binary (2 vCPU VM, `max_threads = 16`, `max_threads_min_free_memory_per_thread = 0`, `timeSeriesLastToGrid(0, 300000, 300, 300)` over `ts_bucketing`, merges stopped after the fill, median of 7 warm runs): fill 1 (20 parts) 0.480 s, fill 2 (20 parts) 0.470 s, fill 3 (15 parts) 0.516 s; merged to one part 0.727 s. `bench/grid-gap-wraparound/layout-draw/run.sh`. I tried pinning the file to one part as #122319 did (an `OPTIMIZE ... FINAL`, 7.1 s + 4.0 s of fill time here) and dropped it before pushing: with one part most ids are read by one thread, so the guard would stop exercising the cross-thread merge this PR changes, and it would change what an upstream file measures in a PR about something else.

Model of the bucket map (`bench/grid-gap-wraparound/model/model.cpp`, clang 20 with the release flags; bucket-map work only, no read path; `run2` = `e6d27f5cc1d4`, `sep` = `84bdb18276cb`, `occ` = the separation check without the `2 × map size` limit), median of 5, change vs master:

| shape | run2 | sep | occ |
|---|---|---|---|
| gap, time order / reverse | −94% / −95% | −94% / −95% | −94% / −95% |
| gap, `-Merge` late first / early first | −54% / −38% | −54% / −36% | −54% / −35% |
| sparse gap, time order | 0%, same cells | −1%, same cells | 0%, same cells |
| sparse gap, reverse | **+29%, 2× cells** | 0%, same cells | −1%, same cells |
| sparse gap, `-Merge` late first | **+24%, 2× cells** | +0.5%, same cells | +0.3%, same cells |
| every 2nd / late start / contiguous, all reverse | +4% / +1% / +4% | +4% / 0% / 0% | +4% / 0% / −2% |
| 30-day grid gap, time order | 0% | −1% | −90% |
| 30-day grid gap, reverse / `-Merge` late first | −94% / −56% | −94% / −56% | −94% / −56% |
| `aggregation_functions` layout (one part, split series) | −5% | −7% | −7% |
| `bucketing` layout draws 1–6 (partial cells vs master) | −3% to −1% (+3–11%) | −6% to 0% (+3–11%) | −4% to +7% (+19–25%) |

Master between layout draws: ±12% of its own bucket-map time. `occ` would fix the 30-day gap but grows the partial states of the multi-part layouts by about a quarter, so the branch keeps the limit.

<details>
<summary>Self-review (preflight) and notes for James</summary>

- Rounds 1–2 (2026-10-31, unbuilt): round 1 raised two Majors (the first rule missed collisions it could catch; late-start series lost the 3/4 saving), both fixed by growing only on a real collision. Round 2: no Blockers or Majors; "Request changes" only for the missing branch numbers.
- Round 3 (2026-11-07, on `46291048116b` with your run-1 numbers): ⚠️ Request changes, two Majors the run didn't cover. (1) Arrival order: the rule only grew for keys at or above the buffer size, so when the run after the gap comes first (reverse order, or a `-Merge` into a state holding the later run, which a multi-threaded GROUP BY can produce) the low keys walked the whole run as on master. (2) A sparse gap doubled the map for no gain. Fixed in `0113a7c96c12`.
- Round 4 (on `0113a7c96c12`): Major: a sample in every 2nd bucket doubled the map vs master; fixed in `e6d27f5cc1d4`. A sparse gap inserted in reverse or merged into the later run doubles once (same speed), disclosed.
- Run 2 (2026-11-10) failed two pass_if lines. T3 1.93× vs my 2× floor: my "about 3×" prediction was wrong; the second state's 3,100 keys go into an 8,192-cell map that is 38% full, so the first ~1,000 inserts walk the wrapped run before the rule (or the old 1/2 limit) can fire. The claim now says about 2×. Also, the T3 query's `ORDER BY ... late DESC` was dropped by `query_plan_remove_redundant_sorting` (EXPLAIN on master shows no Sorting step), so run 2 measured an unpinned mix of orders; `8544ce7267c8` pins it and adds the early-first order. Bucketing guard: see above.
- Round 5 (2026-11-10, on a one-part pin commit and the first draft of this text): ⚠️ Request changes. Majors: (1) the `key < 2 × map size` cap leaves a gap that ends far past the map size as slow as master (a 30-day grid at 60 s; the 1/2 limit grew out of it), and the changelog promised more; the text blamed sparse gaps on "below half full" when the cap is the reason. Fixed in the text, the helper comment and the test comments; the changelog now says "series that cover a large part of the grid". (2) The layout pin belongs in its own PR and would hide the cross-thread merges: dropped. Test notes: the merge order wasn't pinned (fixed); add the early-first merge (added). Nit not taken: key 0 (stored outside the cells) can trigger one doubling when cells 0 and 1 are taken; harmless, and fixing it changes the code run 2 measured.
- Round 6 (2026-11-10, on `8544ce7267c8`): ⚠️ Request changes. Round-5 points resolved. Major: the cap looks only at the inserted key, so when the run after a sparse gap arrives first (reverse batch, later-run-first merge) the near keys doubled the map twice for nothing (8192 vs master's 4096 cells; +24–29% in the model). Fixed in `84bdb18276cb`: grow only if the doubling separates the key from the key in its home cell (needs `keyInCell`, so the map is now a struct over `HashMap`). Nits: early-first twin in 05474 (added); a sparse late-first merge in the perf test for `memory_usage` (added).
- Round 7 (on `84bdb18276cb`, the round cap of this run): ⚠️ Request changes, two Majors, open. (1) The run test reads one cell ahead, so any two filled neighbours qualify: a jittered every-2nd-bucket series (scrape interval 2 × step, ~5% of samples a step late) doubles 8192 → 16384 cells in ~9 of 10 series for ~6K probes of saving, and a random 30% of minutes 4096 → 8192 (affects `e6d27f5cc1d4` too). Suggested: require a real run (cells `home+1 .. home+8` taken; 8 ≈ linear probing's expected miss length at 3/4 fill, (1 + 1/(1−0.75)²)/2 = 8.5). (2) New in `84bdb18276cb`: the separation test reads only the home cell's key; once the incoming run has passed a short wrapped run, its home cells hold its own displaced keys and the test never fires, so merges such as `[0,4000) ∪ [8192,10081)` later-run-first fall back to master's speed (`e6d27f5cc1d4` had 1.8× fewer probes). Suggested: compare the two keys modulo `2 × bufSize` (`((home_key ^ bucket_index) & (2 * buffer_size - 1)) != 0`). Nit: the example's "insert as production does" comment is stale. Not fixed: round cap; and each round has found a new shape, so this needs a decision first.
- For you to decide: backport label (26.10): #124773 is in v26.10.2.103-stable.
- Can't compile ClickHouse here: the new struct and helper were checked in a mock of the HashTable/HashMapTable inheritance with clang 20 `-Wall -Wextra -Wshadow -Wconversion -Werror`, and 05474 passes on a master binary with the new reference.
- Perf XML: extending `timeseries_to_grid_bucketing.xml` instead of adding `timeseries_to_grid_gap.xml` is an option; I kept the new file (its queries use `max_threads = 1` and a 7-day grid, unlike the bucketing file).
- Test number 05474 is used by six other tests on master (numbers aren't unique there); the name is unique.
- Overlap (checked 2026-11-10): #125759 still open; #124777 (dense array, open, last updated 2026-11-02) is the wider alternative, complementary; #122983 (sorted sample buffer for rate/increase/delta) open.
- Commits: `46291048116b` carries a `Co-Authored-By` trailer from before your no-trailer rule (you squash).

</details>
