# [unbuilt] Probe the primary key of parts in parallel when deciding to load statistics

Branch: `lilred-perf-parallel-statistics-probe` · compare: https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred-perf-parallel-statistics-probe

<!-- upstream:start (the PR body is everything between these markers; the title is the heading above) -->
Caused by: https://github.com/ClickHouse/ClickHouse/pull/126627

### Changelog category (leave one):
- Not for changelog (changelog entry is not required)

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):


### Description

#126627 bounds the read with a primary key probe (`MergeTreeDataSelectExecutor::hasFewerMarksToReadThan`) before it loads statistics for pruning parts and for ordering `PREWHERE`. The probe walks the parts one after another, and probing a part loads its primary index if it is not loaded yet. With `primary_key_lazy_load` (the default) that is every part after a restart, and every part a replica did not write. On a remote disk each load is a request, so a cold query waits for one request per part in turn inside `QueryPlan::optimize`, although the range analysis right after the probe loads the same indexes in parallel.

Now, when the indexes of more than one part are not in memory (neither loaded nor in the primary index cache), the probe runs on as many threads as there are such parts, at most `max_threads` (and `max_threads_for_indexes`, as in the range analysis). It still stops once the limit is reached, but the threads finish the parts they took, so it can read more indexes than the serial probe, which stops at the part that reaches the limit. With the indexes in memory it stays serial, as before. No setting, threshold or format change.

Setup: release build of master 7b5f2bf328f9, 2 vCPU, `max_threads = 16`, an `s3` disk with a filesystem cache over a local S3 endpoint that adds 50 ms per GET; a PromQL range query over a `TimeSeries` table with 64 fresh parts per inner table (13 samples parts left after pruning); all caches dropped and primary keys unloaded before each run; median of 5, branch column pending build.

| case | master | branch |
|---|---|---|
| cold query | 3,204 ms | pending build |
| the probe's 13 index loads, 51 ms apart | 617 ms | pending build (one parallel wave expected) |
| master with these 13 indexes loaded in parallel beforehand | 2,540 ms | |
| warm query (control: indexes loaded, serial path) | 98 ms | pending build |
| warm query, inner tables with `use_primary_key_cache = 1` (control: cached indexes, serial path) | 103 ms | pending build |

A/A: two cold series on master have medians of 3,204 and 3,209 ms.

Correctness: query results do not depend on the decision, and the parallel probe decides as the serial one unless a probe throws: an index that fails to load can now fail the query even where the serial probe would have stopped before its part. Tests: `05620_parallel_statistics_probe` runs the small and the large read of `05407_statistics_skip_small_reads` after unloading the primary indexes, with `max_threads` 4 and 1, on a table with and one without `use_primary_key_cache`, and checks `LoadedStatistics` and `SelectedParts`. A parallel path that drops the probed marks turns the large-read lines red, one that always answers "large" turns the small-read lines red; master passes, it is serial. The speed-up is shown by the timings, not by the test. `05407` runs with `max_threads = 1` and does not change.

Cost: on a large read the threads can read the indexes of parts that statistics then prune, at most one part per thread after the limit is reached, all at once.
<!-- upstream:end -->

## For James (not part of the PR body)

**Why this is on the PromQL handoff (#15).** It's the one piece of the cold-read cost inside `QueryPlan::optimize` that a small code change removes. It's generic MergeTree code, and it arrived on master after your captures: #126627 merged 2026-11-07 into 26.11.1.1600, and your staging runs were on 26.11.1.1102 and 1311. The PR is synced to Cloud, so the next staging upgrade gets this serial probe too. Report: `reports/2026-11-11-tags-in-subquery-cold.md`.

**Category.** #126627 isn't in a release yet (release branches end at 26.10), so this fixes an unreleased regression: `Not for changelog`, with `Caused by:`. If 26.11 is cut with #126627 before this lands, switch to `Performance Improvement` with an entry like "Cold queries on remote disks no longer load the primary indexes of the parts one by one before deciding whether to load statistics."

**Emulation, all on master 7b5f2bf328f9 with 50 ms per GET** (lab: `bench/tags-in-subquery-cold/`):

| case | median ms (min–max) | n | what it shows |
|---|---|---|---|
| cold (master as is) | 3,204 (3,152–3,246) | 5 | the 13 recent-samples index loads arrive 51 ms apart (617 ms span) |
| cold, second series (A/A) | 3,209 (3,185–3,230) | 5 | noise ±0.2% |
| cold, 13 indexes preloaded in one parallel wave first | 2,540 (2,481–2,812) | 5 | what the branch should approach: about 2.6 s once the parallel wave (≈ 1 request) is added back |
| cold, probe off (`min_marks_per_part_to_load_statistics = 0`) | 2,649 (2,606–2,709) | 3 | the behavior before #126627: statistics and indexes load in parallel |
| cold, statistics off (`use_statistics = 0`) | 2,309 (2,309–2,321) | 3 | floor without any statistics reads |
| warm | 98 (96–132) | 5 | control: every index loaded, the serial path is unchanged |

**A/B plan:** `ab-plans/parallel-statistics-probe.md`.

<details>
<summary>Self-review (preflight, 3 rounds, bot instructions at master 5252bd271c80)</summary>

| round | sev | finding | verdict | action |
|---|---|---|---|---|
| 1 | ⚠️ Major | With `use_primary_key_cache = 1` the index is never "loaded" in the part, so every warm probe started a pool | AGREE | 70e20937daff: `isIndexInMemory()` also checks the cache; parallel only when ≥ 2 parts must read their index, pool sized by that count; 05620 runs a cache table too |
| 1 | 💡 Nit | "at most one more part per thread" isn't a bound against the serial probe | AGREE | comment and PR text: the threads finish the parts they took; at most one per thread after the limit |
| 1 | 💡 Nit | a failing index load past the serial prefix, or `CANNOT_SCHEDULE_TASK`, can now fail planning | AGREE (text) | PR text says "unless a probe throws"; kept `scheduleOrThrowOnError` as in the range analysis' pool, which the same plans reach next (round 2 judged this acceptable) |
| 1 | ⚠️ (tests) | the test can't prove the parallel path ran | DISAGREE | the test pins the decision on both paths (round 2 traced that both tables reach the parallel branch, so TSan covers it); the speed-up belongs to the A/B, not a new ProfileEvent |
| 2 | 💡 Nit | fourth copy of the cache key | AGREE | 41e5fec573dc: `getPrimaryIndexCacheKey()` for all four |
| 2 | 💡 Nit | the pre-scan visits every part | AGREE (partly) | counting stops at the thread cap; a warm read still visits each part once (inherent); no `containsMany`/`try_to_lock` (round 3: acceptable) |
| 3 | — | no open code findings | — | remaining gates: a build, CI green (clang-tidy, TSan of 05620), the branch column from the A/B |

Mock check (not in the PR): the function body against stub types under clang 20, 32 count/limit cases plus a one-cold-part case, all decide as the serial loop. `05620` and its reference were produced on the master binary (serial path).

Before submission: fill the branch column (cold, the 13-load span, warm, warm with the cache), replace "one parallel wave expected" with the measured span, drop `[unbuilt]`.

</details>
