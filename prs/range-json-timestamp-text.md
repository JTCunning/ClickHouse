# [unbuilt] Format the timestamps shared by the series of a range vector once per block

**Branch:** [`lilred-perf-range-json-timestamp-text`](https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred-perf-range-json-timestamp-text) @ `ff2ee0f3f036` (base master `521c4bcb7b94`) · **Status:** unbuilt, ready to build · **Report:** [reports/2026-11-21-range-json-timestamp-text.md](../reports/2026-11-21-range-json-timestamp-text.md) · **A/B plan:** [ab-plans/range-json-timestamp-text.md](../ab-plans/range-json-timestamp-text.md)

<!-- upstream:start (the PR body is everything between these markers; the title is the heading above) -->
Related: https://github.com/ClickHouse/ClickHouse/pull/115567

### Changelog category (leave one):
- Experimental Feature

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
Faster `/api/v1/query_range` responses of the Prometheus HTTP API for queries that return many series: the step timestamps shared by the series of a result block are formatted once per block instead of once per series ([pending build]% faster for 2,000 series × 1,441 steps).

### Description

`/api/v1/query_range` writes every point of its matrix as `[timestamp,"value"]`, and it formatted the timestamp with `writeText(Decimal64, scale)` for every point. Every series of a range query is evaluated at the same steps (`start + k * step`), so a query over 2,000 series formatted each of its 1,441 step timestamps 2,000 times. This runs in the single thread that writes the response after the query's final `ORDER BY`, so it adds to the request time one to one. In a wall-clock profile of master on a small host, formatting the timestamps took 40-56 ms of a 700 ms request (2,000 series × 1,441 points, integer values) and 10-22 ms of a 550 ms `rate()` over the same series. #115567 already formats the one timestamp of an instant vector once per block.

Now, for `/api/v1/query_range`, the writer formats the timestamps of the longest series of each block once, and every series copies the text of each timestamp it shares with that series. A series' timestamps are increasing, so its next point is usually at the next formatted timestamp; a series that starts later or skips steps gallops forward (+1, +2, +4, … then a binary search of the last stride), so skipping `g` steps costs `O(log g)`. A step the longest series has no point at is formatted as before. Unchanged: blocks of one series, and `/api/v1/query`, including the raw samples of a range selector (`m[5m]`), whose timestamps differ between series. The text comes from the same function, so the response is byte for byte the same. Memory: per point of the block's longest series, one timestamp, one offset and its text (about 30 bytes).

Release build, [pending build: host], master `521c4bcb7b94` and this PR, two servers on the same data, `[pending build]` interleaved requests per case:

| case (6 h of 15 s samples) | master | this PR |
|---|---|---|
| `m`, 2,000 series, step 15 s (2.88M points) | [pending build] | [pending build] |
| `rate(m[5m])`, 2,000 series, step 30 s (1.44M points) | [pending build] | [pending build] |
| `g` (3-decimal gauge), 2,000 series, step 60 s (720k points) | [pending build] | [pending build] |
| skewed block: `c`, 1 series × 1,440 points next to 1,000 series × 72 points at its end, step 15 s | [pending build] | [pending build] |
| every other step: `h`, 1 series × 1,440 points next to 1,000 series × 720 points, step 15 s, `lookback_delta=10s` | [pending build] | [pending build] |
| control: `sum(rate(m[5m]))`, 2,000 series, step 30 s (one result series) | [pending build] | [pending build] |

[pending build: A/A spread.] Responses are identical to master's (md5 of every case's body).

`test_query_api.py::test_range_vector_timestamps_shared_between_series` checks the exact text of a range query in which the shorter series have steps the longest one lacks (before its first point and after its last one), with fractional timestamps, the same with one series per block (`max_block_size=1`), and of a range selector in an instant query. Copying the text of the timestamp before or after the matching one, or dropping the equality check, fails it. It is an integration test because stateless tests have no Prometheus API handler.
<!-- upstream:end -->

**Note for James (not part of the PR):** the review bot suggests `Performance Improvement` here (the instant-vector precedent #115567 went into the changelog under it); I kept `Experimental Feature`, your habit for TimeSeries PRs, so it's your call. the claim is about wall time on wide, non-aggregated range panels; on a 16-core host the writer is a bigger share of the request than on my 2 vCPU, so the gain there should be larger, but that is a prediction. The test is an integration test (stateless tests have no Prometheus handler), so it needs the integration runner.

<details><summary>Self-review (lil red, preflight)</summary>

Preflight against the review prompt at master `521c4bcb7b94` (independent reviewer, 3 rounds, the limit):
- **Round 1: ⚠️ Request changes.** [Major] the per-series scan restarted at index 0 and walked the cache linearly, so a block with one long series and many short late ones cost series × points comparisons and could be slower than master. Fixed: the cache is used only for `/api/v1/query_range` and for blocks of more than one series, and the cursor jumps forward. Nits taken: description wording, a past-the-end test point (`raw` at 2002.5), a skewed benchmark case.
- **Round 2: no Blocker or Major;** ⚠️ only for the pending numbers. Taken: the changelog wording, series counts per row, and the recommended every-other-step case, with a galloping search (+1, +2, +4, … then bisect) so a series that skips steps doesn't pay a full binary search per point. The gallop was checked against `std::lower_bound` from the cursor on 200,000 random caches and series (same cursor, same hits; clang 20, `-Wsign-conversion` clean).
- **Round 3: ✅ Approve** at `4acc6e56f9f`, conditional on the build numbers showing a gain beyond the A/A spread. Nit taken (comment-only, `ff2ee0f3f036`): the invariant after the gallop. Not taken: a gtest of the search (it isn't a free function).
- Category: the reviewer suggests `Performance Improvement` (#115567's precedent); kept `Experimental Feature` for James to decide.

</details>
