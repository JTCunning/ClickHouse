# Format the timestamps shared by the series of a range vector once per block

**Branch:** [`lilred-perf-range-json-timestamp-text`](https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred-perf-range-json-timestamp-text) @ `ff2ee0f3f036` (base master `521c4bcb7b94`) · **Status:** built (James, 2026-11-21, #22 issuecomment-7059502143): pass; ready to submit · **Report:** [reports/2026-11-21-range-json-timestamp-text.md](../reports/2026-11-21-range-json-timestamp-text.md) · **A/B plan:** [ab-plans/range-json-timestamp-text.md](../ab-plans/range-json-timestamp-text.md)

<!-- upstream:start (the PR body is everything between these markers; the title is the heading above) -->
Related: https://github.com/ClickHouse/ClickHouse/pull/115567

### Changelog category (leave one):
- Experimental Feature

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
Faster `/api/v1/query_range` responses of the Prometheus HTTP API for queries that return many series: the step timestamps shared by the series of a result block are formatted once per block instead of once per series (10% faster for 2,000 series × 1,441 steps).

### Description

`/api/v1/query_range` writes every point of its matrix as `[timestamp,"value"]`, and it formatted the timestamp with `writeText(Decimal64, scale)` for every point. Every series of a range query is evaluated at the same steps (`start + k * step`), so a query over 2,000 series formatted each of its 1,441 step timestamps 2,000 times. This runs in the single thread that writes the response after the query's final `ORDER BY`, so it adds to the request time one to one. In a wall-clock profile of master on a 2-vCPU host, formatting the timestamps took 40-56 ms of a 700 ms request (2,000 series × 1,441 points, integer values) and 10-22 ms of a 550 ms `rate()` over the same series. #115567 already formats the one timestamp of an instant vector once per block.

Now, for `/api/v1/query_range`, the writer formats the timestamps of the longest series of each block once, and every series copies the text of each timestamp it shares with that series. A series' timestamps are increasing, so its next point is usually at the next formatted timestamp; a series that starts later or skips steps gallops forward (+1, +2, +4, … then a binary search of the last stride), so skipping `g` steps costs `O(log g)`. A step the longest series has no point at is formatted as before. Unchanged: blocks of one series, and `/api/v1/query`, including the raw samples of a range selector (`m[5m]`), whose timestamps differ between series. The text comes from the same function, so the response is byte for byte the same. Memory: per point of the block's longest series, one timestamp, one offset and its text (about 30 bytes).

Release build (clang 21, RelWithDebInfo), AMD EPYC 9R14, servers and load generator pinned to 16 cores, master `521c4bcb7b94` and this PR as two servers on the same data (a `TimeSeries` table with 2,000 series of a counter `m` and a gauge `g`, 15 s samples over 6 h); 40 requests per case and server in rotating order, request wall time, median (p95):

| case | master | this PR | change |
|---|---|---|---|
| `m`, step 15 s: 2,000 series × 1,441 points, 57 MB | 191.6 ms (203.3) | 172.0 ms (189.7) | -10.2%, p < 0.001 |
| `rate(m[5m])`, step 30 s: 2,000 × 721, 50 MB | 163.1 ms (167.3) | 154.5 ms (160.5) | -5.3%, p < 0.001 |
| `g` (3-decimal gauge), step 60 s: 2,000 × 361, 16 MB | 74.0 ms (78.6) | 71.6 ms (78.6) | -3.3%: not slower, inside the A/A spread |
| skewed block: 1 series × 1,440 points next to 1,000 series × 72 points at its end, step 15 s | 30.7 ms (34.5) | 30.8 ms (33.2) | +0.3%, same |
| every other step: 1 series × 1,440 points next to 1,000 series × 720, step 15 s, `lookback_delta=10s` | 58.9 ms (63.9) | 54.1 ms (61.6) | -8.3%, p < 0.001 |
| control: `sum(rate(m[5m]))`, step 30 s, one result series | 55.5 ms (59.1) | 56.0 ms (59.2) | +0.8%, same |

A/A (master on both servers, 10 requests each): every case within -3.9% to +4.0%, none significant. The query's own CPU barely moves (`m`: 415.7 to 402.8 ms, -3.1%) while its duration drops from 159 to 140 ms: the saving is in the single thread that writes the response. The response bodies are byte-identical to master's in every case but the control, whose summed values differ in their last digits between two requests to master as well (relative difference under 1e-15).

`test_query_api.py::test_range_vector_timestamps_shared_between_series` checks the exact text of a range query in which the shorter series have steps the longest one lacks (before its first point and after its last one), with fractional timestamps, the same with one series per block (`max_block_size=1`), and of a range selector in an instant query. Copying the text of the timestamp before or after the matching one, or dropping the equality check, fails it; it passes on master and on this PR (63 passed with `test_evaluation.py`). It is an integration test because stateless tests have no Prometheus API handler.
<!-- upstream:end -->

**Note for James (not part of the PR):** the review bot suggests `Performance Improvement` here (the instant-vector precedent #115567 went into the changelog under it); I kept `Experimental Feature`, your habit for TimeSeries PRs, so it's your call. the claim is about wall time on wide, non-aggregated range panels; on a 16-core host the writer is a bigger share of the request than on my 2 vCPU, so the gain there should be larger, but that is a prediction. The test is an integration test (stateless tests have no Prometheus handler), so it needs the integration runner.

<details><summary>Self-review (lil red, preflight)</summary>

Preflight against the review prompt at master `521c4bcb7b94` (independent reviewer, 3 rounds, the limit):
- **Round 1: ⚠️ Request changes.** [Major] the per-series scan restarted at index 0 and walked the cache linearly, so a block with one long series and many short late ones cost series × points comparisons and could be slower than master. Fixed: the cache is used only for `/api/v1/query_range` and for blocks of more than one series, and the cursor jumps forward. Nits taken: description wording, a past-the-end test point (`raw` at 2002.5), a skewed benchmark case.
- **Round 2: no Blocker or Major;** ⚠️ only for the pending numbers. Taken: the changelog wording, series counts per row, and the recommended every-other-step case, with a galloping search (+1, +2, +4, … then bisect) so a series that skips steps doesn't pay a full binary search per point. The gallop was checked against `std::lower_bound` from the cursor on 200,000 random caches and series (same cursor, same hits; clang 20, `-Wsign-conversion` clean).
- **Round 3: ✅ Approve** at `4acc6e56f9f`, conditional on the build numbers showing a gain beyond the A/A spread. Nit taken (comment-only, `ff2ee0f3f036`): the invariant after the gallop. Not taken: a gtest of the search (it isn't a free function).
- Category: the reviewer suggests `Performance Improvement` (#115567's precedent); kept `Experimental Feature` for James to decide.

</details>
