# Label registration (`timeSeriesStoreTags`) costs ~1 µs and ~590 bytes per matched series

**Verdict:** worth fixing, but not with a small patch. The 2× goal needs one storage redesign of the per-query tags collector (flat, arena-backed label storage). The small steps (pre-sizing, hashing outside the lock, skipping the sort) are together worth only about 10–12 %. Report only; no branch.
**Task:** `2026-10-31-handoff-cut-per-row-cpu-of-contexttimes`, handoff https://github.com/JTCunning/ClickHouse/issues/3 · **Build:** 26.11.1.974, `a7f713612c8` (code read at upstream master `802411f22ae`, no changes to these files in 60 days) · **Date:** 2026-10-31

## What happens (plain language)

Every PromQL selector runs a lookup over the tags table, and for each series it matches it stores that series' labels in a per-query dictionary. The dictionary maps series id → label-set number → labels. It keeps each label set as its own small heap object: one list allocation and one reference-counted wrapper per series, plus a node in a separate lookup table. That's fine at a few thousand series. At 100k–400k series the query spends about 5× longer storing labels than reading them from disk, and it holds about 590 bytes of heap per series. About 40 % of the cost comes at query end, when those hundreds of thousands of small objects are freed one by one and the allocator hands the pages back to the kernel.

## Numbers

Dataset: 400,000 series over 200 metrics (`metric_0` = 100,000 series; the rest about 1,500 each), 6 labels per series plus `__name__`, the same label generator as the handoff. Tags table merged to 1 part, `max_threads=2`, server on 127.0.0.1 (2 vCPU). Warm median of 5 runs, after one cold run.

| case | median | min–max | CPU (of which kernel) | read_rows | peak memory |
|---|---|---|---|---|---|
| read `id, tags, metric_name`: all 400k series | 76 ms | 75–86 | 74 ms (41) | 400,000 | 4.4 MiB |
| same + `timeSeriesStoreTags(...)` | **507 ms** | 493–521 | 483 ms (151) | 400,000 | **235 MiB** |
| read: `metric_0` only (100k) | 23 ms | 22–24 | 22 ms (7) | 106,496 | 4.4 MiB |
| same + `timeSeriesStoreTags(...)` | **105 ms** | 102–140 | 104 ms (38) | 106,496 | **60 MiB** |

- Storing labels adds **~430 ms and ~230 MiB for 400k series**, which is ~1.07 µs and ~590 B per series. It adds about 82 ms and 55 MiB for `metric_0`.
- Kernel time rises from 41 to 151 ms: page faults and page returns caused by the memory churn.
- Adding `timeSeriesIdToGroup` on top changes nothing (489 ms), so the cost is all in storing.
- At 1 vs 2 threads the numbers are the same here, because a 1-part table reads in a single stream. Lock contention is therefore **not** visible in these numbers. See step 1.

CPU samples (1 ms, 6 runs, all-series case), grouped:

| where | share | what it is |
|---|---|---|
| collector destructor at query end | **39 %** | freeing 400k label lists; 2/3 of it is the allocator returning pages to the OS |
| group lookup-table insert (`tryAddGroupUnlocked`) | 19 % | `std::unordered_map` node allocation, bucket walks, rehash growth |
| building the per-row label lists (`extractTagNamesAndValuesFromArguments`) | 17 % | copying 7 name/value pairs into a new list per row, plus the reference-counted wrapper |
| `storeTagsTyped` body + id-map growth | 9 % | id → group hash map, its resizes, the `found_groups` fill |
| reading the columns | 8 % | the baseline |
| sorting labels per row | 2 % | |

This matches the handoff's 16-thread frame table: the destructor, the shared_ptr frees and jemalloc flushes there come to about 28 %, and the map emplace to about 37 %.

## Why (cause)

The collector stores a label set as `shared_ptr<vector<pair<String, String>>>`, and the public interface (`getTagsByGroup`, `getTagsByID`, the `copyTag`/`removeTags` transforms) hands those pointers to other `timeSeries*` functions. So every new series costs:

1. a vector buffer (7 × 48 B, the 384 B size class), plus a reference-count block, built per row in `makeTagsPtrVector`
2. a node in `std::unordered_map<TagsKey, Group>` (48 B) and its bucket slot; the map grows by rehashing, touching every node again
3. an id-map slot and a `groups` and `sampling_keys` entry

At query end all of it is freed object by object, inside the query's own thread, so it counts in wall time.

Pointers (upstream master `802411f22ae`):
- `src/Interpreters/ContextTimeSeriesTagsCollector.cpp`: `TagsKey::TagsKey` 903, destructor 930, `tryAddGroupUnlocked` 991, `storeTagsTyped` 1203 (the exclusive-lock loop is at 1243–1265)
- `src/Interpreters/ContextTimeSeriesTagsCollector.h`: `groups`, `groups_for_tags` (std::unordered_map), the typed id maps
- `src/Functions/TimeSeries/TimeSeriesTagsFunctionHelpers.cpp`: `sortTagsAndRemoveDuplicates` 217, `makeTagsPtrVector` 260, `extractTagNamesAndValuesFromArguments` 337. It already reserves each row's list exactly, so there are no growth reallocations there.

## Proposed fix: expected saving per step (the handoff's four ideas)

The estimates combine the sample shares above with a standalone C++ model of the same data structures (`bench/tags-collector/`, built with g++ -O2 and glibc malloc, 400k series in 64k-row blocks, median of 5). They are estimates, not measurements of a ClickHouse build.

| step | change | expected saving on the ~430 ms label cost | effort | safe as a "simple" PR? |
|---|---|---|---|---|
| 1 | Build `TagsKey` (the hash) before the exclusive lock; pre-size `groups_for_tags`, `groups`, `sampling_keys` and the typed id map per batch | **~10 %** (~40 ms / 400k). Model: group-map insert −14 %, teardown −20 %. Hashing is under 1 % of CPU, so moving it out of the lock only helps concurrent tag-reading streams (multi-part tables at 16 threads), as wall time, not CPU | S | Yes, with one catch: `std::unordered_map::reserve(size + n)` and `vector::reserve` per batch defeat geometric growth and turn growth quadratic over many small batches. The reserve must round up (e.g. to ≥2× current size), or it draws a review Major |
| 2 | Skip `std::sort` when keys arrive sorted | **≤2 %** | S | Yes, but the gain is too small on its own to justify a PR |
| 3 | Flat storage: one arena per collector for label bytes, groups as (offset, count) ranges, an open-addressing map from group hash to group number | **~45–55 %**: teardown goes from 39 % to ~1 %, building the per-row lists halves, peak memory roughly halves (~590 → ~300 B/series). Model: 525 → ~290 ms, **~1.8×** | M–L | No. It touches the type every `timeSeries*` function receives. Those functions keep compiling only if `getTagsByGroup`/`getTagsByID` build `shared_ptr` lists lazily for the groups they return, as open PR #115223 already does for the transforms |
| 4 | Hash the raw Map bytes and skip building the list when the group already exists | **~0 %** for wide selectors: every series is its own group (unique `pod`, `instance`), so nothing repeats. It only helps repeated selectors, which are handoff #5 | S after 3 | Not on its own |

**Recommendation:** steps 1+3 together should reach the handoff's 2× target. As one PR that is too wide for a clean first review pass, so ship it as two:
- **(a)** Step 1 alone: small, measurable, no interface change.
- **(b)** Step 3: the redesign, written after (or rebased on) #115223, since both change how groups are handed out. It needs a build to prove itself. Its A/B must include `tests/performance/promql_tags_transform*.xml` (which #115223 adds) and the `metric_0` / `sum by (job, cluster)` shapes.

I didn't push a branch for (a). At ~10 % of label cost (roughly 10 ms of CPU for the 100k-series `metric_0`), it doesn't meet the "meaningful" bar without a build to show the real number. It's the first thing to do once builds are possible.

## Upstream overlap

- **#115223** (fallintoplace, open, last updated 2026-10-05), "Materialize tags only for unique groups": the transform path (`copyTag`, `removeTags`, … deduplicating groups before fetching tags). Not the store path, so no overlap with steps 1–2. It's the natural base for step 3.
- **#126492** (groeneai, open): a context-lifetime fix in `timeSeriesStoreTags.cpp` / `timeSeriesTagsToGroup.cpp`. Not perf; at most a trivial textual conflict.
- **#118801** (CTE reuse for selectors) and handoff #5 (one lookup per distinct selector): they cut how often the store runs, not its per-row cost. Complementary.
- No commits to the collector or the helpers on master in the last 60 days. James's fork branches checked by name: `jtcunning/prom-labels-direct-tags`, `jtcunning/timeseries-tags-cache-prewarm` and `jtcunning/tags-dedup-cache-expiration-jitter` are about ingest and the label read path, not the collector store (by name only, not read in full).

## How to reproduce

```bash
# server on 127.0.0.1:19000 with trace_log/query_log, enable_time_series_table=1, max_memory_usage=2.5e9
clickhouse client --port 19000 -q "CREATE TABLE ts ENGINE = TimeSeries"
# load: the handoff's INSERT with FROM numbers(400000) and range(60) samples at 60 s; then OPTIMIZE ... FINAL on every .inner_id.% table
TAGS=$(clickhouse client --port 19000 -q "SELECT name FROM system.tables WHERE name LIKE '.inner_id.tags.%'")
# base
clickhouse client --port 19000 -q "SELECT count() FROM (SELECT id, tags, metric_name FROM \`$TAGS\`) WHERE NOT ignore(id, tags, metric_name) SETTINGS max_threads=2"
# store
clickhouse client --port 19000 -q "SELECT count() FROM (SELECT timeSeriesStoreTags(id, tags, '__name__', metric_name) AS x FROM \`$TAGS\`) WHERE NOT ignore(x) SETTINGS max_threads=2, query_profiler_cpu_time_period_ns=1000000, query_profiler_real_time_period_ns=0"
# add WHERE metric_name = 'metric_0' for the 100k case; read query_log / trace_log by log_comment
# model: g++ -O2 -std=c++20 bench/tags-collector/mb.cpp -o mb && ./mb 0 && ./mb 2   # 0 = as master, 2 = pre-sized group map
#        g++ -O2 -std=c++20 bench/tags-collector/flat.cpp -o flat && ./flat           # step-3 layout estimate
```
