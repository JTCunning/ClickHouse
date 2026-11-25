# [unbuilt] TimeSeries: build the inner INSERTs of the sink with its first block of rows

**Branch:** [`lilred-perf-sink-lazy-pipelines`](https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred-perf-sink-lazy-pipelines) @ `7b6fce7643e` (base master `2176a0e2a33`) · **Status:** ready to build; the table below is James's run of commit 1 as a prototype on master `a1ba05231040` (issue #27); the outbox A/B re-measures the branch · **Report:** [reports/2026-11-25-sink-lazy-pipelines.md](../reports/2026-11-25-sink-lazy-pipelines.md) · **A/B plan:** [ab-plans/sink-lazy-pipelines.md](../ab-plans/sink-lazy-pipelines.md)

<!-- upstream:start (the PR body is everything between these markers; the title is the heading above) -->
Related: (lil red: open the issue drafted on fork #27 and put its link here, or delete this line)
Related: https://github.com/ClickHouse/ClickHouse/pull/123144
Related: https://github.com/ClickHouse/ClickHouse/pull/121062

### Changelog category (leave one):
- Experimental Feature

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
Prometheus remote write into a `TimeSeries` table with asynchronous inserts (the default) no longer builds and discards the INSERTs into the inner tables on every request, which lowers the server CPU per ingested sample by 6.6% at 1M active series.

### Description

The constructor of `TimeSeriesSink` builds one INSERT pipeline per target table: tags, time ranges, samples, recent samples, and metric families when the INSERT has those columns. With async insert, the remote-write handler executes the INSERT, which constructs the sink, then queues its block with `pushQueryWithBlock` and cancels the pipeline; the flush builds a sink of its own. So every request pays for four INSERT pipelines that never get a row: 1.5 µs of server CPU per sample at 2,000 samples per request, 7% of the server's CPU at 1M series. An async INSERT sent as blocks over the native protocol takes the same path in `TCPHandler`, and an `INSERT ... SELECT` with an empty result pays the same fixed cost.

Now the sink creates the target pipelines in `consume`, with the first chunk that has rows. `onFinish` already skips a pipeline that does not exist, and the deduplication caches are marked only after their pipeline finishes, so an INSERT with rows writes the same rows in the same order.

Building a target pipeline checked the `INSERT` privilege on its table. The constructor now makes the same checks, on the target tables this INSERT writes and with the columns the sink writes there (`getTargetColumnNames`; a `chassert` in `createTargetPipeline` keeps it equal to the pipeline's header). A missing grant or target table fails where it did before, also for remote write and a native-protocol async INSERT, before the data is queued. (An async INSERT with its data inlined in the query never builds the sink before queueing, with or without this PR: `StorageTimeSeries` doesn't implement `checkInsertIsAllowed`.) What moves to the first block of rows, and for an async insert to the flush (as for an inlined async insert into any table): the checks of materialized views on the target tables, the targets' insert delay and too-many-parts error, and their share locks. Remote write waits for the flush, so the sender still gets those errors; with `wait_for_async_insert = 0` they show only in `system.asynchronous_insert_log`. No setting, no format change. The handler still runs `InterpreterInsertQuery::execute` for the outer table (0.14 µs per sample).

RelWithDebInfo, AMD EPYC 9R14, server pinned to 8 cores, master `a1ba05231040`. 1M series from 4 Prometheus 3.16.0 agents at a 15 s scrape (66.7k samples/s), 10 min warm-up and a 20 min window. Base and PR ran at the same time on separate core sets, and a second job swapped the core sets. Mann-Whitney, exact, two-sided:

| table | metric | master | this PR | change |
|---|---|---:|---:|---:|
| stock | server CPU µs per sample, median of 6 | 21.24 | 19.83 | -6.6% (p = 0.002) |
| stock | handler CPU in the unused pipelines, µs per sample | 1.52 | 0.14 | -91% (p = 0.002) |
| stock | request duration p50, ms | 249 | 251 | +0.8% (p = 0.22) |
| 1 GiB tags cache | server CPU µs per sample, median of 3 | 15.75 | 14.37 | -8.8% (p = 0.10) |

All 6 PR cells of the stock table are below all 6 master cells. No failed or retried samples on either side. 12 dashboard and alert queries are unchanged (geometric mean 1.00x).

Validation: 60 deterministic remote-write requests (2,000 series each, from 20,000) give equal `count()` and `groupBitXor(cityHash64(*))` with `FINAL` on all five inner tables, with and without async insert. `05875_time_series_sink_lazy_target_pipelines` writes through a synchronous INSERT in 3 blocks, an async insert sent as blocks over the native protocol, an inlined async insert, the metric family columns alone, and INSERTs without rows; a materialized view on the tags table counts the INSERTs built into it (`InsertQueriesWithSubqueries`), which master builds for the INSERT without rows and for the native-protocol handler (2 lines differ on master). `05876_time_series_insert_target_access` runs the synchronous INSERT with and without rows, the native-protocol async INSERT with and without waiting (the same handler-side construction as remote write), and an INSERT of only the metric family columns, with no grant, a grant on the tags and metric families tables, a full grant, and column grants on the tags table without and with `id`; one series per case. Its reference is master's output. Checking fewer tables or columns in the constructor turns its `wait 0` lines to OK, and checking more turns the metric-family and column-grant lines to `ACCESS_DENIED`.

Overlap: #123144 and #121062 also create some target pipelines on demand, as part of other changes, and are open. Whichever lands second keeps the first-chunk creation.
<!-- upstream:end -->

**Note for James (not part of the PR):**
- The numbers in the table are from your prototype run on `a1ba05231040`. Commit 1 is your prototype's 14 lines unchanged. Commit 2 adds the grant checks in the sink's constructor (a few `checkAccess` calls per INSERT, against the 1.1 ms of pipelines removed), so the numbers should hold; the outbox A/B replaces them with the branch run (A/B plan, cases T1–T2).
- Why commit 2: preflight round 1 found that the prototype moved the inner tables' INSERT grant checks into the async flush, so a native-protocol async INSERT with `wait_for_async_insert = 0` and no grant returned success and the flush dropped the data. The constructor now checks the same tables with the same columns as master's pipelines did, so grants behave exactly as on master (05876's reference is master's output).
- Master has a gap of its own that this branch leaves as it is: an async INSERT with its data inlined in the query never builds the sink before queueing, so with `wait_for_async_insert = 0` a user with INSERT on the TimeSeries table but not on its inner tables gets success and the flush drops the rows. I drafted an upstream issue for it in a comment on #27. Open it and put its link in the `Related:` placeholder, or delete that line (the outbox decision asks the same).
- Upstream PR 123144 is yours (open, last pushed 2026-11-03): it creates the tags, time ranges, samples and recent samples pipelines on demand too, as part of the parallel flush. If you'd rather fold this change into 123144, drop this item. My recommendation: submit this small one first; 123144's rebase then keeps its own creation points and drops nothing.
- Idea 2 (skip `InterpreterInsertQuery::execute` on the handler): not simple, see the report. Not in this branch.

<details><summary>Self-review (lil red, preflight)</summary>

Round 1 (independent reviewer on `931efd0dbd0`, prototype only): ⚠️ Request changes. [Major] the inner tables' INSERT grant checks move to the first block / the async flush, breaking the `IStorage::checkInsertIsAllowed` contract on the remote-write and native-protocol async paths (native async with `wait_for_async_insert = 0`: success, then the flush drops the data); [Major] no test of the moved check; [Major, pre-existing] the tags target's metadata is read before its INSERT grant is checked. Nits: `initPipelines` not retryable (comment added), the log-level override in 05875 (removed). Tests: 05875 didn't fail on master (fixed: `InsertQueriesWithSubqueries` via an MV on the tags table).

Round 2 (on `93eb3582056`, with `StorageTimeSeries::checkInsertIsAllowed` checking all targets): ❌ Block. [Blocker] `getTargetColumnNames` binds `*storage.getStorageSettings()` to a temporary `shared_ptr`: use-after-free under a concurrent `ALTER ... MODIFY SETTING`. [Major] the hook doesn't repeat the other checks building the inner INSERTs did before queueing (each target's own `checkInsertIsAllowed`, the privileges of materialized views on the targets). [Major] checking every target is stricter than master (a metric-families-only INSERT, a remote-write request without metadata) and `getTargetTableID` makes a missing unwritten target fail up front, so the description's "missing target table moves" is wrong. Nits: run `checkTimeSeriesVersionIsWritable` first. Tests: 05876's `result()` treats any non-ACCESS_DENIED output as OK; one series per case; add partial-column and MV-privilege cases.

Round 3 (on `3883bf3b506`, the per-target check in the sink constructor): ❌ Block, on two defects outside the diff; none in it. The reviewer walked a synchronous INSERT in 3 blocks, INSERTs without rows, metric families alone, the native-protocol async INSERT with wait 0 and 1, remote write, the flush, an inlined async INSERT, TimeSeries as a materialized-view target, `timeout_overflow_mode = 'break'` and an exception inside `initPipelines`, and found `getTargetColumnNames` equal to every header builder, no retry hazard (`ExceptionKeepingTransform` drops input after an exception) and every pipeline null-checked in `onFinish`. [Major, pre-existing] the inlined async INSERT with `wait_for_async_insert = 0` returns success without the inner tables' grants and the flush drops the rows: description narrowed, upstream issue drafted on #27, `Related:` placeholder. One more pre-existing finding is in my private note to James. Tests: column-level grants added to 05876 (stages s4/s5; reference rerun on master); a remote-write negative integration case not added (the native-protocol async case runs the same handler-side construction, and integration tests can't run here). User lens: with `wait_for_async_insert = 0` the moved checks report only in `system.asynchronous_insert_log` (now in the description). Final head `7b6fce7643e` adds only the 05876 stages to `3883bf3b506`. No round 4 (the cap is 3).

</details>
