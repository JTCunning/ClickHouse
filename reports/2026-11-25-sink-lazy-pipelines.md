# The remote-write handler builds four INSERTs per request that async insert never uses

**Verdict:** worth fixing; branch `lilred-perf-sink-lazy-pipelines` creates the INSERTs into the target tables with the first block of rows (James's prototype, 14 lines), and checks the INSERT grants on the target tables when the sink is created, as master did when it built them (the prototype had moved that check into the flush). A silent data loss on master that the branch leaves as it is goes to an upstream issue draft (below). Idea 2 (skip the outer INSERT on the handler too) isn't simple and is worth about 0.7% of server CPU: not done.
**Task:** `2026-11-25-handoff-remote-write-handler-builds-the`, handoff [#27](https://github.com/JTCunning/ClickHouse/issues/27) · **Build:** master 26.12.1.112 `521c4bcb7b94` (the lab binary; the sink and handler code is unchanged to `2176a0e2a33`) · **Date:** 2026-11-25
**A/B plan:** [ab-plans/sink-lazy-pipelines.md](../ab-plans/sink-lazy-pipelines.md) · **PR text:** [prs/sink-lazy-pipelines.md](../prs/sink-lazy-pipelines.md)

## What happens (plain language)

A `TimeSeries` table writes into five inner tables (tags, time ranges, samples, recent samples, metric families). Its sink prepares one INSERT into each of them as soon as it is created, before any row arrives. With async insert (the default), the remote-write handler creates the sink only to hand the request's block to the async insert queue; it then throws the prepared INSERTs away, and the queue's flush prepares its own. So each request pays for four INSERTs into tables that never get a row from it. The cost is per request, not per sample, so a larger tags cache doesn't reduce it.

## Numbers

This host: 2 vCPU, 4 GiB, one master server, stock `ENGINE = TimeSeries`, async insert on. Raw output: `bench/sink-lazy-pipelines/smoke-2026-11-25.txt`.

Fixed cost of an INSERT that gets no rows (`bench/sink-lazy-pipelines/empty_insert.sh`, 40 interleaved rounds, server CPU from `system.query_log`):

| INSERT (each `SELECT ... FROM numbers(10) WHERE number > 100`) | inner INSERTs built on master | CPU median, µs | min–max |
|---|---:|---:|---:|
| `INSERT INTO sink_lazy.ts (metric_name, tags, samples) SELECT ...` | 4 | 2,557 | 2,232–3,503 |
| `INSERT INTO sink_lazy.ts (metric_family, type, unit, help) SELECT ...` | 1 | 1,437 | 1,332–1,634 |
| `INSERT INTO sink_lazy.mt (metric_name, tags, samples) SELECT ...` (plain MergeTree, control) | 0 | 1,452 | 1,279–1,738 |

So the four inner INSERTs cost about 1.1 ms of CPU per INSERT here, 43% of an empty INSERT into the TimeSeries table.

Remote write (`bench/sink-lazy-pipelines/handler_cpu.sh`: 70 requests of 2,000 series each out of 20,000, one sample per series, the first 10 skipped):

| part of the request | queries | CPU median, µs | µs per sample | min–max, µs |
|---|---:|---:|---:|---:|
| handler (request thread: protobuf, the block, and the unused INSERTs) | 65 | 4,905 | 2.45 | 4,457–5,328 |
| async insert flush (the real write into the five tables) | 65 | 14,555 | 7.28 | 13,708–23,801 |

The same mechanism in the new stateless test's run on master: an async INSERT of 2 rows sent as blocks over the native protocol (which builds and discards the sink as the remote-write handler does) took 1,964 µs of CPU, and the same kind of INSERT with its data inlined in the query (no sink on the request thread) 359 µs.

Prediction for the branch on this host: the handler loses about 1.1 ms per request (4.9 → about 3.8 ms), about 6% of the request's handler plus flush CPU, merges not counted. James's at-scale run of the identical prototype measured 1.52 → 0.14 µs per sample in the handler's unused pipelines and −6.6% server CPU per sample at 1M series (−8.8% with a 1 GiB tags cache); his per-request cost there was 3.1 ms at 1M series and 4.4 ms at 5M, so it grows with the table, which this small host doesn't reproduce.

## Why (cause)

- `insertBlock` in `src/Storages/TimeSeries/PrometheusRemoteWriteProtocol.cpp:241` runs `executeQuery` for the INSERT (line 260). That runs `InterpreterInsertQuery::execute`, which calls `StorageTimeSeries::write` (`src/Storages/StorageTimeSeries.cpp:907`), which constructs a `TimeSeriesSink`.
- The constructor (`src/Storages/TimeSeries/TimeSeriesSink.cpp:830`) calls `initTagsAndSamplesPipelines` (line 882) and, with metric family columns, `initMetricFamiliesPipeline` (line 1227). Each target goes through `createTargetPipeline` (line 786): a full `InterpreterInsertQuery::execute` on the inner table, a started `PushingPipelineExecutor`, and converting actions.
- With async insert, `insertBlock` gives the block to `pushQueryWithBlock` (line 266) and calls `io.resetPipeline(/*cancel=*/ true)` (line 270): the four pipelines are destroyed unused. `TCPHandler::processAsyncInsertQuery` (`src/Server/TCPHandler.cpp:1591`, reset at line 1714) does the same for an async INSERT sent as blocks.
- `onFinish` (line 1292) already checks each pipeline pointer, so creating the pipelines on the first chunk with rows needs no other change.

## Proposed fix

Branch `lilred-perf-sink-lazy-pipelines` @ `7b6fce7643e` on master `2176a0e2a33`, two commits. Effort S.
1. The constructor only decides which target tables the INSERT writes; a new `initPipelines()` creates them on the first chunk with rows (James's prototype from #27, unchanged).
2. The grant check (added after the review-bot preflight, rounds 1 to 3). Building each inner INSERT checked the user's INSERT grant on its inner table. With commit 1 alone, that check moved to the first block of rows, and for an async insert into the flush: with `wait_for_async_insert = 0` a user without the grant got success and lost the data. Now the sink's constructor makes the same checks, on exactly the inner tables this INSERT writes and with the columns the sink writes there, so grants and missing inner tables behave as on master, at the same point. The constructor runs on the handler before the data is queued (remote write, native-protocol async INSERT) and at the start of every synchronous INSERT. Round 2 tried ClickHouse's `checkInsertIsAllowed` hook instead; the review found it stricter than master (the hook can't see the INSERT's columns, so it had to check every inner table) and still not complete, so round 3 dropped it.

Tests: `05875_time_series_sink_lazy_target_pipelines` (synchronous INSERT in 3 blocks, async INSERT as native-protocol blocks, inlined async INSERT, metric families alone, INSERTs with no rows; a materialized view on the tags table counts the INSERTs built into it: master builds one for the INSERT with no rows and for the native-protocol handler, the branch doesn't) and `05876_time_series_insert_target_access` (synchronous INSERT with and without rows, native-protocol async INSERT with and without waiting, and an INSERT of only the metric family columns, with no grant, a grant on the tags and metric families tables, and a full grant; one series per case; the reference is master's output). Both were run on a master server here: 05875 differs from master in exactly its 2 `InsertQueriesWithSubqueries` lines, 05876 equals master (`bench/sink-lazy-pipelines/05875-on-master-2026-11-25.txt`, `05876-on-master-2026-11-25.txt`).

**A defect on master found on the way:** an async INSERT into a TimeSeries table with its data inlined in the query (`INSERT ... VALUES` or an HTTP body with `async_insert = 1`), sent by a user who has INSERT on the TimeSeries table but not on its inner tables, returns success with `wait_for_async_insert = 0`, and the flush then drops the data (05876 on master: `asynchronous, inlined, wait 0: OK`, then 0 rows). With `wait_for_async_insert = 1` the user gets `ACCESS_DENIED` from the flush. Cause (from reading the code): the async queue calls `IStorage::checkInsertIsAllowed` before queueing an inlined INSERT, and `StorageTimeSeries` doesn't implement it, so its inner tables' grants are checked only in the flush. The branch doesn't change this path (it never builds the sink before queueing). An upstream issue draft for James is in a comment on #27; upstream issue 120717 is the same defect class for DeltaLake.

What still moves to the first block of rows (to the flush for an async insert, as for an inlined async insert into any table): the checks of materialized views on the inner tables, an inner table's `parts_to_delay_insert` delay and too-many-parts error, and its share lock. For remote write that means the insert delay under merge pressure happens once per flush instead of once per request; the sender still waits for the flush, so it still slows down. No stateless or integration test checked INSERT grants on inner tables before 05876 (searched: `rg` over the TimeSeries tests for `ACCESS_DENIED` and `GRANT`).

### Idea 2: skip `InterpreterInsertQuery::execute` on the handler

Not simple. What `executeQuery` does for the request that must stay: the process list entry (the handler updates its progress), the `query_log` entry, the quota accounting (`QUERY_INSERTS`, `QUERIES`, `ERRORS`) and the 100-continue callback. The access check would stay anyway: `AsynchronousInsertQueue::preprocessInsertQuery` checks `INSERT` on the table and `checkInsertIsAllowed`. `executeQuery` already has an async branch that does all of that without building the pipeline, but only for data inlined in the query text (`hasInlinedData()`, `pushQueryWithInlinedData`); a block has no way in. The complete fix is a block-carrying variant of that branch in `executeQuery`, a change to generic query execution, and `TCPHandler` couldn't use it as is: it needs the pipeline's header to tell the client the insert structure. The gain is the 0.14 µs per sample left on the handler in James's run, about 0.7% of server CPU at 1M series. Not worth a generic change now.

### Idea 3: other callers that get the same saving

- An async INSERT into a TimeSeries table sent as blocks over the native protocol (`clickhouse-client` with `async_insert = 1` and data on stdin): the same build-and-discard on every INSERT (1,964 µs vs 359 µs above).
- An `INSERT ... SELECT` with an empty result: about 1.1 ms per INSERT here (2,557 µs predicted to fall to about the control's 1,450).
- Not more: `StorageTimeSeries` doesn't support parallel insert, so `max_insert_threads` doesn't multiply sinks, and the async insert flush always has rows.

## Upstream overlap

- Two open upstream PRs also create some target pipelines on demand, as part of larger changes: James's 123144 ("Parallelize flushing samples and recentsamples tables on insert", last pushed 2026-11-03) creates the tags and time ranges pipelines on demand and the samples ones per block; valerypetrov's 121062 (working-set cache in `TimeSeriesSink`, last pushed 2026-11-20) creates and finishes the tags and time ranges pipelines per block. Neither removes the constructor's metric families pipeline, and neither targets this cost. Whichever lands second has a textual conflict and keeps the on-demand creation; this one is the smallest to land first.
- 119734 (remote write v2, open draft) touches the handler file, not the `executeQuery` call. 125215 (perf test for INSERT into a TimeSeries table, open) measures synchronous INSERTs only.
- Recent master commits on the touched files (30 days): none change the sink's constructor or `consume`.

## How to reproduce

```bash
# server with the remote-write handler (TCP 19101, HTTP 20101, remote write 21101/write)
DATA=/root/nerve-workspace/tools/clickhouse/data/sink-lazy-pipelines research/bench/remote-write-arena/server.sh start A 19101 </dev/null
research/bench/sink-lazy-pipelines/empty_insert.sh 19101 40
TS0=$(( ($(date +%s) - 3600) / 3600 * 3600 * 1000 )) research/bench/sink-lazy-pipelines/handler_cpu.sh 19101 21101 70
research/bench/sink-lazy-pipelines/contents.sh 19101
DATA=/root/nerve-workspace/tools/clickhouse/data/sink-lazy-pipelines research/bench/remote-write-arena/server.sh stop A
```
