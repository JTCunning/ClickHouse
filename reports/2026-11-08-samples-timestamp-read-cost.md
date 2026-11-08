# Why `samples.timestamp` dominates the read of a wide 30-day `rate()` + `sum by()` query

**Verdict:** the read path treats both subcolumns the same; the gap is one of bytes on disk, set by the data. Timestamps take about 98 times the bytes of the values because the capture's timestamps are jittered (about 1.45 bytes per sample) while its values are nearly constant (about 0.015 bytes per sample). The read-call count follows the bytes: one call per 128 KiB (`max_read_buffer_size_local_fs`). One change to measure next: local-disk prefetch plus a 1 MiB local read buffer, page cache dropped, on the capture machine.
**Task:** 2026-11-01-handoff-samplestimestamp-read-cost-dom, handoff https://github.com/JTCunning/ClickHouse/issues/12 · **Build:** 26.11.1.1642, git `7b5f2bf328f9` (master 2026-11-07) · **Date:** 2026-11-08
**A/B plan:** [ab-plans/samples-timestamp-read-cost.md](../ab-plans/samples-timestamp-read-cost.md)

## What happens (plain language)

Each row of the samples table holds one series for one hour: about 240 `(timestamp, value)` pairs. They are stored as two separate column files, one for the timestamps and one for the values. The query needs both for every selected row, and ClickHouse reads them the same way: the same granules, the same number of compressed blocks, the same amount of decompressed data. Only the compressed size differs. The read path reads each file in 128 KiB calls, so a file that is 98 times larger on disk takes about 98 times the read calls (46 times in the capture, see below) and almost all of the read wait.

How big each file is depends on the data, not on the read path. With the stored codecs (`Delta, T64, ZSTD(3)` for timestamps, `ALP, ZSTD(3)` for values), timestamps on an exact 15 s grid shrink to 0.04 bytes per sample, and ones jittered by ±500 ms take 1.6 bytes per sample. Counter values with a random mantissa, as real CPU-seconds counters have, take 5.9 bytes per sample. A linear counter takes 0.25. The capture's values took 0.015 bytes per sample, which means they are close to constant. On real counters the values file would likely be the larger one, not the timestamps.

## Numbers

### 1. The capture, per sample

From the handoff: 71,406,489,600 samples in 297,527,040 rows, so 240 samples per row. The query selected 18,497,761 rows, about 4.44 billion samples.

| subcolumn | bytes read | bytes / sample | read calls | bytes / call | wait share |
|---|---|---|---|---|---|
| `samples.timestamp` | 5.98 GiB | 1.45 | 48,954 | 128 KiB | 96.9% |
| `samples.value` | 61.23 MiB | 0.0145 | 1,064 | 59 KiB | 2.1% |

Uncompressed, both subcolumns are 8 bytes per sample (`SelectedBytes` 71.56 GB = 16.1 bytes per sample). The timestamp wait is 83,674 ms over 48,954 calls: 1.71 ms per 128 KiB call, about 77 MB/s per synchronous reader.

### 2. Bytes per sample by data shape (controlled repro)

The DDL is the handoff's samples table (version 11, 1 h buckets, `index_granularity = 256`, `index_granularity_bytes = 524288`, the same subcolumn codecs). Each dataset is 300 series of `container_cpu_usage_seconds_total`, 15 s scrape for 2 days: 11,520 samples per series, 3,456,000 samples, 14,400 rows, settled with `OPTIMIZE ... FINAL`. Sizes are the `.bin` files of the part.

| timestamps | values | timestamp B/sample | value B/sample | timestamp / value |
|---|---|---|---|---|
| exact 15 s grid | linear counter (`i * 0.25`) | 0.042 | 0.250 | 0.17 |
| exact 15 s grid | counter with random mantissa | 0.042 | 5.895 | 0.01 |
| ±5 ms jitter | linear counter | 0.662 | 0.251 | 2.6 |
| ±5 ms jitter | random-mantissa counter | 0.662 | 5.896 | 0.11 |
| ±500 ms jitter | linear counter | 1.577 | 0.253 | 6.2 |
| ±500 ms jitter | random-mantissa counter | 1.577 | 5.895 | 0.27 |
| uniform in the 15 s slot | linear counter | 1.974 | 0.250 | 7.9 |

The capture's 1.45 timestamp bytes per sample sits between the ±5 ms and ±500 ms rows. Its 0.0145 value bytes per sample is 17 times smaller than even a perfectly linear counter: the generator's values are nearly constant within a block.

### 3. The read path on a larger repro

The same DDL with 2,000 series × 2 days at 15 s, ±500 ms jitter, linear values: 23.04 million samples, 96,000 rows, 712 granules. On disk, `samples.timestamp` is 36.3 MB and `samples.value` is 5.8 MB. The machine has 2 vCPU and local SSD, `max_threads = 2`, the uncompressed cache is off, and each figure is the median of 3 to 6 runs.

| read | compressed bytes | blocks decompressed | read calls | KiB / call | median ms |
|---|---|---|---|---|---|
| `samples.timestamp` only | 36,336,419 | 723 | 279 | 127 | 199 |
| `samples.value` only | 5,807,774 | 723 | 46 | 123 | 80 |
| selector (`timeSeriesSelector`, both) | 42,666,504 | n/a | 356 | 117 | 384 warm / 421 cold |
| the handoff's PromQL, transpiled | 42,666,504 | n/a | 356 | 117 | 850 warm / 895 cold |

- Both subcolumns decompress the same 723 blocks. The call ratio (279 / 46 = 6.1) equals the byte ratio (6.3).
- Bytes read equal the bytes on disk (42.67 MB vs a 6.44 MB + 36.3 MB part), so no subcolumn is read twice.

Settings sweep on the selector, 5 runs each:

| setting | read calls | sync read wait, cold | median cold | median warm |
|---|---|---|---|---|
| defaults | 356 | 21.4 ms | 409 ms | 375 ms |
| `max_read_buffer_size_local_fs = 1048576` | 72 | 13.2 ms | 378 ms | 373 ms |
| `local_filesystem_read_prefetch = 1`, `allow_prefetched_read_pool_for_local_filesystem = 1` | 357 | 2.2 ms | 416 ms | 372 ms |
| both | 73 | 2.3 ms | 390 ms | 394 ms |
| `preferred_block_size_bytes` 0.25 / 4 / 16 MB, `max_read_buffer_size` 256 KiB / 4 MiB | 356 | n/a | n/a | 310–367 ms |

The call count changes only with `max_read_buffer_size_local_fs`. On this machine the read wait is about 5% of the query, so wall time barely moves. The capture is different: its synchronous read wait is 86 s across 16 threads, about 5.4 s per thread of a 25.4 s wall (about 21%).

## Why (cause)

Each of the four candidates in the handoff, checked:

1. **Call size (128 KiB) vs `index_granularity_bytes`.** These are unrelated. 128 KiB is the default of `max_read_buffer_size_local_fs` (`src/Core/Settings.cpp:449`, applied in `src/Interpreters/Context.cpp:10135`). `MergeTreeReaderStream::init` only shrinks it to the largest mark range (`src/Storages/MergeTree/MergeTreeReaderStream.cpp:66-76`). Calls = compressed bytes / 128 KiB, for every subcolumn. In the capture, 5.98 GiB / 48,954 = 128 KiB exactly. The value calls average less (59 KiB), probably because a read stops at the end of each task's mark range, and a whole task's values fit in less than one buffer (not verified).
2. **Mark-range coalescing.** Not the cause. All series of one metric share the first element of `id` (`sipHash64(metric_name)`), so they are contiguous in the `(id, bucket)` order. `Ranges: 1` means the selected 136,535 granules are one contiguous range, already a sequential scan.
3. **Zero prefetches.** That is the default for local disks: `local_filesystem_read_prefetch` and `allow_prefetched_read_pool_for_local_filesystem` are both `false` (`src/Core/Settings.cpp:8587`, `:8816`). An EBS volume is a local filesystem to ClickHouse, so every read is synchronous. With prefetch on, the repro's synchronous wait fell from 21.4 ms to 2.2 ms (it moves to `AsynchronousReadWaitMicroseconds`).
4. **Deserialization of the timestamp substream.** Not the cause. Both subcolumns decompress the same number of blocks (723 in the repro). `SerializationAggregateFunction::deserializeBinaryBulk` in the stacks is most likely the decode of the `SimpleAggregateFunction` column, which both subcolumns go through (not traced here). The wait in those stacks is the synchronous read underneath it.

So the timestamp file causes the call count and the read wait only because it holds 98 times the bytes, and that is a property of the dataset: jittered timestamps and nearly constant values.

Two things in the capture that this report does not explain, worth a look:

- **Filesystem reads are 25.60 GiB, but the samples-part files add up to about 6.05 GiB.** `DiskWriteElapsedMicroseconds` is 53,080 ms, so the query also wrote to disk. A spill of the aggregation (peak memory 25.8 GiB) being read back would explain both. Check `ExternalAggregationWritePart`, `ExternalAggregationCompressedBytes` and `ExternalProcessingFilesTotal` in the same `query_log` row. If the spill is most of the reads, it matters more than the timestamp file.
- **`rate(...[5m])` at a 15-minute step uses about a third of the samples it reads.** Each evaluation needs only the 5 minutes before each step, but a 1-hour bucket row is read whole. This comes from the bucket layout, not the read path, so there is no simple fix. It is a third of the bytes of every subcolumn.

## Proposed fix

Report only. The change to measure next is a settings-only experiment on the capture machine, with page cache dropped before each run:

- A: defaults.
- B: `local_filesystem_read_prefetch = 1`, `allow_prefetched_read_pool_for_local_filesystem = 1`, `max_read_buffer_size_local_fs = 1048576`.
- **Prediction:** wall time falls by up to the synchronous-wait share per thread (about 21%, 5.4 s of 25.4 s). It is bounded by the volume's provisioned throughput: physical reads / throughput is the floor, and the capture already spent 9,984 ms at the EBS throughput limit. Read calls fall by up to 8 times (48,954 to about 6,100 for timestamps; 5 times in the repro).
- **If B wins:** the code change is small. `StoragePrometheusQuery::applySettingsForPrometheusQuery` already sets settings for PromQL queries, and it can set these two for the samples read. That is a branch for after the A/B, not before.

Before that, check whether the capture's data shape is the one to optimise. Run the two queries in "How to reproduce" on the capture table. If production timestamps are near-grid (Prometheus aligns scrape timestamps within a small tolerance by default), then the timestamp file is small, the values file dominates, and the order of this work changes.

Effort: S (settings), then S for the helper change if the A/B passes.

## Upstream overlap

- `git log origin/master` since 2026-10-09 on `src/Storages/StorageTimeSeriesSelector.cpp`, `src/Storages/TimeSeries/` and `AggregateFunctionTimeSeriesGroupArray.cpp`: nothing on read buffers or prefetch. #128764 (merged 2026-11-03) disables the adaptive aggregator for PromQL queries. It may change the capture's memory and any spill, but not the samples read. The ALP codec went GA in c66bc278515e (2026-11-06), with no format change.
- Open `comp-promql` PRs searched for timestamp, prefetch and subcolumn codec: only #121216 (bucketed samples, a native PromQL executor) and #122051 (native histograms), neither on this read path.
- James's fork: no branch on read settings.

## How to reproduce

Scripts: [`bench/samples-timestamp-read-cost/`](../bench/samples-timestamp-read-cost/) (`ddl.sql.tmpl`, `load.sh`, `q_sel.sql`, `q_promql.sql`, `tabulate.py`).

```bash
./load.sh j500_lin '(cityHash64(s, i, 1) % 1001) - 500' 'i * 0.25'      # one dataset; see section 2 for the others
# per-subcolumn read, on a server with query_log:
SELECT sum(length(samples.timestamp)) FROM lab.`.inner_id.samples.<uuid>` SETTINGS max_threads = 1;
SELECT sum(length(samples.value))     FROM lab.`.inner_id.samples.<uuid>` SETTINGS max_threads = 1;
SELECT ProfileEvents['ReadCompressedBytes'], ProfileEvents['CompressedReadBufferBlocks'],
       ProfileEvents['ThreadPoolReaderPageCacheHit'] + ProfileEvents['ThreadPoolReaderPageCacheMiss'] AS calls
FROM system.query_log WHERE type = 'QueryFinish' ORDER BY event_time_microseconds DESC LIMIT 2;
```

On the capture table, the two checks of the data shape:

```sql
-- compressed bytes per subcolumn
SELECT arrayZip(subcolumns.names, subcolumns.data_compressed_bytes)
FROM system.parts_columns WHERE active AND table = '.inner_id.samples.<uuid>' AND column = 'samples';

-- scrape-interval jitter: quantiles of consecutive timestamp deltas, in ms
SELECT quantiles(0.01, 0.5, 0.99)(d)
FROM (SELECT arrayJoin(arrayDifference(arrayMap(x -> toUnixTimestamp64Milli(x.1), samples))) AS d
      FROM `.inner_id.samples.<uuid>` WHERE bucket < '2024-01-01 03:00:00')
WHERE d > 0;
```

The repro's ±500 ms dataset gives `[14131.91, 14996, 15867]`. An exact grid gives `[15000, 15000, 15000]`.
