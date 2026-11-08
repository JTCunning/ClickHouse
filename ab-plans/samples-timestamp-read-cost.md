# A/B plan: local prefetch and a 1 MiB local read buffer for the wide `rate()` + `sum by()` read
**Experiment:** ../reports/2026-11-08-samples-timestamp-read-cost.md · **Branch:** none yet: tests a settings-only change before any code (handoff #12) · **Task:** 2026-11-01-handoff-samplestimestamp-read-cost-dom
**Claim:** on the handoff's 30-day `sum by (namespace) (rate(container_cpu_usage_seconds_total{job="cadvisor", image!=""}[5m]))` with the page cache dropped, enabling local prefetch with a 1 MiB local read buffer cuts the synchronous read wait by more than 80% and the wall time by 10–21%. The floor is the volume's throughput (physical bytes ÷ provisioned MB/s).
**Evidence so far:** measured on 2 vCPU with local SSD, 23 M samples. Prefetch moved the sync wait from 21.4 to 2.2 ms (cold), and the 1 MiB buffer cut read calls from 356 to 72. Wall time moved less than 8%, because the read is only about 5% of the query there. The EBS case, where the read wait is the dominant event, is unproven.

## Builds
- One build, current master. The experiment changes query settings only, so A and B run on the same binary and the same server. Record the version and git hash.

## Machine
- The capture's machine class (16 vCPU, `m7g.4xlarge`), with the same EBS volume type and provisioned throughput as the capture. The claim is about synchronous reads from a throughput-limited network volume. Record the volume type, IOPS and MB/s. Nothing else running.

## Data
- The capture table (413,232 series, 30 days at 15 s, 1 part after `OPTIMIZE ... FINAL`), unchanged.
- First record its shape with the two queries in the report's "How to reproduce": compressed bytes per subcolumn, and timestamp-delta quantiles.
- Small and medium sizes: the same generator restricted to 10% and 30% of the series, if the full table takes too long per run.

## Cases
| case | query | role | predicted A → B |
|---|---|---|---|
| T1 | the handoff's PromQL, as a range query (start 1704067800, end 1706659185, step 900) | target | sync wait −80% or more; wall −10–21% |
| T2 | `timeSeriesSelector` alone over the same window, `SELECT count(), sum(length(samples))` | target (read only) | wall −15–30% |
| G1 | `tests/performance/promql_*.xml` and `timeseries_*.xml` | guard | unchanged (±5%) |
| G2 | a 1-hour instant query on the same metric (few granules) | guard: small reads | unchanged; prefetch setup must not add more than 1 ms |
| E1 | T1 with the page cache warm | edge: data in page cache | unchanged; prefetch has nothing to hide |

## Method
- Drop the page cache before every run of T1, T2 and G2: `sync; echo 1 > /proc/sys/vm/drop_caches`. Drop the ClickHouse query cache too, and turn the uncompressed cache off (`use_uncompressed_cache = 0`). The capture's 8 GiB uncompressed cache would hide part of the effect.
- Alternate A and B runs (A B A B …), 7 pairs per case, `max_threads = 16`.
  - A: defaults.
  - B: `local_filesystem_read_prefetch = 1, allow_prefetched_read_pool_for_local_filesystem = 1, max_read_buffer_size_local_fs = 1048576`.
- Split B into its two halves for one pair each (prefetch only, buffer only), so the report can say which half does the work.
- Guards: `tests/performance/scripts/perf.py` with each setting set in the profile.

## Metrics
- Wall time (median, min–max).
- `SynchronousReadWaitMicroseconds`, `AsynchronousReadWaitMicroseconds`, `DiskReadElapsedMicroseconds`.
- `ThreadPoolReaderPageCacheHit` + `ThreadPoolReaderPageCacheMiss` (the read calls), `ReadCompressedBytes`.
- `OSReadBytes`.
- The volume's throughput-limit time, as in the capture.
- `ExternalAggregationWritePart` / `ExternalProcessingFilesTotal`, to answer the report's open question about the 25.6 GiB of filesystem reads.

## Correctness
- T1 results identical between A and B (`FORMAT TSV | md5sum`). The settings change the read schedule only.

## Pass / fail
- **Pass:** T1 wall time −10% or more in every pair, with sync wait −80% or more, and no guard slower by more than 5%. Then the next step is a branch that sets the two settings for PromQL samples reads in `StoragePrometheusQuery::applySettingsForPrometheusQuery`, with its own A/B.
- **Fail (wall flat, sync wait down):** the volume is at its throughput limit either way. The lever is fewer bytes: the data shape, the 1-hour bucket read for a 5-minute window, or a spill (see the report). Update the report.
- **Fail (wall flat, sync wait flat):** prefetch did not engage. Check `MergeTreePrefetchedReadPool` in the trace log and the read method (`local_filesystem_read_method`).

## Checklist
- [ ] record the build, volume type and throughput
- [ ] record the capture table's bytes per subcolumn and the timestamp-delta quantiles
- [ ] run T1, T2, G1, G2 and E1, A/B alternated; save the raw output under `bench/samples-timestamp-read-cost/ab/`
- [ ] update the report verdict and the backlog row
