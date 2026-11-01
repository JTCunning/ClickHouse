# Remote-write ingest baseline (TimeSeries v11, dedup cache)

**Verdict:** baseline only, no fix in this run. At Prometheus's default batch shape the server spends ~20–26 µs of CPU per sample, and half of that is background merges of the tiny parts each request creates. Two cheap levers stand out: (1) the `ALP` value codec re-runs its full parameter search for every 256-row granule, adding about 16% to ingest CPU; (2) the `LowCardinality(UUID)` half of the series id adds about 7%. The tags dedup cache already saves ~27% and works as intended.
**Task:** 2026-10-31-baseline-remote-write-ingest-and-the-ded (scout) · **Build:** 26.11.1.974, a7f713612c80 · **Date:** 2026-11-01

## What happens (plain language)

A remote-write request becomes one `INSERT` into the TimeSeries table. Async insert is on by default, so each request waits 50–200 ms in the async insert queue and is then flushed together with whatever else arrived. The flush writes one new part into each of 3–4 inner tables: tags (only for series the dedup cache hasn't seen), time ranges, samples and recent samples. With one sender, that is 3.2 new parts per 2,000-series request. Merging those parts costs as much CPU as writing them.

## Numbers

Host: 2 vCPU / 4 GiB VM, server and load generator on the same machine (the generator takes the remaining CPU, so read the CPU column, not samples/s). Dataset: series with 10 labels (100 metric names, 1,000 instances, 7 jobs, plus 7 labels of cardinality 10–10,000). Timestamps are within the last 3 hours, 15 s apart. Each sender cycles through 20,000 series. Server CPU = utime+stime of the server process (includes merges) / samples sent. Requests are hand-encoded protobuf in literal-only snappy, so snappy decode is close to a memcpy. Real snappy adds a little.

**Steady state, by batch shape** (warm = series already in the dedup cache)

| shape | senders | CPU µs / sample | CPU ms / request | new parts / request | samples/s (CPU-bound host) |
|---|---|---|---|---|---|
| 2,000 series × 1 sample (Prometheus default), warm | 1 | 22.4–25.8 | 45–52 | 3.1–3.3 | ~18k |
| same, first sight of the series (cold) | 1 | 21.5–25.5 | 43–51 | 4.2 | ~14k |
| same, warm | 4 | 19.1 | 38 | 1.4 | ~30k |
| 10,000 series × 1 sample, cold | 1 | 19.8 | 198 | 3.4 | ~28k |
| 500 series × 20 samples (catch-up / backfill), cold | 1 | 6.6 | 66 | 4.3 | ~69k |

Per-request latency for one sender is 90–300 ms. That is the async insert busy timeout (adaptive, 50–200 ms), not work.

**Where the CPU goes** (2,000 × 1, warm, 1 sender; ClickHouse's CPU sampler at 2 ms, 532 samples; a sample can count in several rows)

| part of the path | share of server CPU |
|---|---|
| background merges of the new parts | 50% |
| …of which `AggregatingMergeTree` merge logic | 11% |
| `ALP` codec, parameter search (`estimateParamCandidates`) | 12% (3% at insert, 10% in merges) |
| `LowCardinality(UUID)` id column (dictionary build / reverse index) | 12% (3% insert, 9% merges) |
| protobuf parse of the request | 10% |
| ZSTD | 6% |
| async insert queue | 5% |
| sipHash of tags (series id) | 3% |
| tags dedup cache lookups | 2% |

With merges paused (`SYSTEM STOP MERGES`), CPU per sample falls from 28.3 to 13.6 µs, which confirms the 50% merge share.

**A/B on whole-path CPU** (2,000 × 1, 1 sender, 5 interleaved warm runs of 20 requests per table; µs CPU per sample)

| table variant | median | min–max | paired change vs default (median of 5) |
|---|---|---|---|
| default v11 schema (`ALP, ZSTD(3)` values, `LowCardinality(UUID)` id, dedup cache on) | 25.8 | 19.3–31.0 | — |
| values `ZSTD(3)` only | 22.8 | 19.0–25.3 | **−16%** (−4.3 µs) |
| id `Tuple(UInt64, UUID)` | 23.3 | 17.8–30.5 | **−7%** (−1.8 µs) |
| tags dedup cache off | 34.8 | 30.3–40.5 | **+37%** (cache saves 27%) |

Runs are noisy (±20%) because merges started by one run spill into the next. The paired differences point the same way in all 5 runs.

**ALP in isolation** (1M Float64 values written as 500 parts of 2,000 rows each with merges stopped; 1 thread; median of 5)

| value codec | granule | CPU ms | vs ZSTD |
|---|---|---|---|
| `ZSTD(3)` | 256 rows | 535 | — |
| `Gorilla, ZSTD(3)` | 256 rows | 531 | −1% |
| `ALP, ZSTD(3)` | 8,192 rows | 572 | +7% |
| `ALP, ZSTD(3)` | **256 rows (TimeSeries default)** | **990** | **+85%** |

Storage size on this synthetic data: samples tables 0.98 MB with ALP and 0.94 MB with ZSTD only. The values are near-integers here, so this says nothing about ALP's ratio on real gauges.

## Why (cause)

- **Merges = half the CPU.** Every flush of the async insert queue writes a part into each inner table. One sender gets no batching across requests, so parts are ~2,000 rows each. With 4 senders the queue combines requests (1.4 parts per request) and CPU per sample drops ~20%. This is the async insert queue working as designed. The lever is batching (`async_insert_busy_timeout_*`, or more concurrent remote-write shards), not code.
- **ALP's parameter search is a fixed cost per compressed block, and blocks are tiny here.** For every compressed block, `ALP` takes 8 sub-samples of 32 values and tries all 190 exponent/factor pairs on each: ~48,600 trial encodings, whether the block holds 256 values or 1M. Compact parts (every new part here) close a compressed block per column per granule. The samples and recent-samples tables use 256-row granules, and a freshly written part has 1 sample per row. So the search runs once per 256 values: about 190 trials per stored value, where at 8,192-row granules it is ~6. Merged parts with full hourly buckets have far more values per granule, so the cost falls on the ingest side: new parts and early merges. Pointers: `src/Compression/CompressionCodecALP.cpp:366-377` (search on every `encode` call), `:585-675` (`estimateParamCandidates`), constants `:194-195`; per-granule blocks in `src/Storages/MergeTree/MergeTreeDataPartWriterCompact.cpp:309-398`; defaults `src/Storages/TimeSeries/TimeSeriesSettings.cpp:34,39` (256) and `normalizeTimeSeriesDefinitionImpl.cpp:622-625` (`ALP, ZSTD(3)`).
- **`LowCardinality(UUID)` id.** The second half of the id is a per-series hash, so the dictionary is nearly as large as the column. Every insert and merge rebuilds a reverse index over it. This is a v11 schema choice (`TimeSeriesIDGenerator.cpp:54-56`); changing the default means a new table version.
- **Ruled out:** the per-sample generic-value loops in `makeTimeSeriesBlock` (`PrometheusRemoteWriteProtocol.cpp:73-79,137-141`) and `findMinMax` (`TimeSeriesSink.cpp:173-189`) look slow on paper but did not appear in the profile at 1 sample per series. Tag sorting, sipHash and the dedup cache are each ≤3%.

## Proposed fix

Report only. Two follow-ups, filed as scout tasks:

1. **ALP: scale the parameter search to the block size** (S). Use one 32-value sub-sample per 1,024-value ALP block, up to the existing 8, instead of always 8. This reuses the existing constants, adds no new threshold and changes no format: the chosen parameters are stored per block as today. A 256-value block then runs ~6k trials instead of ~48.6k. Expected gain: most of the +85% codec overhead at 256-row granules, i.e. up to ~16% of ingest CPU (estimate, needs a build). The risk is compression ratio on small blocks: measure it on realistic gauge and counter values and on the codec's own tests. The codec is outside the TimeSeries code and gated behind `enable_alp_codec`, so its owners need to review it (see #109533 and #122248 for recent work on the search).
2. **`LowCardinality(UUID)` in the series id: measure the read-side benefit before proposing a change** (scout). It costs ~7% of ingest CPU. A schema change needs a new version, so it is only worth it if the read path doesn't depend on it.

No-code lever for users today: more remote-write shards (`queue_config.max_shards` / `min_shards`) or a larger `async_insert_busy_timeout_min_ms` mean fewer, bigger parts. With 4 senders, CPU per sample fell ~20% here.

## Upstream overlap

- Open upstream: #128404 (dedup cache under one lock), #128256 (cache expiry jitter), #126132 (tags cache prewarm), #93833 (remote-write routing by URL). None touches the codec or part count.
- James's fork branches: `timeseries-dedup-cache-set-many` (= #128404), `jtcunning/tags-dedup-cache-expiration-jitter` (= #128256), `jtcunning/timeseries-tags-cache-prewarm` (= #126132), `jtcunning/timeseries-parallel-inner-writes` (writes the inner tables in parallel; helps latency, not CPU per sample), `jtcunning/timeseries-insert-series-cache` and `jtcunning/timeseries-metric-family-insert-cache` (older insert caches, superseded by the upstream dedup caches), `jtcunning/remote-write-v2-*` (protocol 2.0), `jtcunning/119501-bucket-codecs-master` and `dev/timeseries-value-dd-varint-codec` (other codecs; neither changes ALP's search).
- ALP history: #91362 (added), #109533 (AUTO short-sample bias), #117553/#122248 (adaptive pool). No open PR on the search cost.
- Noted only: #122988 (stale dedup cache entry after a failed insert) is a correctness issue; #119433 is the DoubleDelta codec's read cost.

## How to reproduce

Scripts in `bench/remote-write-baseline/` on this branch: `rw.py` (load generator, standard library only), `run.sh` (parallel senders plus server CPU, parts and RSS), `alp_granule_bench.sh`, `server-config.xml`, `variant-*.sql`, and raw results in `results-ab-2026-11-01.txt`.

```bash
clickhouse server --config-file=server-config.xml &          # TCP 19000, remote write on 19093
clickhouse client --port 19000 -q "CREATE TABLE prometheus ENGINE = TimeSeries"
clickhouse client --port 19000 --enable_alp_codec=1 --multiquery < variant-zstd-values.sql
NOW=$(( $(date +%s) * 1000 - 3*3600*1000 ))
./run.sh cold write 1 2000 1 20000 10 --ts0 $NOW              # first sight of the series
./run.sh warm write 1 2000 1 20000 20 --ts0 $((NOW+1200000))  # LABEL PATH SENDERS SERIES SAMPLES UNIVERSE REQUESTS
./run.sh warm write_zstd 1 2000 1 20000 20 --ts0 $((NOW+1200000))
CH=clickhouse ./alp_granule_bench.sh
```

Profile: set `query_profiler_cpu_time_period_ns=2000000` and enable `trace_log`, then group `system.trace_log` by `demangle(addressToSymbol(...))` over the stack. Background merges show up with a `query_id` of `<table uuid>::<part>`.
