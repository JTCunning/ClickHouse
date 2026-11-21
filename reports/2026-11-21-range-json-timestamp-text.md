# The range-query JSON writer formats the same step timestamps once per series
**Verdict:** worth a small fix for wide, non-aggregated range panels: branch `lilred-perf-range-json-timestamp-text` (unbuilt). The size on a 16-core host is the open question.
**Update 2026-11-21: built (James, #22 issuecomment-7059502143): pass. `m` at step 15 s -10.2% wall (172.0 vs 191.6 ms), `rate()` -5.3%, `g` -3.3%, every-other-step block -8.3%, skewed block and control flat; query CPU -3.1% while its duration drops 159 → 140 ms (the saving is in the writer thread, as predicted). Bodies identical except the `sum()` control, whose last digits differ between two requests to master too. Prediction -5..-20% on 16 cores: hit; 2-vCPU emulation -6..-8%: measured ÷ emulated 1.3-1.7, the writer's share grows with cores as expected.**
**Task:** 2026-11-21 weekend scout (James, chat 2026-11-20) · **Build:** master 26.12.1.112, `521c4bcb7b94` · **Date:** 2026-11-21
**A/B plan:** [ab-plans/range-json-timestamp-text.md](../ab-plans/range-json-timestamp-text.md)

## What happens (plain language)
`/api/v1/query_range` returns a matrix: for every series, a list of `[timestamp, "value"]` points. Every series of a range query is evaluated at the same steps, but the writer turned each point's timestamp into text again for every series: a query over 2,000 series at 1,441 steps formats the same 1,441 timestamps 2,000 times. The writer runs in one thread after the query's final `ORDER BY`, so its time adds to the request one to one.

## Numbers
2 vCPU / 4 GiB, master `521c4bcb7b94`, a default `TimeSeries` table with 2,000 series of a counter `m` and a 3-decimal gauge `g`, 15 s samples over 6 h (`bench/range-json-timestamps/gen.sh`). Wall-clock profiler at 2 ms on the API requests (`query_profiler_real_time_period_ns`), 4 requests per case:

| request | points | body | request time | writer | of which timestamp text |
|---|---|---|---|---|---|
| `m`, step 15 s | 2.88M | 57 MB | 668–764 ms | 58–88 ms | 38–56 ms (15–19 ns/point) |
| `rate(m[5m])`, step 30 s | 1.44M | 50 MB | 520–595 ms | 52–68 ms | 10–22 ms |
| `g`, step 60 s | 720k | 16 MB | 204–219 ms | 24–34 ms | 2–12 ms |
| `sum(rate(m[5m]))`, step 30 s | 720 | 24 kB | 440 ms | ≈ 0 | ≈ 0 |

(The profiler counts one sample per 2 ms per thread; the writer is one thread.) Values that are integers are cheap to write, so for `m` the timestamps are most of the writer; `rate()` values are long decimals and take most of the rest.

An A/A pass of the A/B kit on master at 1,000 series (5 rounds): medians within ±3.6%, response bodies identical on both servers.

## Why (cause)
`PrometheusHTTPProtocolAPI::writeQueryResponseRangeVectorBlock` (`src/Storages/TimeSeries/PrometheusHTTPProtocolAPI.cpp:506-518` at `521c4bcb7b94`) calls `writeTimestamp` → `writeText(Decimal64, scale)` for every point: the scale checks, the split into whole and fractional parts, the digit conversion and the trailing-zero trim run each time. The instant-vector writer already formats its single timestamp once per block (#115567, merged 2026-08).

## Proposed fix
For `/api/v1/query_range` only: format the timestamps of the longest series of each block once, and copy the text for every point whose timestamp equals one of them (next-entry fast path, binary search when a series starts later or skips steps). A step the longest series lacks is formatted as before; blocks of one series and `/api/v1/query` keep master's path. Same bytes by construction. Effort S (one file and its header, about +80/−12 lines, plus an integration test). Predicted (emulated by subtraction): −2..−8% on 2 vCPU for `m`; larger on 16 cores, where the parallel part of the request shrinks and the writer doesn't (unmeasured).

Round 1 of the preflight found the first version's per-series scan quadratic on skewed blocks (one long-lived series next to thousands of short ones); fixed with a galloping search from the cursor, and a skewed and an every-other-step case are in the A/B.

## Upstream overlap
- No open `comp-promql` PR or issue changes the range-vector writer (2026-11-21: 88 open PRs, 37 issues). #122585 (open) moves this loop into a `writeSamples()` helper without changing the per-point code: a textual conflict if it lands first. #125230 (compression) and #130397 (warnings) touch other parts of the handler. No James branch matches.

## How to reproduce
```bash
cd research/bench/range-json-timestamps
DATA=/tmp/rj SERVER_MEM=1400000000 QUERY_MEM=1200000000 \
  PROFILE_EXTRA='<query_profiler_cpu_time_period_ns>0</query_profiler_cpu_time_period_ns><query_profiler_real_time_period_ns>2000000</query_profiler_real_time_period_ns>' \
  ./server.sh start A 19301
./gen.sh 19301 && read T_START T_END < window.txt
curl -s -o /dev/null --data-urlencode query=m -d start=$T_START -d end=$T_END -d step=15 http://127.0.0.1:21301/api/v1/query_range
# then group system.trace_log (trace_type = 'Real') by the frames writeQueryResponse / writeText / writeScalar, joined to query_log on query_id
```
