# Remote write spends 8% of the server's CPU allocating and freeing the parsed request
**Verdict:** worth a 6-line fix: parse the request into a protobuf arena; branch `lilred-perf-remote-write-arena` (unbuilt).
**Update 2026-11-21: built (James, #24 issuecomment-7059357140): pass. CPU per sample -9.5% (10 labels), -9.9% (20 labels), -9.6% (values over 22 bytes), merges stopped; -3.8% with merges running (lower in 15 of 15 paired rounds); A/A +2.4% against B. His CPU profile: parse + destroy 12.9% → 4.0% of server CPU samples. Predicted -7..-17% with merges stopped: hit (measured ÷ emulated upper bound 0.56; with merges 0.45).**
**Task:** 2026-11-21 weekend scout (James, chat 2026-11-20) · **Build:** master 26.12.1.112, `521c4bcb7b94` · **Date:** 2026-11-21
**A/B plan:** [ab-plans/remote-write-arena.md](../ab-plans/remote-write-arena.md)

## What happens (plain language)
Each remote-write request is decoded into a tree of protobuf messages: one per series, label and sample, plus two strings per label. On the heap every one of them is a separate allocation through the allocator and the memory tracker, and every one is freed again when the request is done. For 2,000 series with 10 labels that is about 70,000 allocations and as many frees per request. A protobuf arena gives the same tree from a few large blocks and frees them together.

## Numbers
2 vCPU / 4 GiB, master `521c4bcb7b94`, default `TimeSeries` table, `bench/remote-write-baseline/rw.py` with 2,000 series × 1 sample × 10 labels per request from a universe of 20,000 series (warm tags cache), 150 requests after 30 warm-up requests, ClickHouse's CPU profiler at 2 ms (`query_profiler_cpu_time_period_ns` in the default profile):

| part of the server's CPU | profiler samples | share |
|---|---|---|
| all | 1,149 | 100% |
| background merges (`query_id` `<uuid>::<part>`) | 587 | 51% |
| the request handler thread | 199 | 17% |
| parsing the `WriteRequest` | 54 | 4.7% |
| destroying it | 73 | 6.4% |
| parse or destroy with an allocator / memory-tracker frame | 96 | 8.4% (17% of the CPU outside merges) |

The 2026-11-01 baseline on the same shape put protobuf parsing at 10%; destruction wasn't broken out then. Label values in `rw.py` are 22 bytes or shorter, so they fit the inline string buffer: real label values (pod names, instance addresses with domains) are often longer and keep a heap buffer even in the arena.

A/A pass of the A/B kit on master (merges stopped, 4 rounds of 10 requests per server): 17.5 vs 16.25 µs CPU per sample, identical table hashes.

## Why (cause)
`PrometheusRequestHandler` (`src/Server/PrometheusRequestHandler.cpp:356` at `521c4bcb7b94`): `prometheus::WriteRequest write_request;` on the stack, so every nested message and string lives on the heap; `ParsePartialFromZeroCopyStream` allocates them one by one and the destructor at the end of the scope frees them one by one. `PrometheusRemoteWriteProtocol::write` only reads the request through const accessors and copies what it needs into columns.

## Proposed fix
`google::protobuf::Arena arena; auto & write_request = *google::protobuf::Arena::Create<prometheus::WriteRequest>(&arena);` (protobuf 36.x in `contrib/`; arenas are on for proto3 by default), with the arena in the same scope as before. Same parse, same lifetime, same accessors. Effort S (one file, +10/−1, plus an error-path integration test). Predicted (emulated by subtraction): −7..−17% server CPU per sample with merges stopped, −3..−8% with merges running.

## Found on the way
A zstd remote-write body whose frame is cut short is answered 204 and its series are dropped (master `521c4bcb7b94`: a 200-series request cut at 25/50/75/95% of its 496 compressed bytes, 204 every time, nothing written). It predates this branch; a draft upstream issue is on #24.

## Upstream overlap
- No open `comp-promql` PR or issue, no merged or closed PR found for "protobuf arena" / "WriteRequest arena" / "remote write arena" (2026-11-21); no `google::protobuf::Arena` use in `src/`.
- In flight on the same path but other mechanisms: #123144 (James: parallel flush of the samples tables), #121062 (active-series cache in the sink), #115220 (timestamp checks in the protocol), #125232 (503 on timeouts). Remote write 2.0 (#119734, James's `remote-write-v2-*` branches) parses another message in the same handler; the same change applies there.

## How to reproduce
```bash
cd research/bench/remote-write-arena
DATA=/tmp/rw SERVER_MEM=2000000000 QUERY_MEM=1500000000 \
  PROFILE_EXTRA='<query_profiler_cpu_time_period_ns>2000000</query_profiler_cpu_time_period_ns><query_profiler_real_time_period_ns>0</query_profiler_real_time_period_ns>' \
  ./server.sh start A 19401
clickhouse client --port 19401 -q "CREATE TABLE prometheus ENGINE = TimeSeries"
T0=$(( ($(date +%s) - 3600) * 1000 ))
python3 ../remote-write-baseline/rw.py http://127.0.0.1:21401/write --series 2000 --labels 10 --universe 20000 --requests 30 --ts0 $T0
python3 ../remote-write-baseline/rw.py http://127.0.0.1:21401/write --series 2000 --labels 10 --universe 20000 --requests 150 --ts0 $(( T0 + 450000 ))
# then count system.trace_log (trace_type = 'CPU') stacks with ParsePartialFromZeroCopyStream / TcParser / ~WriteRequest / SharedDtor frames
```
