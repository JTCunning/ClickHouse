# recent-samples-merge lab

Lab of `reports/2026-11-22-recent-samples-merge.md`: why short-range PromQL queries are faster on the recent samples table
of a version 11 `TimeSeries` table, and what a single samples table would cost instead. Master binary only, no build.

## Files
- `server.sh`, `config.xml.in`: one server (copied from `bench/lc-uuid-id/ab/`; defaults `SERVER_MEM=2300000000`,
  `QUERY_MEM=2000000000`, data under `tools/clickhouse/data/recent-samples-merge/<name>`). `server.sh start lab 19310`, `server.sh stop lab`.
- `gen.sh [layout ...]`: creates the layouts in database `rsm` and fills their inner tables directly with SQL
  (2000 series × 14 days × 15 s, about 1 minute per layout on 2 vCPU). The layouts are listed at the top of the script.
- `time.sh <tag> <shapes> <arms> [runs]`: PromQL shapes through `prometheusQuery` / `prometheusQueryRange`, timed server side
  from `system.query_log`; `HASH=1` prints the row count and a rounded, order-independent hash per arm and shape.
- `ins.sh <tag> <layout,...>`: insert cost through the TimeSeries sink, on empty copies (`CREATE TABLE ins_L AS L`).
- `tail.sh <layout ...>`: the unmerged state, copies `u_L` whose last 1-2 hours arrive as one part per 2 minutes, merges stopped.
- `lt.sh <name> <young seconds> [hash]`: candidate L4 (one samples table, `PARTITION BY tier`, a mover from 5 h labels to month labels).
- `results/`: raw `*.jsonl` per run (time.sh also writes the `*.sql` it ran there; they hold that day's timestamps and aren't committed); `v1-*` are from the first generator (periodic counter
  increments, which compressed unrealistically well in long runs), kept as a cross-check of the timing ratios.

## Preconditions (check before timing anything)
1. `t_end` (written by `gen.sh` to `$DATA/t_end`) must be less than 12 hours old: the selector reads the recent samples
   table only when the range starts inside its TTL window (`now - 4 d + 60 s`), and the 3.5 d shapes (`r3d_*`) start
   302400 s before `t_end`. After that, run `rm $DATA/t_end && gen.sh` again.
2. The layout after `gen.sh` (its last table; `lt.sh lt1d 86400` gives 7 parts: 6 young + 1 month): `m` samples 1 part, recent samples 21 parts (one per 5 h partition);
   `p5*`/`l2p5` 71 parts, `pd*`/`l2` 15 parts; 674000 rows for 1 h buckets, about 2.69M for 15 min buckets, about 1.25M for `l2*` (they depend on `t_end`'s minute)
   (sub-rows for the last 4 d + 1 h, folded 1 h rows before that). A server restart merges nothing here (every partition is
   one part), but `tail.sh`'s `SYSTEM STOP MERGES` does not survive a restart: run `tail.sh` again after one.
3. Counters per query come from `system.query_log` and include the tags and time-ranges tables (2000 rows each, one part each);
   only the samples-side part of `SelectedMarks`/`read_bytes` differs between layouts.
4. `max_threads = 2`, `use_query_condition_cache = 0` on every timed query (time.sh sets them). For a stream claim, rerun with
   `MAX_THREADS=8 EXTRA_SETTINGS='max_threads_min_free_memory_per_thread = 0'` (this host otherwise caps streams at 2).
5. Run clients with `< /dev/null` and `clickhouse local` from a directory without a `config.xml`.

## Commands (as run on 2026-11-22; results in `results/`)
```bash
D=/root/nerve-workspace/research/bench/recent-samples-merge   # or the batch worktree copy
$D/server.sh start lab 19310
SERIES=2000 $D/gen.sh                                   # all layouts, ~10 min
HASH=1 $D/time.sh hash all m,m:0,mb,p5,p5b,p5bg,pd,pdbg,l2,l2p5          # results/hash-v2.tsv
$D/time.sh d2 short m,m#aa,m:0,mb,p5,p5b,p5bg,pd,pdbg,l2,l2p5 5           # at t_end (6 min after a 5 h boundary)
EVAL_OFFSET=86400 $D/time.sh d3full short m,m#aa,m:0,mb,p5,p5b,p5bg,pd,pdbg,l2,l2p5 5   # full partitions
$D/time.sh m2 mid m,m#aa,m:0,p5,p5bg,pd,pdbg,l2,l2p5 5
$D/time.sh lg2 long m,m#aa,mb,p5,p5bg,pd,pdbg,l2,l2p5 5
EVAL_OFFSET=86400 MAX_THREADS=8 EXTRA_SETTINGS='max_threads_min_free_memory_per_thread = 0' $D/time.sh t8 i_rate,r6h_sum,r7d_n m,m:0,p5,pd 5
$D/time.sh tier tier m,m:0 5                                               # the two halves of a tiered 7 d query
$D/tail.sh m pd && $D/time.sh u2 short m,u_m,m:0,u_m:0,pd,u_pd 5          # unmerged last hour
$D/lt.sh lt1d 86400 hash && HASH=1 $D/time.sh hash-lt all lt1d            # L4, hash also mid-move
EVAL_OFFSET=10800 $D/time.sh s3h short m,m#aa,m:0,p5,pd,lt1d 5
$D/time.sh lt-ml r1d_sum,r1d_n,r3d_sum,r3d_n,r7d_sum,r7d_n,r14d_sum,r14d_n m,m#aa,lt1d,p5,pd 5
$D/ins.sh ins m,pd,p5 && $D/ins.sh ins2 m,lt1d,pd
$D/server.sh stop lab
```
`time.sh` takes one shape group per call (`short`, `mid`, `long`, `tier`, `all`) or a comma-separated list of shape names.
