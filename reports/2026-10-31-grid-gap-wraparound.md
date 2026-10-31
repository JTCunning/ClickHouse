# timeSeries*ToGrid is 4–6× slower on series with a mid-range gap

**Verdict:** Worth fixing. It is a 26.10 regression from #124773, and a small fix restores the 26.9 speed while keeping most of #124773's memory saving. Branch `lilred/perf-grid-gap-wraparound` is ready but unbuilt ([compare](https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred/perf-grid-gap-wraparound)). Neither open PR targets this directly.
**Task:** 2026-10-31-timeseriestogrid-gap-slowdown-125759 · upstream issue https://github.com/ClickHouse/ClickHouse/issues/125759 · **Build:** 26.11.1.974, a7f713612c80 (master, 2026-10-31) · **Date:** 2026-10-31

## What happens (plain language)
Each per-series grid aggregation state stores its time buckets in a small hash table keyed by bucket number. The hash function is the identity, so bucket *k* sits in slot *k mod table size*. Since 26.10 (#124773) the table may fill to 75% before it grows, down from 50%, to save memory. On a contiguous series that is free: bucket numbers fill consecutive slots and never collide.

On a series with a gap in the middle, the bucket numbers after the gap can exceed the table size and wrap around onto the slots already used by the buckets before the gap. Every such insert then walks the whole occupied run, about 3,100 slots per bucket in the case below. That is where the time goes: same rows read, same output, 4–6× the CPU.

## Numbers
Dataset: 200 series, one sample per minute, grid of 7 days at a 60 s step (10,081 points), window 60 s, `max_threads = 1`, 2-vCPU VM. Each series has 6,200 samples in both main cases. Median of 7 warm runs (min–max).

| query / case | median | min–max | vs. contiguous | notes |
|---|---|---|---|---|
| `timeSeriesLastToGrid`, contiguous [0, 6200) | 0.127 s | 0.122–0.150 | 1× | |
| `timeSeriesLastToGrid`, gap [0, 3100) + [6981, 10081) | 0.774 s | 0.771–0.803 | **6.1×** | the #125759 shape |
| `timeSeriesLastToGrid`, same gap, no wrap [0, 3100) + [5092, 8192) | 0.126 s | 0.124–0.150 | 1× | isolates the cause |
| `timeSeriesLastToGrid`, late start [4000, 10081) | 0.099 s | 0.099–0.123 | — | wraps without colliding |
| `timeSeriesLastToGrid`, sparse gap [0, 1500) + [8581, 10081) | 0.191 s | 0.187–0.215 | 3.7× vs. 3,000-sample contiguous (0.051 s) | also slow before 26.10 |
| `timeSeriesRateToGrid`, contiguous | 0.156 s | 0.150–0.183 | 1× | |
| `timeSeriesRateToGrid`, gap | 0.645 s | 0.640–0.670 | **4.1×** | |
| `timeSeriesRateToGrid`, sparse gap | 0.188 s | 0.187–0.213 | 3.0× vs. 3,000-sample contiguous (0.063 s) | also slow before 26.10 |

Rows read and output size are identical between the cases. Only CPU in the aggregation differs.

## Why (cause)
- The bucket table uses linear probing with an identity hash (`TrivialHash`). Commit d973cff, part of #124773, raised the fill limit from 1/2 to 3/4 (`TimeSeriesBucketsHashTableGrower::maxFill`, `src/AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h:50`).
- Walkthrough of the gap case:
  - 3,100 buckets before the gap and 1,211 after it fit in an 8,192-slot table that is 53% full. Under the old 1/2 limit, the table would already have grown to 16,384 slots by this point.
  - Bucket 8,192 lands on slot 0, the start of the 3,100-slot run.
  - Every bucket up to 10,080 then probes about 3,100 slots, until the table finally grows at 6,145 entries.
- The control case with the same gap but no bucket at or above 8,192 is as fast as the contiguous series, which confirms the mechanism.
- All four insert paths share the problem: row-at-a-time add (`:1124`), batch add (`:1234`), merge of partial states (`:329`, multi-threaded and distributed queries) and deserialization of stored states (`:381`).

## Proposed fix
Branch `lilred/perf-grid-gap-wraparound` (unbuilt), one helper of about 15 lines plus 4 call sites:
- **The rule:** grow the table one step before inserting a bucket that would wrap and land on an occupied slot, but only when the table is more than half full.
- **Effect:**
  - Buckets that collide get the old 1/2-limit behavior back, which is the 26.9 speed.
  - Tables with no collisions keep the 3/4 limit and its memory saving. That includes contiguous series, even ones that start late in the range.
  - Memory never exceeds the old 1/2 limit.
- **Safety:** no on-disk or wire format change, no new tuning number (it reuses the old 1/2 limit), and results are identical.
- **Expected effect:** the 6.1× and 4.1× rows drop to about 1×. This is predicted by a model of the hash table and needs a build to confirm.
- **Not fixed:**
  - Gaps whose wrap happens while the table is still below half full stay slow, as they were in 26.9: the sparse-gap rows above, and [0, 2048) + [8192, …).
  - Fixing those needs a different bucket layout, such as #124777's dense array, or a different hash. That is not a small change.
- **Effort:** S. Open item: the branch numbers need a build.
- **Tests and PR text:**
  - New stateless test `05474_timeseries_to_grid_gap_wraparound` covers 4 shapes through the add, batch add, merge and deserialize paths.
  - New perf test `timeseries_to_grid_gap.xml`.
  - The upstream-ready PR text is at [`prs/grid-gap-wraparound.md`](../prs/grid-gap-wraparound.md). Two rounds of local review by the bot's rules left no Blockers or Majors. The only open item is the build-side numbers.

## Upstream overlap
- **#125759** (open, no assignee, no linked PR) is the bug report. It names the same cause and suggests "grow before a wrapping insert when more than half full", which is what this branch does, refined to trigger only on a real collision.
- **#124773** (merged, in 26.10) introduced the 3/4 limit.
- **#122983** (open, +808/−71) replaces the hash table with a sorted sample buffer, but only for rate, increase and delta. It bumps `FORMAT_VERSION` (a backward-incompatible state change), so it does not cover `timeSeriesLastToGrid` and the other grid functions.
- **#124777** (open, CI green, bot recommends approval) stores dense states (at least half the bucket range used) in a plain array.
  - That would also remove this slowdown for the #125759 shape, which is 61% dense.
  - Sparser gapped series would still go through the hash table.
  - It is a much larger change. This branch is a small, backportable fix for 26.10 that does not conflict with it in intent: if #124777 lands, the helper still guards the hash-table path.
- **James's fork branch `jtcunning/timeseries-to-grid-flat-sorted-state`** (2 commits, 2026-10-19/20) moves the raw-sample functions (rate, increase, delta and others) to one flat array, which avoids the bucket table for them.
  - `timeSeriesLastToGrid`, quantile and MAD keep the table, so the two changes are complementary.
  - They touch the same insert sites, so a small rebase would be needed if both land.
- `git log origin/master -- src/AggregateFunctions/TimeSeries/` over the last 30 days shows no other change to the growth policy.
- **Memory cost to know about:** a series with a sample in every 2nd bucket (step smaller than the scrape interval) goes back to the pre-26.10 table size. It still uses no more memory than 26.9 did.

## How to reproduce
```bash
curl -sSfL -o clickhouse https://builds.clickhouse.com/master/amd64/clickhouse && chmod +x clickhouse
CH=./clickhouse
$CH local --path ./db -n -q "
CREATE DATABASE g;
CREATE TABLE g.s (case LowCardinality(String), id UInt32, ts DateTime64(3), v Float64) ENGINE=MergeTree ORDER BY (case,id,ts);
INSERT INTO g.s SELECT 'contig', id, toDateTime64(m*60,3), m FROM (SELECT number%200 id, intDiv(number,200) m FROM numbers(200*6200));
INSERT INTO g.s SELECT 'gap_mid', id, toDateTime64(m*60,3), m FROM (SELECT number%200 id, intDiv(number,200) m FROM numbers(200*10081)) WHERE m<3100 OR m>=6981;
INSERT INTO g.s SELECT 'gap_nowrap', id, toDateTime64(m*60,3), m FROM (SELECT number%200 id, intDiv(number,200) m FROM numbers(200*10081)) WHERE m<3100 OR (m>=5092 AND m<8192);
OPTIMIZE TABLE g.s FINAL;"
# one timing: repeat 8x in one session with --time, drop the first
$CH local --path ./db --enable_time_series_aggregate_functions=1 --max_threads=1 --time -q "
SELECT sum(length(r)) FROM (SELECT id, timeSeriesLastToGrid(0, 604800, 60, 60)(ts, v) r FROM g.s WHERE case='gap_mid' GROUP BY id) FORMAT Null"
```
For the branch column, build `lilred/perf-grid-gap-wraparound` and run the same commands interleaved with master (A B A B …), plus `tests/performance/timeseries_to_grid_{gap,bucketing,aggregation_functions,deserialization}.xml`.
