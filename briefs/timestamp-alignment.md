# Brief: should TimeSeries align timestamps to a grid?

- **ID:** `timestamp-alignment` · **Area:** storage and querying · **Written:** 2026-11-24 by lil red, against master `05f159a69597`
- **Tracking issue:** none yet (see README, "Where the result goes")
- **Spend:** James's cap. Stop points below. This is a design question first and a benchmark second.

## Question (James's)

Should we align sample timestamps closer to the scrape or step intervals, so that queries get faster, or the timestamp column gets much smaller on disk? If so, how: by rounding at ingest (lossy), by storing timestamps relative to a grid (lossless), or by something else? What does each option cost in result accuracy against Prometheus?

## What lil red already knows (verify, don't trust)

**1. Bytes per sample depend on jitter.** These are from `reports/2026-11-08-samples-timestamp-read-cost.md` (master `7b5f2bf328f9`, today's default codecs: `Delta, T64, ZSTD(3)` timestamps, `ALP, ZSTD(3)` values, 1 h buckets, 300 series, 2 days at a 15 s scrape):

| timestamps | timestamp bytes/sample |
|---|---|
| exact 15 s grid | 0.042 |
| ±5 ms jitter | 0.662 |
| ±500 ms jitter | 1.577 |
| uniform inside the 15 s slot | 1.974 |

| values | value bytes/sample |
|---|---|
| counter with random mantissa (like real CPU-seconds) | 5.895 |
| linear counter | 0.250 |
| near-constant (one capture of James's) | 0.0145 |

So the timestamp column alone shrinks 16–47× when the timestamps sit on an exact grid. How much the whole samples column shrinks depends on the values:

| timestamps | busy counters | near-constant gauges |
|---|---|---|
| ±5 ms jitter | about 10% of the bytes | about 98% |
| ±500 ms jitter | about 21% | about 99% |

That near-constant capture really read 98× more timestamp bytes than value bytes. Report every storage number for the timestamp column **and** for the whole samples column, on at least these three value shapes.

**2. Where today's jitter comes from.**

- Prometheus spreads its targets over the scrape interval with a per-target offset. Each series therefore sits on its own grid, not on a global one.
- Prometheus also adjusts scrape timestamps that fall within a small tolerance of the expected time. Check the `--scrape.adjust-timestamps` and `--scrape.timestamp-tolerance` flags and the version that added them. With that, a Prometheus sender's series may already have exact intervals, and so already compress to about 0.04 bytes/sample.
- Other senders may not align: the OpenTelemetry Collector (Prometheus receiver plus remote-write exporter), Grafana Alloy, or Prometheus with the adjustment turned off.
- So the storage question may be mostly about non-Prometheus senders. Measure before deciding.

**3. Query cost.**

- On one wide range query at 16 streams, decoding samples was about 70% of CPU (`reports/2026-11-09-join-fixed-cost.md`).
- Fewer timestamp bytes means less I/O and less decompression. But the `Delta, T64` decode runs per value whatever the bytes.
- A real query win would come from not decoding timestamps at all: implicit timestamps, where a grid function computes a sample's grid position from its index.

**4. Semantics that alignment changes.**

- Prometheus range selectors and the instant lookback select `(t - range, t]`: open on the left, closed on the right, in Prometheus 3.x. Check what the converter does. Samples snapped onto step boundaries sit exactly on those edges, so which edge is open decides whether a sample is in or out.
- lil red hit this once already: a rollup with left-closed intervals returned the previous scrape at every point on exact 15 s scrapes (`reports/2026-11-11-rollup-samples.md`).
- `rate` and `increase` extrapolate to the window's edges from the first and last sample timestamps, so moving those timestamps changes results.
- `timestamp()` returns the stored timestamps.
- Staleness markers and the lookback delta work on real times.
- Snapping to a global grid moves a sample by up to half an interval (7.5 s at a 15 s scrape). Snapping to the series' own grid moves it by at most the tolerance.

**5. Related work.**

- `reports/2026-11-11-rollup-samples.md` and the branch `lilred-perf-rollup-samples` (James's rollups: pre-aggregated rows for coarse steps).
- James's draft upstream PR 128889 (`samples_downsampling_periods`: TTL `GROUP BY` keeps the last sample per interval, with its original timestamp).
- Draft upstream PR 121216 (bucketed native execution).

Alignment, rollups and downsampling overlap. The answer should say how they fit together.

## Options to evaluate

| id | option | lossless? | what it needs |
|---|---|---|---|
| O0 | today: `Delta, T64, ZSTD(3)` | yes | nothing |
| O1 | a better standard codec for timestamps: `DoubleDelta`, `DoubleDelta, ZSTD`, `Delta, ZSTD(1..9)`, `T64, ZSTD`, … | yes | a default codec change, pinned per table version |
| O2 | a jitter-aware lossless encoding: per row (series × bucket), store the start, the median interval and small residuals; as a new codec or a new column layout | yes | new code; maybe a new table version |
| O3 | opt-in rounding at ingest to the series' own grid, within a tolerance (for example 1–100 ms), Prometheus-style | no, bounded by the tolerance | a TimeSeries setting; small |
| O4 | opt-in snapping to a global grid (multiples of the scrape interval) | no, up to half an interval | a setting, plus decisions on rate extrapolation and boundary semantics |
| O5 | implicit timestamps for regular rows: store start and interval, plus exceptions; grid functions compute positions without decoding | yes, with the exceptions | a large change in storage and grid functions |
| O6 | no timestamp change: leave coarse steps to rollups and downsampling | n/a | already in progress |

## Method

1. **Measure real jitter first.**
   - Scrape a realistic set of targets for at least an hour with each sender: Prometheus with defaults, Prometheus with the timestamp adjustment off, the OpenTelemetry Collector, and Grafana Alloy if it's quick to set up. Remote-write each into its own ClickHouse TimeSeries table.
   - For each sender, report quantiles of the interval error: consecutive-sample deltas minus the scrape interval, in ms. The report's "How to reproduce" section has the query. Also report the share of rows with exactly constant intervals.
   - **Stop point 1:** if every sender lands within ±1 ms with exact intervals in most rows, O1–O4 have little storage to win on real data. Report that, then continue only with O5 and the query-side question.
2. **Storage.**
   - Emulate O1–O4 on master with DDL and `INSERT SELECT` (rounding expressions; codec declarations through explicit `INNER COLUMNS`), on the real-jitter data plus three value shapes: busy counters, histogram buckets, near-constant gauges.
   - For O2 and O5, estimate the size from the residual distribution (a harness is fine; follow the README's compiler rule). A rough prototype codec is better if the estimate looks big.
   - Report the timestamp bytes/sample and the samples-column bytes/sample for each option and shape, with 14 days of data so that merges have settled.
3. **Query speed.**
   - Use the dashboard and alert-rule set from `dashboard-query-profile.md` (same query text on both sides, the full query in every row), on O0 vs O1 vs O3/O4 tables holding the same data, warm and cold.
   - For O5, measure what an implicit-timestamp grid function could save: the cost of decoding timestamps plus the grid functions' position search, as a share of each query, from `system.trace_log`. That is the ceiling of O5's query win.
4. **Accuracy (O3, O4).** Hold the same raw scrapes in a real Prometheus as the reference. Run the query set on Prometheus, and on ClickHouse with O0 and with each lossy option. Report per query the share of changed points, and the maximum absolute and relative difference. Call out `rate`, `increase`, `timestamp()` and lookback edge cases separately.
5. **Fit with rollups and downsampling.** One paragraph: does alignment make rollups or downsampling simpler or more exact, for example rollup intervals that divide the step exactly? Or do they make alignment unnecessary for coarse steps?

## Deliverables

- A jitter table: sender | interval error p50, p99, p99.9 | share of exact-interval rows.
- A decision matrix: option | timestamp bytes/sample | samples bytes/sample on each value shape | query set change | result differences vs Prometheus | size of the change | compatibility (table version, existing data).
- A recommendation: one option (or "none"), the next step, and what would change the answer.
- Optional: an O1 codec prototype or an O3 setting prototype, with its A/B.

## Out of scope

Values (ALP and its codecs): only as the denominator of the total. Native histograms. Changing Prometheus.
