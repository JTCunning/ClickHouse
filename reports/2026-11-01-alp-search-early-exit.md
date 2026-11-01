# ALP codec: cut the parameter search without changing a byte
**Verdict:** worth fixing; fix branch pushed (unbuilt). An exact early exit in ALP's brute-force parameter search makes the encoder 40–90% cheaper on decimal-like values (2–9% on full-precision doubles). The compressed bytes are the same in all 76 cases measured. The candidate fix in the task (fewer samples for small blocks) was rejected: it costs up to 4.8% of compressed size.
**Task:** 2026-10-31-alp-codec-scale-the-parameter-search-to (scout, follow-up of the remote-write baseline) · **Build:** 26.11.1.974 (a7f713612c80); code at c189bd99fbda · **Date:** 2026-11-01

## What happens (plain language)
ALP stores floats as small integers by finding a decimal scale (an exponent/fraction pair) per block. It finds the scale by brute force. It takes up to 8 samples of 32 values and scores every pair on each sample by estimated size: 190 pairs for Float64, 55 for Float32, about 48,600 trial encodings per compressed block. That cost doesn't depend on the block size. TimeSeries samples tables use 256-row granules, and compact parts compress every granule on its own, so on ingest the search runs once per 256 values. Even at 8,192-value blocks it is most of ALP's encode time.

Most pairs are hopeless after a few values: the values turn into exceptions, or the integer range is already too wide. The fix stops scoring a pair as soon as its partial score can no longer beat the best pair so far. The score only grows as values are added, so the stopped pair could never have won. The choice and the stored bytes stay exactly the same.

## Numbers
Dataset: 1M values per data set, 19 data sets. 7 value kinds are modelled on Prometheus metrics: integer counters with resets, histogram bucket counts, 2-decimal random-walk gauges, 0/1 gauges, seconds counters built up in float (binary noise), full-precision doubles, and a 40/20/10/10/10/10 mix. Each comes in two layouts: a **fresh** part (50 requests × 20,000 series, 1 sample per series per row) and a **merged** layout (2,048 consecutive samples per series). The rest are the two sequences of the in-tree perf tests (`rtn`, `rnd`), the earlier baseline's bench sequence, and the test data.

Method: a standalone harness compiles ALP's encoder (master and branch, copied verbatim from the source) with the real bit-packer, and runs the default (`AUTO`) path over blocks of 256 or 8,192 values. It decodes every block back with the unchanged decoder. Calibration against the master build: ALP bytes match within 0.01% at 256-row compact granules and exactly at 8,192-value blocks. Median of 5, 1 thread.

| data set (1M values) | block | Float64 encode, master → branch | Float32 |
|---|---|---|---|
| perf-test `rtn` (3 decimals) | 256 | 1,251 → 451 ms (−64%) | −58% |
| perf-test `rtn` | 8,192 | 46 → 20 ms (−56%) | −40% |
| perf-test `rnd` (random doubles) | 256 | 1,118 → 827 ms (−26%) | −13% |
| fresh part, counters | 256 | 597 → 110 ms (−82%) | −67% |
| fresh part, histogram buckets | 256 | 501 → 73 ms (−85%) | −83% |
| fresh part, 2-decimal gauges | 256 | 589 → 223 ms (−62%) | −64% |
| fresh part, mixed metrics | 256 | 692 → 390 ms (−44%) | −30% |
| fresh part, full-precision doubles | 256 | 584 → 563 ms (−4%) | −24% |
| fresh part, float-accumulated seconds | 256 | 574 → 561 ms (−2%) | −70% |
| merged layout, mixed metrics | 256 | 512 → 192 ms (−62%) | −66% |

- **Compressed bytes are identical in all 76 cases** (19 data sets × 2 block sizes × 2 float types), and every block round-trips bit-exact. A separate sweep over block sizes 1–7,169 found no mismatch either. No case got slower.
- End to end on the master build: the new perf test inserts 2M rows into 256-row compact granules. `ALP, ZSTD(3)` takes 1.53 s (`rtn`) and 1.55 s (`rnd`); `ZSTD(3)` alone takes 0.36 s and 0.20 s. The baseline's ingest micro-benchmark is 990 ms CPU with `ALP, ZSTD(3)` and 535 ms with ZSTD alone. Branch numbers are pending a build.
- Expectation (estimate): ALP's search was ~12% of remote-write CPU in the baseline profile. Typical metric values are counters and gauges, the −60–85% rows, so roughly 6–10% less ingest CPU per sample. It's less if a deployment's values are mostly full-precision doubles.

## Why (cause)
- `estimateParamCandidates` (`src/Compression/CompressionCodecALP.cpp:585-675`) calls `estimateEncodedSize` (`:680-704`) for every (exponent, fraction) pair on every 32-value sample, and always scores all 32 values.
- The score is `values × bit-width of the encoded range + exceptions × 80 bits`, where the bit-width and the exceptions only grow. So once the partial score reaches the best score so far, the pair is out.
- Pairs with too few decimals produce an exception on nearly every value; pairs with too many produce a wide range. Either way they're out after a handful of values.
- The gain is smallest on values that aren't decimals at all (full-precision doubles, float noise). There every pair has many exceptions and the best score stays high; those blocks go to ALP's `RD` variant anyway.

## Proposed fix
- **The change.** Pass the best score so far into `estimateEncodedSize` as a limit, and return as soon as the lower bound reaches it (S, ~15 lines in one function). No setting, no constant, no format change.
- **Why it's exact.** The search keeps a pair only if its score is strictly smaller than the best, so a pair whose lower bound already reaches the best could never be kept. The best score per sample is unchanged, so ALP's `AUTO` decision between its two variants is unchanged too. The per-block choice among the top candidates still scores in full.
- **Links.**
  - Branch: `lilred/perf-alp-search-early-exit` (unbuilt).
  - Compare: https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred/perf-alp-search-early-exit
  - Description: `prs/alp-search-early-exit.md`.
- **Tests.**
  - Stateless `05480_alp_codec_small_compress_blocks`: a round trip, plus the exact compressed size of decimal data at 256-row granules. A deliberately wrong bound changes that size, so the test catches it.
  - Perf test `codecs_float_alp_small_blocks.xml`. The existing `codecs_float_insert.xml` ALP cases should also improve.
- **Review.** ALP is a beta codec behind `enable_alp_codec`, so it needs the codec owners' review.

**Rejected: one sample per 1,024 values for small blocks** (the task's candidate fix).
- Measured: −82 to −87% encode CPU, but compressed size grows up to +4.8% (ALP+ZSTD on the `rtn` sequence) and +1.1% on 2-decimal gauges. A first preflight review asked for changes too.
- Cause: on a 32-value sample, several scale pairs tie, with no exceptions and the same width, and ties go to the smallest exponent, e.g. (3,0). That pair fails to round-trip some values outside the sample. With 8 samples, a majority vote picks the robust pair, e.g. (14,11).
- Neither a strided sample nor a reference-style tie-break (prefer larger exponents) fixed it, and the tie-break also changed large blocks.

**Side finding (format, not fixed).** ALP's bit-packing always reserves room for 1,024 values. So a 256-value block only compresses when its values fit in under ~16 bits (Float64), and most fresh-part blocks of counters, gauges or mixed metrics are stored raw. ZSTD alone is within ±1% of ALP+ZSTD on those sets. Bucket counts and 0/1 gauges still gain 38–50%. Packing only the values present would change the format, so it's out of scope; it's worth raising with the codec owners.

## Upstream overlap
- No open upstream PR touches ALP's search. The open codec PRs are elsewhere: #113575 (Wallaby codec), #109453 (recompress) and #98483 (byte-stream-split).
- Recent ALP history: #109533 (AUTO short-sample bias), #122248 / #117553 (adaptive pool), #120454 (ALP for TimeSeries samples), #125135 (Parquet ALP, a separate encoder that this change doesn't affect).
- James's fork: `dev/timeseries-value-dd-varint-codec` (a new timestamp codec) and `jtcunning/119501-bucket-codecs-master` (nested codecs) don't touch ALP's encoder.

## How to reproduce
The scripts are in `bench/alp-search-early-exit/` on this branch. Raw output is in `results-harness.txt`, and the 76-case table is in `summary.md`.

```bash
R=/path/to/ClickHouse                       # source checkout, branch checked out
git -C $R show origin/master:src/Compression/CompressionCodecALP.cpp > /tmp/alp_master.cpp
./extract.sh /tmp/alp_master.cpp enc_master.h
./extract.sh $R/src/Compression/CompressionCodecALP.cpp enc_branch.h
./extract_decoder.sh /tmp/alp_master.cpp dec_master.h
g++ -std=c++20 -O2 -march=x86-64-v2 -Istub -I$R/src -o harness harness.cpp $R/src/Compression/FFOR.cpp /usr/lib/x86_64-linux-gnu/libzstd.so.1
mkdir -p data && python3 gen.py data
./run_matrix.sh data > results-harness.txt && python3 summarize.py results-harness.txt
```

Calibration against a build (ALP bytes = harness bytes + 25 bytes of block header per granule):

```sql
CREATE TABLE r (v Float64 CODEC(ALP)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 256, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = '1Gi';
INSERT INTO r SELECT v FROM file('fresh_mixed.f64', RowBinary, 'v Float64');
SELECT part_type, marks, data_compressed_bytes FROM system.parts WHERE table = 'r' AND active;
```
