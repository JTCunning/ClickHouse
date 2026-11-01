# [unbuilt] Stop evaluating an `ALP` parameter pair once it cannot beat the best one

Branch: `lilred-perf-alp-search-early-exit` · compare: https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred-perf-alp-search-early-exit

<!-- upstream:start (the PR body is everything between these markers; the title is the heading above without [unbuilt]) -->

`ALP` chooses its exponent/fraction pair by brute force. For every compressed block it takes up to 8 samples of 32 values and computes the estimated encoded size of every pair on each sample: 190 pairs for `Float64`, 55 for `Float32`, so about 48,600 trial encodings per block, whether the block holds 256 values or a million. This search is most of `ALP`'s encode time even at 8,192-value blocks. It dominates with small blocks: compact parts compress every granule separately, and the inner samples tables of `TimeSeries` use `ALP, ZSTD(3)` with `index_granularity = 256`.

The estimate is `values × bit-width of the encoded range + exceptions × exception size`. The bit-width and the exception count only grow as values are added, so the estimate over the values seen so far is a lower bound of the final one. `estimateEncodedSize` now gets the best estimate of the sample so far as a limit, and returns as soon as the lower bound reaches it. Such a pair could never be chosen, because only a strictly smaller estimate replaces the best one. The chosen parameters, the `AUTO` choice between `STD` and `RD`, and the compressed bytes are exactly the same as before. Only the search in `estimateParamCandidates` uses the limit; the per-block choice among the candidates (`selectBlockParams`) still evaluates in full.

### Evidence

`ALP` encode CPU, standalone harness. It compiles the encoder half of `src/Compression/CompressionCodecALP.cpp` from master and from this branch, copied verbatim, with the real `FFOR.h`. It runs the `DEFAULT`/`AUTO` path of `doCompressData` over 1M values cut into compressed blocks of N values, and decodes every block back with the unchanged decoder. Its `ALP` output size matches a real master build (26.11.1.974) within 0.01% at 256-row compact granules and to the byte at 8,192-value wide-part blocks. Host: 2 vCPU Xeon (AVX-512), g++ 13 `-O2`, 1 thread. Base sha c189bd99fbda. Median of 5.

**Compressed bytes are identical in all 76 cases**: 19 data sets × blocks of 256 / 8,192 values × `Float64` / `Float32`. The round trip is bit-exact in all of them and in a sweep of block sizes 1, 31, 33, 255, 1,025, 7,168 and 7,169. No case is slower.

| data set (1M values) | block | `Float64` master → branch | `Float32` master → branch |
|---|---|---|---|
| perf-test sequence `rtn`: `round(number / 20 + sin(number), 3)` | 256 | 1,251 → 451 ms (−64%) | 360 → 151 ms (−58%) |
| same | 8,192 | 46 → 20 ms (−56%) | 17 → 10 ms (−40%) |
| perf-test sequence `rnd`: `(intHash64(number) - 4294967295) / pi()` | 256 | 1,118 → 827 ms (−26%) | 341 → 297 ms (−13%) |
| same | 8,192 | 52 → 43 ms (−18%) | 19 → 18 ms (−8%) |
| integer counters, fresh part (1 sample per series) | 256 | 597 → 110 ms (−82%) | 185 → 61 ms (−67%) |
| 2-decimal gauges, fresh part | 256 | 589 → 223 ms (−62%) | 173 → 63 ms (−64%) |
| mixed Prometheus metrics, fresh part | 256 | 692 → 390 ms (−44%) | 227 → 160 ms (−30%) |
| mixed Prometheus metrics, fresh part | 8,192 | 29 → 20 ms (−33%) | 14 → 12 ms (−15%) |
| full-precision doubles (go to `RD`) | 256 | 584 → 563 ms (−4%) | 208 → 157 ms (−24%) |
| float-accumulated seconds (binary noise) | 256 | 574 → 561 ms (−2%) | 175 → 52 ms (−70%) |

The gain is smallest on values that are not decimals, because every pair then has many exceptions and the best estimate stays high. The full table of 76 cases is `summary.md` and the raw output is `results-harness.txt`.

End to end on a real build. The `INSERT` runs on a compact `MergeTree` table with merges stopped and `max_threads = 1`, median of 5 warm runs, master 26.11.1.974:

| case | master | branch |
|---|---|---|
| perf test `codecs_float_alp_small_blocks`, `rtn` (2M rows, 256-row granules, `ALP, ZSTD(3)`) | 1.53 s (1.03–1.76) | pending build |
| same, `rnd` | 1.55 s (1.07–1.95) | pending build |
| same tables with `ZSTD(3)` only, `rtn` / `rnd` | 0.36 s / 0.20 s | — |
| 1M values as 500 parts of 2,000 rows, `ALP, ZSTD(3)`, 256-row granules | 990 ms CPU | pending build |
| same, 8,192-row granules (blocks of 2,000 values) | 572 ms CPU | pending build |

Commands for the branch column:

```bash
tests/performance/scripts/perf.py --runs 7 tests/performance/codecs_float_alp_small_blocks.xml
tests/performance/scripts/perf.py --runs 7 tests/performance/codecs_float_insert.xml    # the ALP cases should also improve: blocks of 8,192 values
```

### Tests

- `05480_alp_codec_small_compress_blocks`: compact parts with 256-row granules, so `ALP` compresses blocks of 256 values.
  - A bit-exact round trip for `ALP`, `ALP(STD)` and `ALP` on `Float32`, with values that encode, become exceptions, or fall back to `RD` or raw.
  - The exact compressed size of 3-decimal and 2-decimal data. Several pairs encode a sample of such values without exceptions, but only some round-trip every value, so a bound that cut the search too early would change the size. A mutant that stops at half the limit changes the 3-decimal size from 159,770 to 160,030 bytes.
  - The table settings pin `min_compress_block_size` / `max_compress_block_size`, so that randomized session settings cannot change the block boundaries.
- New perf test `codecs_float_alp_small_blocks.xml`: `INSERT` into a compact table with 256-row granules and `ALP, ZSTD(3)`. The existing `codecs_float_insert.xml` covers `ALP` with wide-part blocks.
- The existing `ALP` tests are unaffected by construction, because the output bytes are the same. That includes the size-dependent ones: `04612_alp_codec_float32_decimal_ratio` and `05054_adaptive_codec_float_alp`.

`ALP` is a beta codec behind `enable_alp_codec`; codec owners should review (recent work on the search: https://github.com/ClickHouse/ClickHouse/pull/109533, https://github.com/ClickHouse/ClickHouse/pull/122248).

Related: https://github.com/ClickHouse/ClickHouse/pull/120454

### Changelog category (leave one):
- Performance Improvement

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
The `ALP` codec chooses its encoding parameters with less work and the same result, so the compressed data is unchanged. The `ALP` encoder uses roughly 35–90% less CPU on decimal-like values, most of all with small compressed blocks such as the 256-row granules of compact parts and `TimeSeries` tables.

<!-- upstream:end -->

<details><summary>Self-review (lil red, 2026-11-01)</summary>

Two preflight rounds against `.claude/skills/review/SKILL.md`.
- Round 1 reviewed an earlier approach: one sample per 1,024 values for small blocks. Verdict: request changes, because the sampling trade-off lost compression ratio (up to +4.8% on `rtn`). I measured that, rejected the approach, and replaced it with this exact early exit.
- Round 2 found the code exactly equivalent to the full evaluation, verified by its own fuzz test (300k random samples per type, including NaN, ±inf, -0 and values near the Int64 limits). Verdict: request changes, for the end-to-end numbers only. The open item is the branch column above, which needs a build or the CI perf report. The two nits are fixed: the in-code note on strict `<` and the changelog wording.
- Remove the `[unbuilt]` title prefix once the numbers are in.
</details>
