# Stop evaluating an `ALP` parameter pair once it cannot beat the best one

Branch: `lilred-perf-alp-search-early-exit` @ `d6f173c6e3d3` (base `c189bd99fbda`, merges cleanly with master `2b1bef1d3b52`) · compare: https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred-perf-alp-search-early-exit

<!-- upstream:start (the PR body is everything between these markers; the title is the heading above) -->
Related: https://github.com/ClickHouse/ClickHouse/pull/120454
Related: https://github.com/ClickHouse/ClickHouse/pull/129732

### Changelog category (leave one):
- Performance Improvement

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
The `ALP` codec finds its encoding parameters with less work and the same result, so the compressed data does not change. Encoding decimal-like values (counters, gauges, prices) needs much less CPU, most of all in small compressed blocks: an `INSERT` of 2 million values rounded to 3 decimals into a table with 256-row granules is 2.2 times faster.

### Description

`ALP` chooses its exponent/fraction pair by brute force. For every compressed block it scores all 190 pairs (55 for `Float32`) on up to 8 samples of 32 values: about 48,600 trial encodings, whether the block holds 256 values or a million. In small blocks this is most of `ALP`'s encode time. Compact parts compress every granule separately; for example, the `TimeSeries` samples tables use `ALP` with 256-row granules (#120454), and a part written by one remote-write request holds about one sample per row.

Now the search stops scoring a pair as soon as it cannot win. The score, `values × bit-width of the encoded range + exceptions × 80 bits`, only grows as values are added, and only a strictly smaller score replaces the best, so a pair that is cut could never have been chosen. Each sample starts from the score of the previous sample's best pair plus one, so pairs are cut from the first one on and the first pair with the smallest score still wins. The cut-off runs only while the best score is under `RD_SIZE_THRESHOLD_LIMIT` (48 bits per value, where `AUTO` switches to `RD`): above it a pair is cut late or never, and the check per value would only make the vectorized scoring loop scalar. Unchanged: the chosen pairs, the `AUTO` choice, the bytes, `selectBlockParams`.

Release build (clang 21), AMD EPYC 9R14, master `c189bd99fbda` and this PR on one host, two servers pinned to 16 cores, `perf.py` 13 runs per query:

| case | master | this PR | change |
|---|---|---|---|
| `codecs_float_alp_small_blocks` `rtn` (2M values rounded to 3 decimals, 256-row granules) | 0.858 s | 0.389 s | −54.6% |
| same, `rnd` (random doubles) | 0.899 s | 0.895 s | −0.4%, not significant |
| `codecs_float_insert` `ALP` `rtn` | 0.656 s | 0.591 s | −9.9% |
| `codecs_float_insert` `ALP` `seq` / `mon` / `rnd` | 0.422 / 0.683 / 0.557 s | 0.414 / 0.681 / 0.551 s | −1.9% / −0.3% / −1.0%, not significant |

Controls: the 24 rows of the other codecs in `codecs_float_insert` run no changed code and moved −7.6% to +6.4% (20 of them not significantly), which is the noise of this pair of builds; no `ALP` row is slower. Random doubles score above `RD_SIZE_THRESHOLD_LIMIT`, so they keep master's loop and their time does not change. The encoder alone, both versions built with clang 20 at the build's flags, 1M values per case: `Float64` decimal-like data −38% to −93% CPU, full-precision doubles −1% to −9%, `Float32` −6% to −88%; nothing slower.

The bytes are identical to master in all 76 encoder cases (17 data sets in 256- and 8,192-value blocks and the 4 `codecs_float_insert` sequences, both types) and in 300,000 random blocks with NaN, ±inf, −0, subnormals and values near the `Int64` limits. `05480_alp_codec_small_compress_blocks` pins a round trip and four compressed sizes and passes on master and on this PR: a cut at half the limit changes all four, counting 64 bits before any value encodes changes the NaN one (167,720 → 202,600 bytes), and starting without the `+ 1` changes the shared-pair one (216,123 → 205,883). `04612_alp_codec_float32_decimal_ratio` and `05054_adaptive_codec_float_alp` pass. New perf test, because no existing file has a 256-row compact table.

`ALP` became GA in #129732; its owners should review (earlier search work: #109533, #122248).
<!-- upstream:end -->

<details><summary>Run 1 and 2, harness tables, A/B method and self-review (not for upstream)</summary>

**Run 2 (head `d6f173c6e3d3`, James's A/B 2026-11-10, EPYC 9R14, clang 21 release, perf.py 13 runs): pass** (https://github.com/JTCunning/ClickHouse/issues/11#issuecomment-6774261119). Every `pass_if` line holds: 05480 passes on A and B with the four pinned sizes; 04612 and 05054 pass on B; small-block `rtn` −54.6% (prediction about −45%), `rnd` −0.4% (n.s.); `codecs_float_insert` `ALP` `rtn` −9.9%, `seq` −1.9%, `mon` −0.3%, `rnd` −1.0% (n.s.); slowest unchanged codec `FPC` `rtn` +6.4%, fastest `ZSTD` `rnd` −7.6%. Not run: 05480 on a debug or sanitizer build (the `chassert`; upstream CI covers it), and the plan's older cases (500-part bench, `SELECT` guard, edge `INSERT`s, part checksums), which run 2's plan section did not ask for.

**Run 1 (head `d4c58e0009b4`, James's A/B 2026-11-10, EPYC 9R14, clang 21 release, perf.py 13 runs): fail.** Small blocks were faster (`rtn` 0.859 → 0.579 s, −32.6%; `rnd` 0.902 → 0.755 s, −16.2%), but two `codecs_float_insert` `ALP` rows were significantly slower: `mon` 0.708 → 0.781 s (+10.2%), `seq` 0.447 → 0.465 s (+4.0%); `rnd` −9.1%, `rtn` −6.5%. Unchanged codecs moved −17.1% to +6.2% in the same run (binary/order effect).

**Cause.** clang vectorizes master's `estimateEncodedSize` loop (4 values per AVX2 instruction at x86-64-v3). The per-value cut-off of `d4c58e0009b4` has an early exit, so that loop became scalar. On values that are not decimals (`seq` = `number / pi()`, `mon` = `number + sin(number)`) the best score of a sample stays above 48 bits per value, a pair is cut after most of the sample at best, and the scalar loop cost more than it saved. My first harness used g++ 13 `-O2 -march=x86-64-v2`, which vectorizes neither version, so it showed −2% to −9% there instead. Rebuilt with clang 20 at ClickHouse's flags, `d4c58e0009b4` reproduced the direction: `mon` +3.8% encode CPU, full-precision and float-accumulated values at 256-value blocks +15–17%.

**Variants tried** (clang harness, encode CPU vs master; bytes identical in all):

| variant | `fullprec` 256 | `rtn` 256 | `mon` 8,192 | `rtn` 8,192 |
|---|---|---|---|---|
| per-value cut-off (`d4c58e0009b4`) | +17% | −52% | +4% | −36% |
| check every 4 / 8 / 16 values (chunked loop) | — | — | +35% / +13% / +7% | −27% / −34% / −27% |
| check the first 4 / 8 / 16 values, then master's loop | +5% / +10% / +22% | −38% / −70% / −71% | +5% / +10% / +19% | −18% / −32% / −38% |
| seed from the previous sample's best pair | +16% | −72% | −1% | −40% |
| **seed + cut-off only under `RD_SIZE_THRESHOLD_LIMIT` (this branch)** | **−3%** | **−71%** | **−5%** | **−38%** |

**Harness, this branch** (`32eb990cdbb9`'s encoder extracted verbatim, clang 20 `-O3 -march=x86-64-v3 -ffp-contract=off -fno-math-errno`, 2 vCPU Xeon, median of 5; ms per 1M values, the `ins_*` rows per 20M):

| data set | block | `Float64` master → branch | `Float32` |
|---|---|---|---|
| perf-test `rtn` | 256 | 894 → 262 (−71%) | −54% |
| perf-test `rnd` | 256 | 947 → 909 (−4%) | −10% |
| fresh part, counters | 256 | 443 → 107 (−76%) | −12% |
| fresh part, 2-decimal gauges | 256 | 447 → 128 (−71%) | −63% |
| fresh part, mixed metrics | 256 | 449 → 392 (−13%) | −11% |
| merged layout, mixed metrics | 256 | 445 → 170 (−62%) | −62% |
| fresh part, full-precision doubles | 256 | 467 → 452 (−3%) | −9% |
| merged layout, float-accumulated seconds | 256 | 463 → 459 (−1%) | −58% |
| `codecs_float_insert` `seq` (20M) | 8,192 | 350 → 319 (−9%) | −36% |
| `codecs_float_insert` `mon` (20M) | 8,192 | 366 → 347 (−5%) | −36% |
| `codecs_float_insert` `rnd` (20M) | 8,192 | 444 → 435 (−2%) | −7% |
| `codecs_float_insert` `rtn` (20M) | 8,192 | 375 → 232 (−38%) | −35% |

Price of the gate: random doubles (`rnd`) sit above the line, so they keep master's loop; the run-1 small-block `rnd` gain (−16% end to end) shrinks to about −2%.

**A/B method (run 2):** `perf.py` from the branch's tree against two servers on one host, pinned to cores 0-15, 13 runs per query. A = run 1's master binary (`c189bd99fbda`, `GIT_HASH` checked), B = a new build of `d6f173c6e3d3` (`GIT_HASH` checked).

```bash
tests/performance/scripts/perf.py --runs 13 tests/performance/codecs_float_alp_small_blocks.xml
tests/performance/scripts/perf.py --runs 13 tests/performance/codecs_float_insert.xml
```

**Self-review (preflight against the review bot's instructions at master `303cf081`):**
- 2026-11-01, rounds 1–2 (old approach, then `d4c58e0009b4`): see the git history of this file.
- 2026-11-10, round 1 (`bd592b96a5d7`): ⚠️ Request changes. The reviewer confirmed the change is exactly equivalent to a full search, with its own 80,000-block harness (0 mismatches) and an encoder model that reproduces the three pinned sizes. Findings:
  - Major: the end-to-end numbers are still pending. They need run 2.
  - Major: no pinned size covers a sample whose first value is an exception, where the bound must count 0 bits for the range. Fixed in `ded70dddecf1` with the NaN section, 167,720 bytes from a master build; a `calculateEncodeBits` bound gives 202,600 bytes in my harness.
  - Nit: add `chassert(occurred_times == 1)`. Done.
  - Nit: "TimeSeries 256-row granules" overstated the effect, because merged parts hold full buckets. The changelog and the perf-test comment now say that a part from one remote-write request holds about one sample per row (`TimeSeriesSink.cpp:460-517`, confirmed in round 2).
  - The changelog's "up to 70%" had no end-to-end source and is removed until run 2.
- 2026-11-10, round 2 (`ded70dddecf1`): ⚠️ Request changes, only because the numbers are still pending. One Nit: the NaN comment said the search must not cut pairs before a value encodes, but it does, by their exceptions alone. Reworded in `d6f173c6e3d3`, a comment-only change, so no third round. After run 2 (2026-11-13): branch column, controls and T1's number in the changelog entry filled in, `[unbuilt]` dropped.
- 2026-11-13, round 3 (final text and the diff at `d6f173c6e3d3`, review prompt at master `d09276d883d5`, which adds the Preconditions gate and puts unchanged code the inputs reach in scope): ✅ Approve. The reviewer re-derived the exact equivalence by hand (the start is above the best size, a cut pair can never win, the first best pair is never cut), checked the memory-safety preconditions (sample buffers of at most 32 values, exponent/fraction indexes under 19 from every producer, the `chassert` safe even if violated) and the unchanged encoders and decoders the inputs reach: no finding. One optional Nit, not taken: 05480 pins sizes only for `Float64`; a `Float32` size pin would cover the 55-pair search's own constants. The proof does not depend on them, and a test change would need another build, so it stays out unless a reviewer asks.
- The obsolete `enable_alp_codec` (a no-op on master since #129732) stays in 05480 and the perf XML, because the base still needs it.
</details>
