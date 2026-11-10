# A/B plan: stop scoring an ALP parameter pair once it cannot beat the best one
**Experiment:** ../reports/2026-11-01-alp-search-early-exit.md · **Branch:** `lilred-perf-alp-search-early-exit` @ `d6f173c6e3d3` (run 2; run 1 was `d4c58e0009b4`; based on master `c189bd99fbda`; merges cleanly with master `2f701ba6d627`, 2026-11-10) · **Task:** 2026-10-31-alp-codec-scale-the-parameter-search-to · **Outbox:** item 5, issue #11 (`ready-to-build`, run 2)
**Claim:** `ALP` encode CPU drops 40–90% on decimal-like values and 2–26% on full-precision ones, with byte-identical output. End to end, the new perf test's `INSERT` of 2M rows into 256-row compact granules with `ALP, ZSTD(3)` goes from 1.53 s to about 0.8 s (`rtn`, −45–50%) and from 1.55 s to about 1.2 s (`rnd`, −20–25%).
**Evidence so far:** a standalone harness that compiles master's and the branch's encoder verbatim (76 cases, bytes identical in all, calibrated against a master build within 0.01%), plus master-only end-to-end `INSERT` timings on 2 vCPU. Unproven: the branch on a real build, the end-to-end share of the search (the predictions above assume the `ALP, ZSTD(3)` minus `ZSTD(3)` time is all `ALP` encode), and the effect on TimeSeries ingest.

## Run 2 (head `d6f173c6e3d3`), 2026-11-10
Run 1 (`d4c58e0009b4`, results on #11) failed: `codecs_float_insert` `ALP` `mon` +10.2% and `seq` +4.0%. The cause was a scalar scoring loop (see the report's correction). Run 2 checks the fix: the cut-off only while the sample's best score is under `RD_SIZE_THRESHOLD_LIMIT`, master's vectorized loop above it, and each sample seeded from the previous sample's best pair.

- **Builds:** A = `c189bd99fbda` again (the base is unchanged, so run 1's A binary can be reused), B = `d6f173c6e3d3`. Same host, method and `perf.py --runs 13` as run 1.
- **Tests on B:** `05480_alp_codec_small_compress_blocks`, which now has 4 pinned sizes (3-decimal 159,770, 2-decimal 145,000, NaN at every sample start 167,720, shared best pair 216,123 bytes). Also `04612_alp_codec_float32_decimal_ratio` and `05054_adaptive_codec_float_alp`. 05480 must also pass on A, because the bytes are master's. I took the new reference lines from a master build (26.11.1.1642, same encoder as the base).
- **Debug or sanitizer build, if one is at hand:** run 05480 on it once. The new `chassert(best_estimation.occurred_times == 1)` only fires there.
- **Expected (encoder-alone harness under clang 20, scaled by run 1's share of `ALP` in each `INSERT`):**

| case | run 1 (`d4c58e0009b4`) | run 2 prediction |
|---|---|---|
| T1 `codecs_float_alp_small_blocks` `rtn` | −32.6% | about −45% (encode −71% vs run 1's −52%) |
| T2 same, `rnd` | −16.2% | about −2%, maybe not significant (random doubles keep master's loop now) |
| T4 `codecs_float_insert` `ALP` `rtn` | −6.5% | faster, similar to run 1 (encode −38% vs −36%) |
| G1 `codecs_float_insert` `ALP` `seq` / `mon` / `rnd` | +4.0% / +10.2% / −9.1% | faster or unchanged (encode −9% / −5% / −2%) |
| G2 the unchanged codecs of `codecs_float_insert` | −17.1% to +6.2% | the noise band of this run |

- **Pass:**
  - every listed test passes on B, and 05480 also on A;
  - T1 is at least 30% faster;
  - T2 is not significantly slower;
  - T4 is significantly faster;
  - no `ALP` row of `codecs_float_insert` (G1, T4) is slower than the slowest unchanged-codec row (G2) of the same run.

  Run 1 showed shifts of up to 17% in code this change does not touch, so a G1 row inside that band is noise, not a regression.
- **Fail, G1 still slower beyond the band:** the gate does not hold on Zen 4. Send the `perf annotate` of `estimateParamCandidates` on B for the slow row if you can; otherwise I drop the branch.
- **Changelog number:** after a pass, I put T1's end-to-end change into the changelog entry. It has no number now.

## Builds
- A: master at `c189bd99fbda` (the branch base). B: `lilred-perf-alp-search-early-exit` @ `d4c58e0009b4`. Master's only `CompressionCodecALP.cpp` change since the base (`bc9f38691549`, `FirstApplicable`) touches codec registration, not the encoder, so rebasing is optional; if you rebase, build A at the new base and record both shas.
- Release build, no sanitizers, as in the CI performance job.

## Machine
- 4 vCPU / 8 GiB is enough: every target query runs with `max_threads = 1` and the claim is CPU per compressed block, not parallelism. The 16-core pinned host used for items 1–3 is fine and keeps runs comparable. Pin A and B to separate cores, nothing else running, note the CPU model (the harness ran on an AVX-512 Xeon; the bit-packer is vectorized, so the ratio may move a little on other CPUs).

## Data
- Generated inside each query from `system.numbers` (no data directory to copy):
  - `rtn`: `round(number / 20 + sin(number), 3)`, 3-decimal values (the encoder's best case among the in-tree sequences)
  - `rnd`: `(intHash64(number) - 4294967295) / pi()`, full-precision doubles (most pairs stay close, so few early exits)
  - gauge shape of the remote-write baseline: `100 + (number % 1000) * 0.25`, 1M values as 500 parts of 2,000 rows (`bench/remote-write-baseline/alp_granule_bench.sh`)
- Sizes: 2M rows at 256-row granules (the TimeSeries samples-table shape), 20M rows in wide parts (`codecs_float_insert.xml`, about 8,192 values per compressed block), and the 500-part shape above.

## Cases
| case | query | role | predicted A → B |
|---|---|---|---|
| T1 | `codecs_float_alp_small_blocks.xml`, `rtn` (branch's new perf test) | target | 1.53 → ~0.8 s (−45–50%) |
| T2 | same, `rnd` | target | 1.55 → ~1.2 s (−20–25%) |
| T3 | `alp_granule_bench.sh`, `alp` (256-row granules, 500 parts of 2,000 rows) | target | 990 → ~620–700 ms CPU (−30–37%) |
| T4 | `codecs_float_insert.xml`, `ALP` × `rtn` (20M rows, wide part) | target | faster, ≥ 10% (harness: −56% of encode at 8,192 values; insert share unmeasured) |
| G1 | `codecs_float_insert.xml`, `ALP` × `seq` / `mon` / `rnd` | guard | faster or unchanged; harness `rnd` at 8,192: −18% of encode |
| G2 | `codecs_float_insert.xml`, every non-`ALP` codec (`NONE`, `LZ4`, `ZSTD`, `DoubleDelta`, `Gorilla`, `FPC`) | guard / A-A control | unchanged (code not touched; bounds the noise) |
| G3 | `codecs_float_alp.xml` (`SELECT` over `ALP` `AUTO`/`STD`/`RD`, `Float32`/`Float64`, `rtn`/`rnd`) | guard | unchanged (decoder not touched; fills write the same bytes) |
| G4 | `alp_granule_bench.sh`, `alp8192` and `zstd` | guard | `zstd` unchanged; `alp8192` 572 → ~545 ms (−3–6%, may be under the noise floor) |
| E1 | T2 with `n Float32` (`CREATE TABLE ... (n Float32 CODEC(ALP, ZSTD(3)))`, same `INSERT`) | edge | faster or unchanged (harness: −13%); the pair loop has one extra comparison per value, and this is where it exits least |
| E2 | `INSERT ... SELECT number * 1e-9 + intHash64(number) / 18446744073709551615 FROM numbers(2000000)` (deterministic, so A and B write comparable bytes) into the T1 table shape (no decimal structure, goes to `RD`) | edge | unchanged within ±3% (harness worst case: −2%) |
| O1 | remote-write ingest, `bench/remote-write-baseline/run.sh` with the baseline's 1-sample-per-series shape, against a TimeSeries table with default codecs | observe only | server CPU per sample −6–10% (estimate from the ~12% `ALP` share in the baseline profile); no pass/fail |

## Method
- Two servers (A on port 9001, B on 9002), same config, `enable_alp_codec = 1`, merges stopped in every case (the XMLs already do it).
- In-tree XMLs (T1, T2, T4, G1–G3): `tests/performance/scripts/perf.py --runs 13` from B's tree against both servers, then `compare_perf_tsv.py` with its defaults, as for items 1–3.
- T3, G4: run `alp_granule_bench.sh` against each server in turn (`CH=<binary>`, port changed), A B A B, 5 rounds each; report the median `UserTime + SystemTime` from `query_log`.
- E1, E2: copy `codecs_float_alp_small_blocks.xml` to `bench/alp-search-early-exit/ab/alp_small_blocks_edge.xml` with the two edge `INSERT`s as its queries (its `create_query`/`drop_query` already make repeated `INSERT`s comparable) and run it through `perf.py` like the in-tree files. Not for upstream.
- O1: one run of 5 minutes per server, after the others, if time allows.

## Metrics
- Wall time (median, p95) and `ProfileEvents['UserTimeMicroseconds'] + ['SystemTimeMicroseconds']` per `INSERT`, from `system.query_log`.
- Bytes: `sum(data_compressed_bytes)` from `system.parts` per table, and `column_data_compressed_bytes` from `system.parts_columns`.
- No dedicated profile event proves the early exit; the byte check below plus the CPU drop on T1 and the flat E2 do.

## Correctness
- **Byte identity, the core of the claim.** For T1, T2, E1, E2 and the `ALP` tables of `codecs_float_insert.xml`, insert once on each server with merges stopped and `max_threads = 1`, then compare `sum(data_compressed_bytes)` per table A vs B (must be equal), and the `md5sum` of each part's `n.bin` (`find <data path>/store -name 'n.bin' | sort | xargs md5sum`, after mapping parts by name). Any difference fails the plan.
- Values: `SELECT sum(cityHash64(n)) FROM <table>` equal on A and B, and equal to the same expression over the source `SELECT`.
- Tests on B: `05480_alp_codec_small_compress_blocks` (exact sizes, including the 3-decimal 159,770 bytes), `04612_alp_codec_float32_decimal_ratio`, `05054_adaptive_codec_float_alp`. 05480 passes on A too (the output must be the same); that is expected, its job is to catch a wrong bound later.

## Pass / fail
- **Pass:** T1 at least −30%, T2 at least −10%, T3 at least −20% CPU, T4 faster and significant; byte identity holds everywhere; no G or E case significantly slower; G2 within the noise (its spread is the floor for the rest).
- **Fail, bytes differ:** the bound is not exact. Withdraw the branch, re-run the harness's block-size sweep on the failing input, and update the report.
- **Fail, T1/T2 miss but bytes match:** the search is a smaller share of `INSERT` than the `ALP, ZSTD(3)` minus `ZSTD(3)` estimate (other `ALP` encode work, for example the bit-packing of 1,024-value frames from the report's side finding). Record the measured share, lower the claim in `prs/alp-search-early-exit.md`, and keep the branch if T3 and T4 still pass.
- **Fail, E1 or E2 slower:** the extra comparison costs more than it saves when nothing exits early; check the generated code of `estimateEncodedSize` on B before changing the branch.

## Checklist
- [ ] build A and B; record both shas
- [ ] run every case; save the raw output under `bench/alp-search-early-exit/ab/`
- [ ] byte identity: the per-table sizes and `n.bin` checksums, A vs B
- [ ] fill the branch column of `prs/alp-search-early-exit.md`, remove "pending build" and `[unbuilt]`
- [ ] update the report verdict and the backlog row
