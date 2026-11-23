# lil red's outbox

What James does for lil red, in order. When a move is done, its row leaves this list: a submitted item moves to **Submitted**, and an item lil red is working on moves to **With lil red**.

Generated from [`index.json`](index.json) by `render.py`; don't edit this file by hand. Updated 2026-11-23 15:39 UTC.

## For you, in order

| # | your move | item | issue | what it's for |
|---|---|---|---|---|
| 1 | **Submit**: `submit range-json-timestamp-text` | [range-json-timestamp-text](../prs/range-json-timestamp-text.md) | [#22](https://github.com/JTCunning/ClickHouse/issues/22) | built: /api/v1/query_range over 2,000 series -10.2% (m, step 15 s), rate() -5.3%, every-other-step -8.3%; skewed block and control flat; same bytes |
| 2 | **Decide** | [rollup-samples](../prs/rollup-samples.md) | [#16](https://github.com/JTCunning/ClickHouse/issues/16) | Run 2 met 1 of 5 floors (my predictions); results equal, setting off = master. A (rec.): submit with run 2's numbers, no A/B rerun: you re-run only 05842 (test fix 520aade, binary unchanged), or one tests-only build if you also want the rename (rec. yes). B: park. C: one more rework + full A/B build. Answer on #16. |
| 3 | **Decide** | [time-ranges-metric-filter](../prs/time-ranges-metric-filter.md) | [#17](https://github.com/JTCunning/ClickHouse/issues/17) | Blocked on upstream #115482 (idle since 2026-10-19, conflicts with master). Post the drafted nudge on #115482 (recommended), have someone take it over, or park this. Also OK the +6..11% broad-regex cost into the A/B? Answer on #17; no build is possible until then. |
| 4 | **Decide** | recent-samples-merge | [#25](https://github.com/JTCunning/ClickHouse/issues/25) | A (recommended): run the A/B plan at scale (no build: two table definitions on one master build + a steady state with the mover); if it passes I branch a version-12 default (`tier` column, recent off) and an engine-side mover. B: keep both tables and only cut the duplicate's cost (shorter recent TTL, pick the table by query shape). C: drop it. Answer on the issue. |
| 5 | **Decide** | [ts-from-grid-typed-write](../prs/ts-from-grid-typed-write.md) | [#26](https://github.com/JTCunning/ClickHouse/issues/26) | Before you submit: open the upstream issue drafted in #26's description (two pre-existing defects on this path: NULL start/end/step computed as 0, and a DateTime/UInt32 start with a DateTime64 end after 2106 wrapping to 1970; both reproduced on master) and put its link in the placeholder Related: line. Doesn't block the build. Answer on #26. |
| 6 | **Build and A/B**: `ab ts-from-grid-typed-write` | [ts-from-grid-typed-write](../prs/ts-from-grid-typed-write.md) | [#26](https://github.com/JTCunning/ClickHouse/issues/26) | run 2 needed: dashboard PromQL queries via /api/v1/query_range (run 1: function 2x/5x, but PromQL range query only -0.6..-2.4%, not significant) |

Order: review fixes to sync first (an upstream reviewer is waiting), then submits, then decisions, then builds. Within each, lil red's `priority` order. A move is a command for the `lil-red-outbox` skill; answer a decision on the item's issue.

## With lil red: nothing for you

None.

## Submitted

| item | upstream | issue | what it does |
|---|---|---|---|
| [promql-shared-selector-sets](../prs/promql-shared-selector-sets.md) | PR 130402 | [#5](https://github.com/JTCunning/ClickHouse/issues/5) | x + x −15.5%, x + x + x −21.8% (run 3): a repeated selector's id set is built once per query |
| [promql-drop-name-exact-selector](../prs/promql-drop-name-exact-selector.md) | PR 126763 | [#8](https://github.com/JTCunning/ClickHouse/issues/8) | sum(rate) −19% CPU at 100k series: no duplicate-series check when the metric name of one metric is dropped |
| [time-ranges-id-ranges-index-hint](../prs/time-ranges-id-ranges-index-hint.md) | PR 129064 | [#6](https://github.com/JTCunning/ClickHouse/issues/6) | 100-name selector −19.9% (176 → 141 ms); 50–300 names −35–39% (build, 2026-11-01) |
| [alp-search-early-exit](../prs/alp-search-early-exit.md) | PR 131497 | [#11](https://github.com/JTCunning/ClickHouse/issues/11) | run 2 passed: small-block INSERT of decimal-like values 2.2x faster (-54.6%), codecs_float_insert ALP rtn -9.9%, nothing slower; same bytes |
| [ts-group-array-result-copy](../prs/ts-group-array-result-copy.md) | PR 133502 | [#23](https://github.com/JTCunning/ClickHouse/issues/23) | built: samples-table merge CPU -12.1%, FINAL -11..-14%, GROUP BYs -13..-15%, optimize_on_insert INSERT -7%; controls flat, same results (under my predicted floors; judged against the 5% bar) |
| [remote-write-arena](../prs/remote-write-arena.md) | PR 133485 | [#24](https://github.com/JTCunning/ClickHouse/issues/24) | built: remote-write CPU per sample -9.5..-9.9% (merges stopped, 10/20 labels, long values too), -3.8% with merges running; parse+destroy 12.9% -> 4.0% of server CPU |

## Dropped

| item | issue | what it was |
|---|---|---|
| [grid-gap-wraparound](../prs/grid-gap-wraparound.md) | [#10](https://github.com/JTCunning/ClickHouse/issues/10) | parked: James chose upstream 124777 (dense array) as the fix for 125759; run 2 was 8.4x / 5.7x on the gap but the rule kept misfiring |
| [parallel-statistics-probe](../prs/parallel-statistics-probe.md) | [#15](https://github.com/JTCunning/ClickHouse/issues/15) | dropped: upstream #131667 (merged 2026-11-15) is the same fix; your run 1 was -6.1% on a window with 2 of the 13 serial index loads |
| [selector-one-tags-row-per-id](../prs/selector-one-tags-row-per-id.md) | [#18](https://github.com/JTCunning/ClickHouse/issues/18) | dropped 2026-11-17: built T1 -13% at 2 threads but -5% at 8 (bar 10%); merged wide +13% at 2 threads; the cost is the part-stream merge (#19) |
| [selector-ids-fixed-join-order](../prs/selector-ids-fixed-join-order.md) | [#20](https://github.com/JTCunning/ClickHouse/issues/20) | dropped after your build: cold -6.8..-17%, but prewarmed +15..+40% (99.5%); the loss doesn't reproduce on 2 vCPU |

## How an item moves

1. `ready-to-build`: James's Cursor skill builds the item's `base` and `head` commits, runs the listed tests and the A/B, and posts the results on the item's issue.
2. `results-in`: lil red checks the results against `pass_if` and writes the real numbers into the PR text. It drops `[unbuilt]` and runs its review-bot preflight again.
3. `ready-to-submit`: James's skill submits it. That means a branch `jtcunning-<slug>` and an upstream PR whose body is the text between the `<!-- upstream:start -->` and `<!-- upstream:end -->` markers of `prs/<slug>.md`.
   - `needs-work`: lil red is reworking or dropping the item instead.
4. `submitted`: done. The skill's `<!-- lil-red-outbox:submitted -->` comment sets it. lil red also sets it on its own when it finds an upstream PR from `jtcunning-<slug>`, or a PR that James linked on the item's issue; it checks every day at 02:00 PT, and at the start of each 03:00 and 15:00 PT work run. If review feedback comes in, the skill posts it on the issue. lil red pushes fixes to its branch (`fixes-ready`), and the skill syncs them into `jtcunning-<slug>`.

A **decision** (the item's `decision` field) is a question for James that lil red asked on the item's issue. It stays in the list until lil red reads his answer.

Statuses:

- `ready-to-build`: Build base and head, run the tests and the A/B, post the results on the issue.
- `results-in`: James posted results; lil red is turning them into the final PR text. Nothing to do.
- `needs-work`: The results missed the pass rule or a test failed; lil red is reworking the branch or dropping it. Nothing to do.
- `ready-to-submit`: The PR text has real numbers and passed lil red's review-bot preflight. James can submit.
- `fixes-ready`: Submitted upstream; lil red pushed new commits for review feedback. Sync them into James's branch.
- `submitted`: James opened the upstream PR. Nothing to do unless review feedback comes in.
- `dropped`: Not worth submitting; see the issue.

Only lil red edits this directory. James's skill reports through comments on the item's issue. Each comment starts with a marker that lil red's intake reads: `<!-- lil-red-outbox:results -->`, `<!-- lil-red-outbox:submitted -->` or `<!-- lil-red-outbox:feedback -->`.
