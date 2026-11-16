# lil red's outbox

What James does for lil red, in order. When a move is done, its row leaves this list: a submitted item moves to **Submitted**, and an item lil red is working on moves to **With lil red**.

Generated from [`index.json`](index.json) by `render.py`; don't edit this file by hand. Updated 2026-11-16 00:57 UTC.

## For you, in order

| # | your move | item | issue | what it's for |
|---|---|---|---|---|
| 1 | **Decide** | [rollup-samples](../prs/rollup-samples.md) | [#16](https://github.com/JTCunning/ClickHouse/issues/16) | Rate-type functions: you chose A (exact-only). Still open: rename `time_series_prefer_rollup_tables` to `..._columns`? Answer on #16; it does not block the build. |
| 2 | **Decide** | [time-ranges-metric-filter](../prs/time-ranges-metric-filter.md) | [#17](https://github.com/JTCunning/ClickHouse/issues/17) | Blocked on upstream #115482 (idle since 2026-10-19, conflicts with master). Post the drafted nudge on #115482 (recommended), have someone take it over, or park this. Also OK the +6..11% broad-regex cost into the A/B? Answer on #17; no build is possible until then. |
| 3 | **Build and A/B**: `ab rollup-samples` | [rollup-samples](../prs/rollup-samples.md) | [#16](https://github.com/JTCunning/ClickHouse/issues/16) | instant selectors at 5m/1h steps read rollup columns with the raw result: emulated 7-day selector grid ~29× (1h), ~5× (5m); storage +5.3% (5m), +0.45% (1h) |

Order: review fixes to sync first (an upstream reviewer is waiting), then submits, then decisions, then builds. Within each, lil red's `priority` order. A move is a command for the `lil-red-outbox` skill; answer a decision on the item's issue.

## With lil red: nothing for you

| item | issue | lil red is | what it's for |
|---|---|---|---|
| [grid-gap-wraparound](../prs/grid-gap-wraparound.md) | [#10](https://github.com/JTCunning/ClickHouse/issues/10) | reworking the branch, or dropping it | run 2: gap slowdown gone in every arrival order; no memory growth on sparse or every-2nd-minute series |
| [parallel-statistics-probe](../prs/parallel-statistics-probe.md) | [#15](https://github.com/JTCunning/ClickHouse/issues/15) | judging your results | your build, verdict fail: cold T1 -6.1% (857 -> 805 ms), under the 15% bar; the 13 serial index loads did not occur on that window (6 parts) |
| [selector-one-tags-row-per-id](../prs/selector-one-tags-row-per-id.md) | [#18](https://github.com/JTCunning/ClickHouse/issues/18) | judging your results | your build, verdict fail: wide selector -13% at 2 threads, -5% at 8; merged wide selector +13% at 2 threads (52 -> 59 ms) |
| [selector-ids-fixed-join-order](../prs/selector-ids-fixed-join-order.md) | [#20](https://github.com/JTCunning/ClickHouse/issues/20) | judging your results | your build, verdict fail: cold -6.8..-17% (T3 under the 7% bar, 13 statistics loads left); prewarmed +15..+40% |

## Submitted

| item | upstream | issue | what it does |
|---|---|---|---|
| [promql-shared-selector-sets](../prs/promql-shared-selector-sets.md) | PR 130402 | [#5](https://github.com/JTCunning/ClickHouse/issues/5) | x + x −15.5%, x + x + x −21.8% (run 3): a repeated selector's id set is built once per query |
| [promql-drop-name-exact-selector](../prs/promql-drop-name-exact-selector.md) | PR 126763 | [#8](https://github.com/JTCunning/ClickHouse/issues/8) | sum(rate) −19% CPU at 100k series: no duplicate-series check when the metric name of one metric is dropped |
| [time-ranges-id-ranges-index-hint](../prs/time-ranges-id-ranges-index-hint.md) | PR 129064 | [#6](https://github.com/JTCunning/ClickHouse/issues/6) | 100-name selector −19.9% (176 → 141 ms); 50–300 names −35–39% (build, 2026-11-01) |
| [alp-search-early-exit](../prs/alp-search-early-exit.md) | PR 131497 | [#11](https://github.com/JTCunning/ClickHouse/issues/11) | run 2 passed: small-block INSERT of decimal-like values 2.2x faster (-54.6%), codecs_float_insert ALP rtn -9.9%, nothing slower; same bytes |

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
