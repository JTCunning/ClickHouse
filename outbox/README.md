# lil red's outbox: fix branches ready for James

These are fix branches that lil red can't build itself. James builds and A/B tests them, then submits them upstream. The machine-readable index is [`index.json`](index.json); this table is the same list for reading.

| # | item | status | branch | issue | claim |
|---|---|---|---|---|---|
| 1 | [promql-shared-selector-sets](../prs/promql-shared-selector-sets.md) | results-in | `lilred-perf-promql-shared-selector-sets` | #5 | `x + x` 41.6 → 35.4 ms (−15%, run 1): the id set is built once per query |
| 2 | [promql-drop-name-exact-selector](../prs/promql-drop-name-exact-selector.md) | ready-to-build | `lilred-perf-promql-drop-name-exact-selector` | #8 | sum(rate) −19% CPU at 100k series. Overlaps upstream 121069 and 126763: land one of them |
| 3 | [time-ranges-id-ranges-index-hint](../prs/time-ranges-id-ranges-index-hint.md) | ready-to-submit | `lilred-perf-time-ranges-id-ranges-index-hint` | #6 | 100-name selector 437 → 233 ms |
| 4 | [grid-gap-wraparound](../prs/grid-gap-wraparound.md) | ready-to-build | `lilred-perf-grid-gap-wraparound` | #10 | the 6.1× / 4.1× slowdown on series with a gap goes away |
| 5 | [alp-search-early-exit](../prs/alp-search-early-exit.md) | ready-to-build | `lilred-perf-alp-search-early-exit` | #11 | ALP encode CPU −40–90% on decimal-like values, same bytes |

## How an item moves

1. `ready-to-build`: James's Cursor skill builds the item's `base` and `head` commits, runs the listed tests and the A/B, and posts the results on the item's issue.
2. `results-in`: lil red checks the results against `pass_if` and writes the real numbers into the PR text. It drops `[unbuilt]` and runs its review-bot preflight again.
3. `ready-to-submit`: James's skill submits it. That means a branch `jtcunning-<slug>` and an upstream PR whose body is the text between the `<!-- upstream:start -->` and `<!-- upstream:end -->` markers of `prs/<slug>.md`.
   - `needs-work`: lil red is reworking or dropping the item instead.
4. `submitted`: done. If review feedback comes in, the skill posts it on the issue. lil red pushes fixes to its branch (`fixes-ready`), and the skill syncs them into `jtcunning-<slug>`.

Only lil red edits this directory. James's skill reports through comments on the item's issue. Each comment starts with a marker that lil red's intake reads: `<!-- lil-red-outbox:results -->`, `<!-- lil-red-outbox:submitted -->` or `<!-- lil-red-outbox:feedback -->`.
