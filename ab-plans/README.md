# A/B backlog: to run once lil red can build ClickHouse

Every experiment so far was measured on a 2 vCPU / 4 GiB machine that cannot build ClickHouse. Fix branches are `[unbuilt]`; their "after" numbers come from emulation (rewritten SQL run on a master binary) or from standalone models of the changed code. This list is what has to be proven on real builds before any of it goes upstream.

Each plan follows the template in skill `clickhouse-timeseries-perf-lab` (section 7): the claim with its predicted number, builds, machine, data, target and guard cases, method (two servers side by side, as the CI performance comparison does), metrics, a correctness check and pass/fail criteria.

Order: fix branches first (they need a build anyway), then report conclusions that the small machine limited, then the rest.

| # | experiment | kind | task | what has to be proven | plan |
|---|---|---|---|---|---|
| 1 | [promql-shared-selector-sets](../reports/2026-11-01-promql-shared-selector-sets.md) (handoff #5) | branch `lilred-perf-promql-shared-selector-sets` | 2026-10-31-handoff-run-the-tags-lookup-once-per-di | a repeated selector builds its id set once (−5–7 ms per repeat, run 1); no single-use cost; pk_range guard settled | [plan](promql-shared-selector-sets.md) |
| 2 | [drop-name-exact-selector](../reports/2026-11-01-drop-name-exact-selector.md) (handoff #8) | branch `lilred-perf-promql-drop-name-exact-selector` | 2026-10-31-handoff-skip-the-tags-read-for-label-fr | sum(rate) −19% CPU at 100k series; identical results | no plan needed: measured on James's build 2026-11-05, passed (−24% `sum(rate)`), see [prs](../prs/promql-drop-name-exact-selector.md) |
| 3 | [name-regex-in-list](../reports/2026-10-31-name-regex-in-list.md) (handoff #6) | branch `lilred-perf-time-ranges-id-ranges-index-hint` | 2026-10-31-handoff-rewrite-name-regex-to-a-met | 100-name selector 437 → 233 ms | no plan needed: measured on James's build 2026-11-01, passed (−19.9%), see [prs](../prs/time-ranges-id-ranges-index-hint.md) |
| 4 | [grid-gap-wraparound](../reports/2026-10-31-grid-gap-wraparound.md) (#125759) | branch `lilred-perf-grid-gap-wraparound` | 2026-10-31-timeseriestogrid-gap-slowdown-125759 | run 1 passed (8.8× / 6.5×, 2026-11-05); run 2: the reworked rule also fixes reverse order and merge into the later run, sparse gap and contiguous unchanged | [plan](grid-gap-wraparound.md) |
| 5 | [alp-search-early-exit](../reports/2026-11-01-alp-search-early-exit.md) | branch `lilred-perf-alp-search-early-exit` | 2026-10-31-alp-codec-scale-the-parameter-search-to | encode CPU −40–90% on decimal-like values; byte-identical output | [plan](alp-search-early-exit.md) |
| 6 | [tags-read-parallelism](../reports/2026-11-01-tags-read-parallelism.md) (handoff #9) | report: verify at 16 threads | 2026-10-31-handoff-measure-and-raise-read-parallel | the setting sweep holds at 8–16 threads (not possible on 2 vCPU) | todo |
| 7 | [lowcardinality-uuid-id](../reports/2026-11-01-lowcardinality-uuid-id.md) | report: verify the plain-id default | 2026-10-31-timeseries-id-what-lowcardinalityuuid | plain UUID id reads faster, ingest −7%, at realistic scale | todo |
| 8 | [label-regex-in](../reports/2026-11-01-label-regex-in.md) (handoff #7) | report: needs a branch first | 2026-10-31-handoff-rewrite-label-value-regex-to-in | IN via the text index: 4–14% end to end on 100k series | todo |
| 9 | [remote-write-baseline](../reports/2026-11-01-remote-write-baseline.md) | report: re-baseline at scale | 2026-10-31-baseline-remote-write-ingest-and-the-ded | per-sample CPU and the merge share hold with more cores and senders | todo |
| 10 | [tags-collector-store-cost](../reports/2026-10-31-tags-collector-store-cost.md) (handoff #3) | report: model only | 2026-10-31-handoff-cut-per-row-cpu-of-contexttimes | the ~2× from steps 1+3 on a real build (needs a branch first) | todo |
| 11 | [samples-timestamp-read-cost](../reports/2026-11-08-samples-timestamp-read-cost.md) (handoff #12) | report: settings A/B on EBS | 2026-11-01-handoff-samplestimestamp-read-cost-dom | local prefetch + 1 MiB local buffer: sync read wait −80%, wall −10–21% on the 30-day `rate()` + `sum by()` with a cold page cache (2 vCPU SSD showed the mechanism only) | [plan](samples-timestamp-read-cost.md) |

No A/B needed:
- [selector-probe](../reports/2026-10-31-selector-probe.md): already fixed upstream by #117344.
- [selector-lookup-once](../reports/2026-10-31-selector-lookup-once.md): superseded by #1.
- [skip-tags-label-free](../reports/2026-11-01-skip-tags-label-free.md): superseded by #2.
- [selector-eager-ids](../reports/2026-10-31-selector-eager-ids.md): the verdict is "no gain"; re-check only if asked.

Every new experiment adds a row here and its plan in the same run.
