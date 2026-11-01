# Label value regex → `IN` via the text index dictionary: it works, but the net gain is small and needs a new threshold
**Verdict:** Not worth building as a "simple" change. The rewritten form is served by the text index (111 posting lookups instead of 0). On a 100k-series metric it makes the selector **4–14% faster** before the cost of resolving the values, and **~3–12%** after it. It gains nothing on narrow metrics. To be safe it needs a literal-prefix guard, a value cap (a new setting) and a second query against a different snapshot of the data. Report only; no branch.
**Task:** 2026-10-31-handoff-rewrite-label-value-regex-to-in · handoff https://github.com/JTCunning/ClickHouse/issues/7 · **Build:** 26.11.1.974, `a7f713612c8` (upstream master is now `24037cfec1c`, with no change to the selector or the text index condition since) · **Date:** 2026-11-01

## What happens (plain language)
- **How the selector filters today:** a `instance=~"host-1.*"` matcher becomes a regex filter on `tags['instance']`. The text index on `tags` can't serve regexes, so ClickHouse reads the `instance` values of every row of the metric and runs the regex on each one.
- **How the rewrite helps:** an `IN` list of literal values is served by the index's direct read: the filter comes from the posting lists, and the `instance` values are never read or compared.
- **What it doesn't change:** in this data (as in most real data), the matching series are spread over every granule of the metric. So the rows read stay the same (all of `metric_0`), and so does the read of the whole tags Map that `timeSeriesStoreTags` needs. The rewrite saves only the regex work and about 1.5 MiB of reads. That is why the gain is modest.
- **What the values cost to look up:** the index dictionary is sorted by token, and each token is `key ++ value ++ trailer`. With a literal prefix, `startsWith(token, 'instance' ++ 'host-1')` jumps to the right dictionary blocks, and the lookup takes ~2–3 ms. With a filter on `token_key` alone, it reads the whole dictionary instead: 30 ms here, more than the whole gain.

## Numbers
- **Dataset** (synthetic; the handoff's generator at 300k series instead of 1M):
  - `metric_0` has 100,000 series; 199 other metrics have ~1,000 each
  - 6 labels; `pod` is unique per series, so the dictionary holds ~300k tokens
  - 40 samples at 15 s; TimeSeries schema v11; inner tables merged
- **Window and settings:** 5 min window, `max_threads = 2`, local server on 2 vCPU.
- **Timing:** median of 9 warm interleaved rounds (min–max). The `IN` form is produced as an alternation of literals, which the selector already turns into `IN`, so this is exactly the rewrite's output.

**A. End to end: regex vs. the `IN` list it would become (master)**

| shape on `metric_0` | series | selector ms | `sum(rate(x[5m]))` ms | instant `x` ms |
|---|---|---|---|---|
| `instance=~"host-1.*"` (regex) | 22,254 | 126 (123–133) | 172 (157–177) | 165 (160–188) |
| same as `IN` of its 111 values | 22,254 | **116** (111–123) | **162** (158–169) | **158** (153–171) |
| `instance=~"host-1[0-9]:9100"` (regex) | 2,094 | 121 (115–125) | 126 (123–137) | 124 (121–133) |
| same as `IN` of its 10 values | 2,094 | **104** (102–152) | **114** (109–132) | **111** (109–157) |
| `job="job3"` (equality, reference) | 9,848 | 128 (126–139) | 160 (151–168) | 146 (141–167) |

Read rows are identical within each pair (313,122 and 311,222).

**B. The tags lookup alone** (the selector's `id IN (…)` subquery; 7 runs, median)

| shape | ms | CPU ms | tags rows | tags MiB | text index posting lookups |
|---|---|---|---|---|---|
| `host-1.*` regex | 39.5 | 34 | 212,992 | 9.21 | 0 |
| `IN` of 111 values | 29.5 | 27 | 212,992 | 7.71 | 111 |
| `host-1[0-9]:9100` regex | 34.5 | 26 | 212,992 | 9.21 | 0 |
| `IN` of 10 values | 23.0 | 23 | 212,992 | 7.71 | 10 |
| `job="job3"` | 25.5 | 19 | 212,992 | 7.71 | 1 |

This confirms the handoff's direction: the `IN` form brings the regex close to the equality matcher.

**C. Resolving the values from `mergeTreeTextIndex(db, <tags table>, 'tags_idx')`**

| resolution filter | ms (6 runs) | values |
|---|---|---|
| `token_key = 'instance' AND match(token_value, '^host-1.*$')` | 30–32 | 111 |
| `startsWith(token, 'instancehost-1')` + the same filter | 2–3 | 111 |
| `startsWith(token, 'instance')` + the same filter (regex without a literal prefix) | 2–3 | 111 |
| `startsWith(token, 'podpod-1')`: a high-cardinality label | 27–29 | 111,111 |
| `startsWith(token, 'pod')`: same, without a prefix | 50–57 | 111,111 |

With 6 unmerged parts the prefix lookup stayed at ~3 ms. Only a condition on `token` reaches the dictionary-block pruning; one on `token_key` doesn't.

**Net:** the wide 111-value case saves ~10 ms and pays ~3 ms, so ~5% end to end. The 10-value case nets ~10%. A regex on a high-cardinality label (`pod`, `container_id`, a trace id) **loses** 20–50 ms, because the lookup scans that label's whole slice of the dictionary.

## Why (cause)
- **The regex as a filter:** `matcherToAST` (`src/Storages/StorageTimeSeriesSelector.cpp:367`) emits `match(arrayElement(tags, 'k'), '^re$')`. A plain alternation of literals already becomes `in(...)` via `tryGetLiteralAlternatives`.
- **What the text index serves:** `MergeTreeIndexConditionText` serves equality and `IN` on a Map element by direct read (`:2089–2185`), but not `match`, so a regex matcher evaluates per row.
- **What the gain is bounded by:** the matched series cover every granule, so neither the granules read nor the Map read by `timeSeriesStoreTags` shrinks. Only the per-row regex and the `instance` subcolumn read go away.

## What a fix would need (why I didn't build it)
1. **A literal-prefix guard.** Resolve only when `extractFixedPrefixFromRegularExpression` (`src/Common/RegexpUtils.h:50`) gives a non-empty prefix, and query by `startsWith(token, key ++ prefix)`. The token encoding (`KeyValuePairsTokenizer::encodeToken`, `src/Interpreters/ITokenizer.cpp:545`) then leaks into the selector. Even a key-only prefix is unsafe on high-cardinality labels (table C).
2. **A value cap with `LIMIT cap + 1`**, falling back to `match` above it. The handoff asks for the cap to be a setting. That is a new threshold plus a new setting (`Settings.cpp` and `SettingsChangesHistory`), and the review bot reliably questions both. The cap also bounds the scan only when values match; a rare match on a big label still scans its whole slice.
3. **A second query on another snapshot.** The values are resolved before the main query reads the tags table. A series inserted between the two steps with a new matching value would be missed. That is a small but real change of behaviour; the existing probe in `tryMakeWholeMetricIDRangeConditions` only chooses between two equivalent forms, so it doesn't have this problem.
4. **The guards from the handoff:** no match on the empty string, positive matchers only, labels outside `tags_to_columns` only, an index check on `tags` with `keyValuePairs`, and an empty list mapped to `makeSelectNoIDsQuery`.

Effort: M, for a 3–12% gain on wide metrics and nothing on narrow ones. Items 1–3 together exceed "one mechanism, no new threshold".

**A better place for it:** inside the text index condition, which would serve `match(tags['k'], re)` from each part's own dictionary while the query runs. That needs no second query, no snapshot gap and no token encoding in the selector, and the part's dictionary is what the posting lists index anyway. The open upstream PR #121650 (an automaton walk over sorted text index dictionaries for `LIKE`) is building exactly that machinery; extending it to `match` on `keyValuePairs` Map elements is a follow-up there. That's outside the handoff's "selector only" constraint, so I'm flagging it rather than doing it.

**Cheaper alternative, unmeasured:** the selector could rewrite only regexes that are a literal prefix plus `.*` (`host-1.*`) into a range on the token (`startsWith`), if the text index learns to serve a token range. That is the same "do it in the index" answer.

## Upstream overlap
- **Open PRs checked:**
  - #127848 expands literal-with-group regexes into `IN`. It's complementary: more regexes become `IN` without any dictionary lookup.
  - #121650: dictionary automaton for `LIKE` (see above).
  - #126999: a text-index `match` case-insensitivity bug fix, unrelated to Map keys.
  - #116009: regex anchoring for alternation, correctness.
  None does this rewrite.
- **Recent commits:** nothing on `matcherToAST` or the Map-element direct read since `a7f713612c8`.
- **James's fork branches:** none touch label regex matchers (checked the `promql-*` and `prom-*` branches by name; the closest, `jtcunning/prom-labels-direct-tags`, is about the labels API).

## How to reproduce
- **Scripts:** in research `bench/label-regex-in/`.
  - `bench.py` times the full selector plus its tags subquery, with posting-lookup counts.
  - `ab.py` runs the interleaved A/B rounds.
  - `alt111.txt` holds the 111-value alternation.
- **Data:** the handoff's `INSERT` with `numbers(300000)`, `if(number < 100000, 0, (number % 199) + 1)` and `range(40)`, `T0 = T_END - 600`; then `OPTIMIZE TABLE <each .inner_id.*> FINAL`.

```sql
-- Value resolution through the dictionary (fast form):
SELECT DISTINCT token_value FROM mergeTreeTextIndex(default, `<.inner_id.tags.*>`, tags_idx)
WHERE startsWith(token, 'instancehost-1') AND token_key = 'instance' AND match(token_value, '^host-1.*$');
-- The IN form, without a build: pass the resolved values as an alternation
--   metric_0{instance=~"host-1:9100|host-10:9100|…"}  (tryGetLiteralAlternatives turns it into IN)
SELECT count(), sum(length(samples))
FROM timeSeriesSelector(ts, '<selector>', toDateTime64(T_END - 300, 3), toDateTime64(T_END, 3), 'samples')
SETTINGS max_threads = 2;
-- system.query_log: ProfileEvents['TextIndexPostingsCacheHits'] + ['TextIndexPostingsCacheMisses'] = posting lookups.
```
