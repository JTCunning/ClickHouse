#pragma once

#include <base/types.h>

#include <string_view>


namespace DB
{

/// Packs a Prometheus / OpenMetrics-style metric name into UInt32 for MergeTree **prefix** locality
/// (`ORDER BY (metric_locality_id, …)` on the TimeSeries **data** inner table).
///
/// **Why metric name only (not `metric_name + tag keys` nor `metric_name + tag values` in this UInt32):**
/// `timeSeriesSelector` can push `PREWHERE metric_locality_id = f(selector)` when `__name__` is an equality matcher.
/// Then `f` must depend **only** on information available from that matcher (the metric name string). If each row’s
/// locality also depended on the series’ full label set or tag-key set, the value on disk would differ per series while
/// the selector gives only `__name__` — or only a **subset** of labels — so no single constant would match all selected
/// rows (or we would need a second sort column / subquery to compute the prefix, which defeats one-shot part pruning).
/// Mixing tag keys or tag KV into **this** column without an extra key would either break that fast path or force
/// `IN (very large set)` from partial knowledge. Coarsely bucketing by metric name matches typical Grafana/Thanos
/// panels (one metric/recording rule per `expr`) and stays consistent with PromQL’s universal `__name__` label.
///
/// **Older / external data targets:** the physical **data** table is inspected; if `metric_locality_id` is absent,
/// **reads** (`timeSeriesSelector`) stay valid: `PREWHERE` locality pruning applies only when the column exists; otherwise
/// locality is synthesized (constant from `__name__` EQ when possible, else `ANY INNER JOIN` to the tags table and
/// `timeSeriesMetricLocalityId(metric_name)`). **Remote-write ingest** omits the column in `INSERT` when it is missing on
/// the data target, so Prometheus payloads keep working without a migration flag. No user setting — behavior follows
/// the on-disk schema.
///
/// **Stored vs computed (tags/metadata):** The **data** inner table keeps `metric_locality_id` on disk as the first
/// sort-key column (written at ingest). The **tags** inner table already reads `metric_name` for every matched series
/// (PromQL matchers and `timeSeriesStoreTags`), so computing `computeTimeSeriesMetricLocalityId(metric_name)` in memory
/// avoids an extra physical column (~4 bytes/series + codec/decode overhead) without skipping any I/O for `metric_name`.
/// The hash is O(length of name) on a short ASCII string (on the order of tens of nanoseconds per row in a tight loop —
/// still negligible next to decompressing and scanning the `metric_name` column from a MergeTree granule).
///
/// Up to six lowercase letters are taken from **initials** of snake_case segments and camelCase words
/// (ASCII uppercase after lowercase or digit starts a new word). Only `a-z` / `A-Z` contribute; digits
/// and other ASCII symbols are ignored as initials.
/// Packing: base-27 big-endian mix — digit 0 = unused slot, digits 1..26 = `a`..`z`. (`27^6` fits in UInt32.)
/// Series identity remains in `id`; this value is for coarse clustering only.
UInt32 computeTimeSeriesMetricLocalityId(std::string_view metric_name);

}
