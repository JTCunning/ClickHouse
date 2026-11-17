#pragma once

#include <Columns/IColumn_fwd.h>
#include <Common/PODArray_fwd.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ContextTimeSeriesTagsCollector.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>

#include <memory>
#include <optional>


namespace DB
{
class StorageTimeSeries;

/// An index of the time series of a TimeSeries table kept in memory by the server.
/// It resolves the matchers of a PromQL selector and a time range to the identifiers and the tags of the matching time series
/// without reading the "tags" and "time ranges" target tables.
///
/// The index holds for each time series its identifier, its tags and its time range,
/// and for each pair of a tag name and a tag value the list of the time series having it.
/// Regular expressions are checked against the distinct values of a tag, not against the time series.
/// The time series are divided into shards by the metric name, and each shard has its own lock.
///
/// The index is built from the data parts of the "tags" and "time ranges" tables, and follows the new data parts in the background.
/// Both tables only grow: a time series never changes its tags, and its time range only widens. So the rows of a data part
/// can be applied in any order and any number of times, and a merge of data parts already applied changes nothing.
/// The index remembers which ranges of block numbers it has applied. It answers a query only if every data part
/// the query would read from these tables is inside the applied ranges. A query applies the data parts with few rows itself
/// and waits a limited time for the other ones; after that the query reads the tables as usual.
/// A change that can remove rows (a mutation, a removed data part, an `ALTER` of a table) makes the index build itself anew.
///
/// The "tags" table keeps a time series forever, and the index does not. It drops a time series which has no samples newer
/// than the horizon (the retention of the "recent samples" table, if there is one) and which got no samples for some time.
/// The watermark of a shard is the newest maximum time of the time series the shard has dropped or has not admitted.
/// The index answers a query only if its time range starts after the watermark, so a dropped time series changes
/// how fast a query is and never what it returns. A query with an older time range reads the tables.
/// A time series which returns after it was dropped can come without its tags, because a writer does not write
/// the tags it has written recently. The index reads the tags of such a time series from the "tags" table in the background,
/// and the queries which can select it read the tables meanwhile.
/// When the index is near its size limit, the horizon gets shorter. If that is not enough, the index frees its memory
/// and the queries read the tables.
///
/// The index supports the identifiers which are tuples of two components with the first one calculated from the metric name alone,
/// as the default ones are. It is created at the first query which can use it.
class TimeSeriesSeriesIndex
{
public:
    using TagNamesAndValuesPtr = ContextTimeSeriesTagsCollector::TagNamesAndValuesPtr;

    TimeSeriesSeriesIndex(const StorageTimeSeries & time_series_storage_, const ContextPtr & global_context_, UInt64 max_size_bytes_);
    ~TimeSeriesSeriesIndex();

    /// Stops the background task. The index cannot be used after that.
    void shutdown();

    struct LookupResult
    {
        /// The identifiers of the matching time series, without `LowCardinality` in the type.
        ColumnPtr ids;
        /// The tags of each of these time series, including the metric name.
        VectorWithMemoryTracking<TagNamesAndValuesPtr> tags;
        /// Whether the query has waited for the index to apply the data parts the query needs.
        bool waited = false;
    };

    /// Tells the index that a query is going to look for time series with a time range starting at `min_time`:
    /// the index starts building if it is not built. Returns whether the index is built and can answer for this time range,
    /// so the query can skip the lookup otherwise.
    bool prepareLookup(std::optional<Int64> min_time, const ContextPtr & query_context);

    /// Finds the time series matching `matchers` with a time range intersecting [`min_time`, `max_time`].
    /// The bounds are optional and have the scale of the timestamps in the table.
    /// The result contains every time series the query `StorageTimeSeriesSelector::makeSelectIDsQuery` would return
    /// for the data parts visible to `query_context` at the moment of the call.
    /// Returns nothing if the index cannot answer: it is not built yet, or the time range does not start after the watermark,
    /// or the index has not applied the data parts of the query in the time set by `time_series_series_index_max_wait_ms`,
    /// or it waits for the tags of a time series which the matchers can select.
    /// With `wait` the function builds the index and applies all the data parts itself instead of returning nothing.
    std::optional<LookupResult> tryLookup(
        const PrometheusQueryTree::MatcherList & matchers,
        std::optional<Int64> min_time,
        std::optional<Int64> max_time,
        const ContextPtr & query_context,
        bool wait);

    /// Sets `res[i]` to 1 if the index contains the time series with the identifier in the row `i` of `id_column`.
    /// Returns false and leaves `res` empty if the index is not built.
    /// Zero means that the time series is not in the index: the table can have it, because the index drops time series.
    bool contains(const ColumnPtr & id_column, PaddedPODArray<UInt8> & res) const;
    /// The number which changes when the index is built anew.
    [[nodiscard]] UInt64 getEpoch() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
};

using TimeSeriesSeriesIndexPtr = std::shared_ptr<TimeSeriesSeriesIndex>;

}
