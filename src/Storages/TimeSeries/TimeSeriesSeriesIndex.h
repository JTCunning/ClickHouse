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
///
/// The index is built from the data parts of the "tags" and "time ranges" tables, and follows the new data parts in the background.
/// Both tables only grow: a time series never changes its tags, and its time range only widens. So the rows of a data part
/// can be applied in any order and any number of times, and a merge of data parts already applied changes nothing.
/// The index remembers which ranges of block numbers it has applied. It answers a query only if every data part
/// the query would read from these tables is inside the applied ranges; otherwise the query reads the tables as usual.
/// A change that can remove rows (a mutation, a removed data part, an `ALTER` of a table) makes the index build itself anew.
///
/// The index contains only the time series with samples newer than the retention of the "recent samples" table (if there is one),
/// and it stops growing and frees its memory when it reaches its size limit. A query with an older time range reads the tables.
///
/// The index is created at the first query which can use it.
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
    };

    /// Finds the time series matching `matchers` with a time range intersecting [`min_time`, `max_time`].
    /// The bounds are optional and have the scale of the timestamps in the table.
    /// The result contains every time series the query `StorageTimeSeriesSelector::makeSelectIDsQuery` would return
    /// for the data parts visible to `query_context` at the moment of the call.
    /// Returns nothing if the index cannot answer: it is not built yet, new data parts are not applied yet,
    /// or the time range is older than what the index holds.
    /// With `wait` the function builds the index and applies new data parts itself instead of returning nothing.
    std::optional<LookupResult> tryLookup(
        const PrometheusQueryTree::MatcherList & matchers,
        std::optional<Int64> min_time,
        std::optional<Int64> max_time,
        const ContextPtr & query_context,
        bool wait);

    /// Sets `res[i]` to 1 if the index contains the time series with the identifier in the row `i` of `id_column`.
    /// Returns false and leaves `res` empty if the index is not built.
    /// The answer stays valid while `getEpoch` returns the same number: the index only grows within an epoch.
    bool contains(const ColumnPtr & id_column, PaddedPODArray<UInt8> & res) const;
    [[nodiscard]] UInt64 getEpoch() const;

    /// For the check of the index against the tables (setting `time_series_check_series_index`): whether it is correct that
    /// the index did not return the time series in the row `row` of `id_column` for the specified time range
    /// while the tables returned it. It is correct only if the index holds the time series with its tags, and the time range
    /// of the rows it has applied does not intersect the specified one: a merged row of the "time ranges" table can cover
    /// a range without samples, and the index does not hold the rows older than its horizon.
    [[nodiscard]] bool isMissExplained(const ColumnPtr & id_column, size_t row, std::optional<Int64> min_time, std::optional<Int64> max_time) const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
};

using TimeSeriesSeriesIndexPtr = std::shared_ptr<TimeSeriesSeriesIndex>;

}
