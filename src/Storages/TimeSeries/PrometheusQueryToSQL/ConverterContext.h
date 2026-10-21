#pragma once

#include <DataTypes/IDataType.h>
#include <Interpreters/StorageID.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRangeGetter.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLSubquery.h>
#include <Parsers/Prometheus/TimeSeriesVersion.h>

#include <unordered_map>


namespace DB::PrometheusQueryToSQL
{

/// A vector grid of an instant selector which occurs more than once in a query with the same evaluation range.
/// The grid is added to the subqueries as a materialized CTE and every occurrence of the selector reads from it.
struct SharedInstantSelectorGrid
{
    String subquery_name;
    bool metric_name_dropped = false;
    TimestampType start_time = {};
    TimestampType end_time = {};
    DurationType step = {};
};

/// Contains information which is used to convert a prometheus query to SQL.
struct ConverterContext
{
    const std::shared_ptr<const PrometheusQueryTree> promql_tree;

    const StorageID time_series_storage_id;
    UInt64 time_series_version = TimeSeriesVersion::LATEST;

    /// Data type of the column `timestamp` returned by the query built by the converter.
    /// All timestamps and durations in the converter (see TimestampType and DurationType) use `result_timestamp_scale`.
    /// The samples read from the TimeSeries table keep the types of the table, see the comment for StoreMethod::RAW_DATA.
    DataTypePtr result_timestamp_type;
    UInt32 result_timestamp_scale;

    const ResultType result_type;
    const NodeEvaluationRangeGetter node_range_getter;
    SQLSubqueries subqueries;

    /// How many times each instant selector occurs in the query, keyed by the selector text and its evaluation range
    /// (see makeInstantSelectorKey()). Filled by countInstantSelectorUses() before the conversion starts.
    std::unordered_map<String, size_t> instant_selector_use_count;

    /// Grids shared between occurrences of the same instant selector, keyed like `instant_selector_use_count`.
    std::unordered_map<String, SharedInstantSelectorGrid> shared_instant_selector_grids;

    ConverterContext(std::shared_ptr<const PrometheusQueryTree> promql_tree_,
                     const PrometheusQueryEvaluationSettings & settings_);
};

}
