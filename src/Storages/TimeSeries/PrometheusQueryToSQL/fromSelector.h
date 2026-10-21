#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Counts the occurrences of each instant selector in the query and stores them in `context.instant_selector_use_count`.
/// Only the instant selectors converted by `fromSelector(instant_selector_node)` are counted, i.e. the instant selector
/// inside a range selector is not.
void countInstantSelectorUses(ConverterContext & context);

/// Makes a SQL query to read from an instant selector, for example
/// http_requests{job="prometheus"}
/// If the same selector occurs more than once in the query with the same evaluation range, the vector grid is built
/// once as a materialized CTE and each occurrence reads from it.
SQLQueryPiece fromSelector(const PrometheusQueryTree::InstantSelector * instant_selector_node, ConverterContext & context);

/// Makes a SQL query to evaluate the PromQL function timestamp() on an instant selector, for example
/// timestamp(http_requests{job="prometheus"})
/// It selects the same samples as `fromSelector(instant_selector_node)` does, but returns their timestamps
/// (in seconds since epoch) instead of their values.
SQLQueryPiece fromSelectorSampleTimestamps(const PrometheusQueryTree::InstantSelector * instant_selector_node, ConverterContext & context);

/// Makes a SQL query to read from a range selector, for example
/// http_requests{job="prometheus"}[20m]
SQLQueryPiece fromSelector(const PrometheusQueryTree::RangeSelector * range_selector_node, ConverterContext & context);

}
