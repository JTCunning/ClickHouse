#pragma once

#include <Core/Types.h>
#include <DataTypes/IDataType.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>

#include <functional>
#include <optional>
#include <vector>


namespace DB
{

/// The range of the primary key `id` of the samples table and of the time ranges table covering all the time series of one metric.
/// If the identifiers are two-component tuples `Tuple(F, S)` and the id generator computes the first component from the metric name
/// alone (as the default generator does, see `TimeSeriesIDGenerator::getDefault`), the identifiers of the time series of one metric
/// occupy one continuous range: tuple(hash(metric_name), min_S) <= id <= tuple(hash(metric_name), max_S).
struct TimeSeriesIDRange
{
    String metric_name;
    ASTPtr first_component; /// hash(metric_name) as a constant expression, for example sipHash64('my_metric').
    ASTPtr min_second_component; /// The minimum and the maximum of the type of the second component.
    ASTPtr max_second_component;
};

using TimeSeriesIDRanges = std::vector<TimeSeriesIDRange>;

/// Returns the names of the metrics matched by a regex matcher on `__name__`, or nullopt if there are too many to list.
using MetricNamesResolver = std::function<std::optional<Strings>(const PrometheusQueryTree::Matcher & name_matcher)>;

/// Makes the ranges of the identifiers of the metrics selected by the matchers, one range per metric (see TimeSeriesIDRange):
/// the matchers must contain exactly one equality matcher or one regex matcher on `__name__`,
/// the id generator must be `tuple(<expression of the metric name only>, <anything>)`,
/// and the id type must be a two-component tuple whose second component has a type with known bounds.
/// The names come from the equality, from the regex if it is a list of plain literals (all names non-empty),
/// or from `resolve_metric_names` for another regex.
/// Returns nullopt if any of these conditions doesn't hold, otherwise at least one range.
std::optional<TimeSeriesIDRanges> tryGetTimeSeriesIDRanges(
    const PrometheusQueryTree::MatcherList & matchers,
    const ASTPtr & id_generator,
    const DataTypePtr & id_type,
    const MetricNamesResolver & resolve_metric_names = {});

/// `<id> >= tuple(<first_component>, <min_S>) AND <id> <= tuple(<first_component>, <max_S>)`
ASTPtr makeIDInRangeCondition(const ASTPtr & id, const TimeSeriesIDRange & range);

/// `<id is in ranges[0]> OR <id is in ranges[1]> OR ...` (see makeIDInRangeCondition). `ranges` must not be empty.
ASTPtr makeIDInRangesCondition(const ASTPtr & id, const TimeSeriesIDRanges & ranges);

/// `tupleElement(<id>, 1) != <first_component>`: the identifier is outside the range.
ASTPtr makeIDOutOfRangeCondition(const ASTPtr & id, const TimeSeriesIDRange & range);

/// `tupleElement(<id>, 1) NOT IN (<first components of the ranges>)`: the identifier is outside all the ranges. `ranges` must not be empty.
ASTPtr makeIDOutOfRangesCondition(const ASTPtr & id, const TimeSeriesIDRanges & ranges);

}
