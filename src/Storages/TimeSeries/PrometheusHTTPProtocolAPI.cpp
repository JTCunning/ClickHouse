#include <Storages/TimeSeries/PrometheusHTTPProtocolAPI.h>

#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/StringUtils.h>
#include <Common/isValidUTF8.h>
#include <Core/DecimalFunctions.h>
#include <Core/Field.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/StorageTimeSeriesSelector.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Parsers/Prometheus/PrometheusQueryResultType.h>
#include <Parsers/Prometheus/parseTimeSeriesTypes.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/Converter.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <Storages/TimeSeries/splitTimeSeriesType.h>
#include <Storages/TimeSeries/timeSeriesMatchersToAST.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Parsers/Prometheus/PrometheusQueryParsingUtil.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Core/Types.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>
#include <Interpreters/DatabaseCatalog.h>
#include <fmt/format.h>
#include <Common/re2.h>

#include <algorithm>
#include <limits>
#include <utility>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsBool filter_by_min_time_and_max_time;
    extern const TimeSeriesSettingsBool store_min_time_and_max_time;
    extern const TimeSeriesSettingsMap tags_to_columns;
}

namespace
{

bool containsUnsupportedPrometheusRegexpEscape(std::string_view regexp)
{
    bool escaped = false;
    for (const char character : regexp)
    {
        if (character == '\\')
        {
            escaped = !escaped;
            continue;
        }

        if (escaped && character == 'C')
            return true;
        escaped = false;
    }
    return false;
}

/// Whether a matcher matches a series that does not have the label at all, under the Prometheus
/// semantics "a missing label is equal to the empty label value". A regexp matcher is evaluated
/// against the empty string with the same fully-anchored RE2 semantics as the generated `match`
/// condition; an invalid regexp is rejected here (fail closed), like in the Prometheus parser.
bool matcherCanMatchEmptyLabelValue(const PrometheusQueryTree::Matcher & matcher, const String & match_param)
{
    switch (matcher.matcher_type)
    {
        case PrometheusQueryTree::MatcherType::EQ:
            return matcher.label_value.empty();
        case PrometheusQueryTree::MatcherType::NE:
            return !matcher.label_value.empty();
        case PrometheusQueryTree::MatcherType::RE:
        case PrometheusQueryTree::MatcherType::NRE:
        {
            if (containsUnsupportedPrometheusRegexpEscape(matcher.label_value))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Invalid value {} of the 'match[]' parameter: regexp escape sequence \\C is not supported",
                    quoteString(match_param));

            re2::RE2 regexp(matcher.label_value, re2::RE2::Quiet);
            if (!regexp.ok())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Invalid value {} of the 'match[]' parameter: cannot parse the regexp of the {}{}~{} matcher: {}",
                    quoteString(match_param),
                    matcher.label_name,
                    (matcher.matcher_type == PrometheusQueryTree::MatcherType::NRE) ? "!" : "=",
                    quoteString(matcher.label_value),
                    regexp.error());
            bool empty_matches_regexp = re2::RE2::FullMatch("", regexp);
            return (matcher.matcher_type == PrometheusQueryTree::MatcherType::RE) ? empty_matches_regexp : !empty_matches_regexp;
        }
    }

    UNREACHABLE();
}

/// The `match[]` parameter of the metadata endpoints is a Prometheus series selector: a bare metric name
/// (`go_info`) or a full instant selector with label matchers (`go_info{group="PROD"}`, `{job=~"prom.*"}`).
/// Grafana emits the selector forms when it narrows label names / label values by filters. Each value is
/// parsed with the same PromQL parser as `/api/v1/query`, and each label matcher is translated into the
/// same condition over the `tags` table that the real query path builds in `StorageTimeSeriesSelector`
/// (see `timeSeriesMatcherToAST`), so the metadata endpoints filter series exactly like the query
/// endpoints do. Prometheus defines the result of a repeated `match[]` as the union of the series matched
/// by each selector, hence the disjunction over the parameters. Each value is parsed with the PromQL
/// metric-selector grammar used by the query parser. Returns an empty string when there is no filter
/// to apply (no `match[]` values at all).
///
/// An explicitly empty `match[]` value (e.g. `?match[]=`), a value that is not an instant selector (e.g.
/// a range selector `up[5m]` or a PromQL expression), the empty selector `{}`, and a selector whose
/// matchers all match the empty label value (e.g. `{job=~".*"}` - see `matcherCanMatchEmptyLabelValue`)
/// are rejected (fail closed), as in Prometheus, instead of being silently dropped or treated as an
/// unfiltered result. The last rule is what keeps the "`match[]` is required" guard on `/api/v1/series`
/// meaningful: without it `match[]={job=~".*"}` would degenerate into a full scan of the `tags` table.
String makeMatchCondition(const Strings & match_params, const std::unordered_map<String, String> & column_name_by_tag_name)
{
    ASTs selector_conditions;
    for (const auto & match_param : match_params)
    {
        if (match_param.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Invalid empty value of the 'match[]' parameter: expected a series selector");

        PrometheusQueryTree selector;
        String error_message;
        if (!selector.tryParseMetricSelector(match_param, /* timestamp_scale_ = */ 3, &error_message))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Invalid value {} of the 'match[]' parameter: cannot parse a series selector: {}",
                quoteString(match_param), error_message);

        const auto * root = selector.getRoot();
        if (!root || root->node_type != PrometheusQueryTree::NodeType::InstantSelector)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Invalid value {} of the 'match[]' parameter: expected a series selector, not a PromQL expression",
                quoteString(match_param));

        const auto & matchers = typeid_cast<const PrometheusQueryTree::InstantSelector &>(*root).matchers;
        if (matchers.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Invalid value {} of the 'match[]' parameter: the selector must contain at least one matcher",
                quoteString(match_param));

        /// Prometheus rejects a selector whose matchers all match the empty label value (i.e. all match
        /// a series that has none of the mentioned labels), because such a selector does not narrow the
        /// series set at all: `{job=~".*"}` would degenerate into a full scan of the `tags` table.
        /// Every matcher is checked (no short-circuit), so an invalid regexp anywhere in the selector
        /// is rejected here with `bad_data` instead of failing later inside the generated SQL.
        bool has_non_empty_matcher = false;
        for (const auto & matcher : matchers)
            has_non_empty_matcher |= !matcherCanMatchEmptyLabelValue(matcher, match_param);
        if (!has_non_empty_matcher)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Invalid value {} of the 'match[]' parameter: the selector must contain at least one matcher "
                "that does not match the empty label value",
                quoteString(match_param));

        ASTs matcher_asts;
        matcher_asts.reserve(matchers.size());
        for (const auto & matcher : matchers)
            matcher_asts.push_back(timeSeriesMatcherToAST(matcher, column_name_by_tag_name));
        selector_conditions.push_back(makeASTForLogicalAnd(std::move(matcher_asts)));
    }

    if (selector_conditions.empty())
        return {};

    /// The parenthesized form keeps the condition intact when the caller appends it to other
    /// conditions with a plain textual " AND ".
    return fmt::format("({})", makeASTForLogicalOr(std::move(selector_conditions))->formatWithSecretsOneLine());
}

/// Closes the "data" array of a metadata endpoint response. When the optional `limit` parameter cut
/// the result short, the response carries the same warning Prometheus produces for a truncated
/// /api/v1/series, /api/v1/labels or /api/v1/label/<name>/values result.
void writeMetadataResponseFooter(WriteBuffer & response, bool truncated)
{
    if (truncated)
        writeString(R"(],"warnings":["results truncated due to limit"]})", response);
    else
        writeString("]}", response);
}

}

namespace Setting
{
    extern const SettingsBool enable_materialized_cte;
}

namespace
{
constexpr UInt32 LOOKBACK_DELTA_SCALE = 9;

/// Clip a request range to the domain represented by the storage timestamp column. Values outside that
/// domain cannot be converted to the native type safely: for DateTime/UInt32 the conversion can wrap,
/// while DateTime64 has a signed Int64 tick domain that can overflow at high precision.
bool clipTimestampRangeToStorageType(
    std::optional<Decimal128> & start_time,
    std::optional<Decimal128> & end_time,
    const DataTypePtr & timestamp_data_type,
    UInt32 timestamp_scale,
    UInt32 request_timestamp_scale)
{
    WhichDataType which_data_type{timestamp_data_type};
    if (!(which_data_type.isDateTime64() || which_data_type.isDateTime() || which_data_type.isUInt32()))
        return true;

    Decimal128 min_storage_timestamp;
    Decimal128 max_storage_timestamp;
    if (which_data_type.isDateTime64())
    {
        min_storage_timestamp = Decimal128{static_cast<Int128>(std::numeric_limits<Int64>::min())};
        max_storage_timestamp = Decimal128{static_cast<Int128>(std::numeric_limits<Int64>::max())};
    }
    else
    {
        min_storage_timestamp = Decimal128{0};
        max_storage_timestamp = Decimal128{
            static_cast<Int128>(std::numeric_limits<UInt32>::max())
            * DecimalUtils::scaleMultiplier<Decimal128>(timestamp_scale)};
    }

    /// Compare in the same scale as the parsed request values. `request_timestamp_scale` is never
    /// lower than `timestamp_scale`, so this conversion cannot lose fractional request precision.
    const auto min_timestamp
        = DecimalUtils::convertTo<Decimal128>(request_timestamp_scale, min_storage_timestamp, timestamp_scale);
    const auto max_timestamp
        = DecimalUtils::convertTo<Decimal128>(request_timestamp_scale, max_storage_timestamp, timestamp_scale);

    /// A range entirely before or after the representable storage domain cannot overlap any series.
    if ((end_time && *end_time < min_timestamp) || (start_time && *start_time > max_timestamp))
        return false;

    /// Clamp only bounds that extend beyond the domain. The resulting native-type predicates are then
    /// equivalent for every representable timestamp and cannot wrap during AST conversion.
    if (start_time && *start_time < min_timestamp)
        *start_time = min_timestamp;
    if (end_time && *end_time > max_timestamp)
        *end_time = max_timestamp;

    return true;
}

/// Reduce a request timestamp to the storage precision while preserving an inclusive overlap predicate.
/// A lower bound on max_time needs a ceiling; an upper bound on min_time needs a floor. DecimalUtils' generic
/// scale conversion truncates towards zero, which is not a directional rounding operation for negative values.
DateTime64 rescaleTimestampBound(const Decimal128 & value, UInt32 storage_scale, UInt32 request_scale, bool round_up)
{
    if (storage_scale >= request_scale)
        return DecimalUtils::convertTo<DateTime64>(storage_scale, value, request_scale);

    const auto divisor = DecimalUtils::scaleMultiplier<Decimal128>(request_scale - storage_scale);
    auto quotient = value.value / divisor;
    const auto remainder = value.value % divisor;

    if (remainder != 0 && ((round_up && value.value > 0) || (!round_up && value.value < 0)))
        quotient += round_up ? Int128{1} : Int128{-1};

    return DateTime64{static_cast<Int64>(quotient)};
}

Decimal64 parsePrometheusLookbackDelta(const String & value, UInt32 timestamp_scale)
{
    const auto high_precision_value = parseTimeSeriesDuration(value, LOOKBACK_DELTA_SCALE);
    if (high_precision_value <= 0 || timestamp_scale >= LOOKBACK_DELTA_SCALE)
        return high_precision_value;

    const auto divisor = DecimalUtils::scaleMultiplier<Decimal64>(LOOKBACK_DELTA_SCALE - timestamp_scale);
    auto timestamp_ticks = high_precision_value.value / divisor;
    if (high_precision_value.value % divisor)
        ++timestamp_ticks;

    return Decimal64{timestamp_ticks};
}

/// SQL expression that rejects a row of the `tags` table carrying different non-empty values for the same
/// tag in its dedicated `tags_to_columns` column and in the residual `tags` Map. Such a row is invalid: it
/// is rejected by `timeSeriesStoreTags` on the write path and by the metadata endpoints, so the remaining
/// metadata endpoints must not report label names or label values derived from it either - otherwise they
/// would hide the corruption while the other endpoints fail. Returns `throwIf(...)`, i.e. `0` on a valid row
/// (fail closed on an invalid one).
String makeTagCarrierConflictCheck(const String & tag_name, const String & column_name)
{
    /// A dedicated tag column may be Nullable (a series without the tag stores NULL there), so normalize
    /// NULL to '' - as `timeSeriesTagNameToAST` does - before comparing the two carriers.
    String column_expr = fmt::format("coalesce(toString({}), '')", backQuoteIfNeed(column_name));
    String map_expr = fmt::format("{}[{}]", TimeSeriesColumnNames::Tags, quoteString(tag_name));
    return fmt::format(
        "throwIf(({0} != '') AND ({1} != '') AND ({0} != {1}), {2})",
        column_expr,
        map_expr,
        quoteString(fmt::format(
            "Found two tags with the same name {} but different values in a row of the 'tags' table", quoteString(tag_name))));
}

/// Validate the raw logical label pairs before a metadata endpoint projects them to label names. The
/// expression returns `__name__` for a valid row so it can also provide the virtual label name in the result.
String makeLabelRowValidationExpression(const String & metric_name, const String & label_pairs)
{
    String result = quoteString(TimeSeriesTagNames::MetricName);

    result = fmt::format(
        "if(throwIf(arrayExists(x -> (isValidUTF8(x.1) = 0) OR (isValidUTF8(x.2) = 0), {0}), {1}), '', {2})",
        label_pairs,
        quoteString("Found invalid UTF-8 in a label name or value in a row of the 'tags' table"),
        result);
    result = fmt::format(
        "if(throwIf(arrayExists(x -> (x.1 = ''), {0}), {1}), '', {2})",
        label_pairs,
        quoteString("Found a tag with an empty name in a row of the 'tags' table"),
        result);
    result = fmt::format(
        "if(throwIf(isValidUTF8({0}) = 0, {1}), '', {2})",
        metric_name,
        quoteString("Found invalid UTF-8 in a metric name in a row of the 'tags' table"),
        result);
    result = fmt::format(
        "if(throwIf({0} = '', {1}), '', {2})",
        metric_name,
        quoteString("Found empty metric name in a row of the 'tags' table"),
        result);

    return result;
}
}

PrometheusHTTPProtocolAPI::PrometheusHTTPProtocolAPI(ConstStoragePtr time_series_storage_, const ContextMutablePtr & context_)
    : WithMutableContext{context_}
    , time_series_storage(storagePtrToTimeSeries(time_series_storage_))
    , log(getLogger("PrometheusHTTPProtocolAPI"))
{
}

PrometheusHTTPProtocolAPI::~PrometheusHTTPProtocolAPI() = default;

void PrometheusHTTPProtocolAPI::executePromQLQuery(
    WriteBuffer & response,
    const Params & params,
    QueryFinishCallback query_finish_callback)
{
    PrometheusQueryEvaluationSettings evaluation_settings;
    evaluation_settings.time_series_storage_id = time_series_storage->getStorageID();
    auto time_series_metadata = time_series_storage->getInMemoryMetadataPtr(getContext(), false);
    std::tie(evaluation_settings.timestamp_data_type, evaluation_settings.scalar_data_type)
        = splitTimeSeriesType(time_series_metadata->columns.get(TimeSeriesColumnNames::TimeSeries).type);
    UInt32 timestamp_scale = tryGetDecimalScale(*evaluation_settings.timestamp_data_type).value_or(0);

    if (!params.lookback_delta_param.empty())
    {
        const auto lookback_delta = parsePrometheusLookbackDelta(params.lookback_delta_param, timestamp_scale);
        if (lookback_delta > 0)
            evaluation_settings.instant_selector_window = lookback_delta;
    }

    auto query_tree = std::make_shared<PrometheusQueryTree>();
    query_tree->parse(params.promql_query, timestamp_scale);
    LOG_TRACE(log, "Parsed PromQL query: {}. Result type: {}", params.promql_query, query_tree->getResultType());

    if (params.type == Type::Instant)
    {
        evaluation_settings.mode = PrometheusQueryEvaluationMode::QUERY;
        if (params.time_param.empty())
        {
            evaluation_settings.use_current_time = true;
        }
        else
        {
            evaluation_settings.start_time = parseTimeSeriesTimestamp(params.time_param, timestamp_scale);
            evaluation_settings.end_time = evaluation_settings.start_time;
            evaluation_settings.step = 0;
        }
    }
    else if (params.type == Type::Range)
    {
        evaluation_settings.mode = PrometheusQueryEvaluationMode::QUERY_RANGE;
        evaluation_settings.start_time = parseTimeSeriesTimestamp(params.start_param, timestamp_scale);
        evaluation_settings.end_time = parseTimeSeriesTimestamp(params.end_param, timestamp_scale);
        evaluation_settings.step = parseTimeSeriesDuration(params.step_param, timestamp_scale);
    }

    PrometheusQueryToSQL::Converter converter{query_tree, evaluation_settings};
    auto sql_query = converter.getSQL();

    chassert(sql_query);
    LOG_TRACE(log, "SQL query to execute:\n{}", sql_query->formatForLogging());

    /// The generated SQL relies on `AS MATERIALIZED` to avoid evaluating subqueries referenced more than once
    /// repeatedly (see SQLSubqueryType::MATERIALIZED_TABLE), and that mark has effect only with the setting
    /// `enable_materialized_cte` enabled. Enable it unless the user set it explicitly.
    if (!getContext()->getSettingsRef()[Setting::enable_materialized_cte].changed)
        getContext()->setSetting("enable_materialized_cte", true);

    /// `AS MATERIALIZED` is honored by the analyzer only, so the generated SQL always runs the analyzer.
    getContext()->setSetting("allow_experimental_analyzer", true);

    auto [ast, io] = executeQuery(sql_query->formatWithSecretsOneLine(), getContext(), {}, QueryProcessingStage::Complete);

    try
    {
        PullingAsyncPipelineExecutor executor(io.pipeline);

        /// Mind using the getResultType() method from PrometheusQueryToSQL::Converter, not from the PrometheusQueryTree.
        writeQueryResponse(response, executor, converter.getResultType());

        /// Store the buffered result in the query result cache now (no-op if no cache writers exist in the pipeline):
        /// the executor's destructor cancels the pipeline processors, after which the pending write would be discarded.
        io.pipeline.finalizeWriteInQueryResultCache();
    }
    catch (...)
    {
        io.onException();
        throw;
    }

    /// Release the query slot early so a slow client draining the response does not keep occupying it,
    /// then flush the response (query_finish_callback) and record QueryFinish.
    finishExecutedQuery(io, query_finish_callback);
}

void PrometheusHTTPProtocolAPI::writeQueryResponse(
    WriteBuffer & response, PullingAsyncPipelineExecutor & pulling_executor, PrometheusQueryResultType result_type)
{
    /// Pull until the first non-empty block is ready before writing the header
    /// because pulling_executor.pull() can throw an exception and it's better to catch it early and write
    /// the correct error header {"status":"error", ...} in PrometheusRequestHandler::QueryImpl.
    bool has_output = false;
    Block block;
    while (pulling_executor.pull(block))
    {
        if (block.rows() > 0)
        {
            has_output = true;
            break;
        }
    }

    writeQueryResponseHeader(response, result_type);

    if (has_output)
    {
        writeQueryResponseBlock(response, result_type, block, /*first=*/ true);

        while (pulling_executor.pull(block))
        {
            if (block.rows() > 0)
                writeQueryResponseBlock(response, result_type, block, /*first=*/ false);
        }
    }

    writeQueryResponseFooter(response);
}

void PrometheusHTTPProtocolAPI::writeQueryResponseHeader(WriteBuffer & response, PrometheusQueryResultType result_type)
{
    std::string_view result_type_str;
    switch (result_type)
    {
        case PrometheusQueryTree::ResultType::SCALAR:
            result_type_str = "scalar";
            break;
        case PrometheusQueryTree::ResultType::STRING:
            result_type_str = "string";
            break;
        case PrometheusQueryTree::ResultType::INSTANT_VECTOR:
            result_type_str = "vector";
            break;
        case PrometheusQueryTree::ResultType::RANGE_VECTOR:
            result_type_str = "matrix";
            break;
    }
    chassert(!result_type_str.empty());
    writeString(R"({"status":"success","data":{"resultType":")", response);
    writeString(result_type_str, response);
    writeString(R"(","result":[)", response);
}

void PrometheusHTTPProtocolAPI::writeQueryResponseFooter(WriteBuffer & response)
{
    writeString("]}}", response);
}

void PrometheusHTTPProtocolAPI::writeQueryResponseBlock(WriteBuffer & response, PrometheusQueryResultType result_type, const Block & result_block, bool first)
{
    LOG_TRACE(log, "Prometheus: Writing {} result ({} rows)", result_type, result_block.rows());

    switch (result_type)
    {
        case PrometheusQueryTree::ResultType::SCALAR:
        {
            writeQueryResponseScalarBlock(response, result_block, first);
            return;
        }
        case PrometheusQueryTree::ResultType::STRING:
        {
            writeQueryResponseStringBlock(response, result_block, first);
            return;
        }
        case PrometheusQueryTree::ResultType::INSTANT_VECTOR:
        {
            writeQueryResponseInstantVectorBlock(response, result_block, first);
            return;
        }
        case PrometheusQueryTree::ResultType::RANGE_VECTOR:
        {
            writeQueryResponseRangeVectorBlock(response, result_block, first);
            return;
        }
    }
    UNREACHABLE();
}

void PrometheusHTTPProtocolAPI::writeQueryResponseScalarBlock(WriteBuffer & response, const Block & result_block, bool first)
{
    if (!first || (result_block.rows() > 1))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Prometheus query outputs multiple rows but expected to return a scalar");

    // Write timestamp
    const auto & timestamp_column = result_block.getByName(TimeSeriesColumnNames::Timestamp).column;
    auto timestamp_data_type = result_block.getByName(TimeSeriesColumnNames::Timestamp).type;
    UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_data_type).value_or(0);
    DateTime64 timestamp = timestamp_column->getInt(0);
    writeTimestamp(response, timestamp, timestamp_scale);

    writeString(",", response);

    // Write value
    const auto & scalar_column = result_block.getByName(TimeSeriesColumnNames::Value).column;
    Float64 value = scalar_column->getFloat64(0);
    writeString("\"", response);
    writeScalar(response, value);
    writeString("\"", response);
}

void PrometheusHTTPProtocolAPI::writeQueryResponseStringBlock(WriteBuffer & response, const Block & result_block, bool first)
{
    if (!first || (result_block.rows() > 1))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Prometheus query outputs multiple rows but expected to return a string");

    // Write timestamp
    const auto & timestamp_column = result_block.getByName(TimeSeriesColumnNames::Timestamp).column;
    auto timestamp_data_type = result_block.getByName(TimeSeriesColumnNames::Timestamp).type;
    UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_data_type).value_or(0);
    DateTime64 timestamp = timestamp_column->getInt(0);
    writeTimestamp(response, timestamp, timestamp_scale);

    writeString(",", response);

    // Write value
    const auto & string_column = result_block.getByName(TimeSeriesColumnNames::Value).column;
    auto value = string_column->getDataAt(0);
    writeJSONString(value, response, format_settings);
}

void PrometheusHTTPProtocolAPI::writeQueryResponseInstantVectorBlock(WriteBuffer & response, const Block & result_block, bool first)
{
    const auto & timestamp_column = result_block.getByName(TimeSeriesColumnNames::Timestamp).column;
    auto timestamp_data_type = result_block.getByName(TimeSeriesColumnNames::Timestamp).type;
    UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_data_type).value_or(0);
    const auto & value_column = result_block.getByName(TimeSeriesColumnNames::Value).column;

    bool need_comma = !first;

    for (size_t i = 0; i < result_block.rows(); ++i)
    {
        if (need_comma)
            writeString(",", response);

        writeString("{", response);

        // Write metric labels
        writeString(R"("metric":)", response);
        writeTags(response, result_block, i);

        writeString(",", response);

        // Write value [timestamp, "value"]
        writeString("\"value\":[", response);

        // Write timestamp
        DateTime64 timestamp = timestamp_column->getInt(i);
        writeTimestamp(response, timestamp, timestamp_scale);

        writeString(",", response);

        // Write value
        Float64 value = value_column->getFloat64(i);
        writeString("\"", response);
        writeScalar(response, value);
        writeString("\"", response);

        writeString("]}", response);
        need_comma = true;
    }
}

void PrometheusHTTPProtocolAPI::writeQueryResponseRangeVectorBlock(WriteBuffer & response, const Block & result_block, bool first)
{
    const auto & time_series_column = result_block.getByName(TimeSeriesColumnNames::TimeSeries).column;
    const auto & array_column = typeid_cast<const ColumnArray &>(*time_series_column);
    const auto & offsets = array_column.getOffsets();
    const auto & tuple_column = typeid_cast<const ColumnTuple &>(array_column.getData());
    const auto & timestamp_column = tuple_column.getColumn(0);
    const auto & value_column = tuple_column.getColumn(1);

    auto timestamp_data_type
        = typeid_cast<const DataTypeTuple &>(
              *typeid_cast<const DataTypeArray &>(*result_block.getByName(TimeSeriesColumnNames::TimeSeries).type).getNestedType())
              .getElement(0);

    UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_data_type).value_or(0);

    bool need_comma = !first;

    for (size_t i = 0; i < result_block.rows(); ++i)
    {
        if (need_comma)
            writeString(",", response);

        writeString("{", response);

        // Write labels
        writeString(R"("metric":)", response);
        writeTags(response, result_block, i);
        writeString(",", response);

        // Extract time series data
        writeString(R"("values":[)", response);

        size_t start = (i == 0) ? 0 : offsets[i-1];
        size_t end = offsets[i];

        for (size_t j = start; j < end; ++j)
        {
            if (j > start)
                writeString(",", response);

            writeString("[", response);
            DateTime64 timestamp = timestamp_column.getInt(j);
            writeTimestamp(response, timestamp, timestamp_scale);
            writeString(",\"", response);
            Float64 value = value_column.getFloat64(j);
            writeScalar(response, value);
            writeString("\"]", response);
        }

        writeString("]}", response);
        need_comma = true;
    }
}


std::vector<std::pair<String, String>> PrometheusHTTPProtocolAPI::getConfiguredTagColumns() const
{
    std::vector<std::pair<String, String>> result;
    auto settings = time_series_storage->getStorageSettings();
    const Map & tags_to_columns = (*settings)[TimeSeriesSetting::tags_to_columns];
    for (const auto & tag_name_and_column_name : tags_to_columns)
    {
        const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
        const auto & tag_name = tuple.at(0).safeGet<String>();
        const auto & column_name = tuple.at(1).safeGet<String>();
        result.emplace_back(tag_name, column_name);
    }
    return result;
}

void PrometheusHTTPProtocolAPI::appendTimeRangeConditions(
    std::vector<String> & conditions, const StoragePtr & tags_table, const String & start_param, const String & end_param)
{
    if (start_param.empty() && end_param.empty())
        return;

    /// `min_time`/`max_time` store the timestamp type of the `TimeSeries` table (`DateTime64(X)`, `DateTime`,
    /// `UInt32`, ...), which is not necessarily `DateTime64(3)`. Derive that type/scale from the `time_series`
    /// column the same way the PromQL query path does, and build the comparison literals with the same
    /// conversion path (`timeSeriesTimestampToAST`), so no precision is lost for higher-scale tables and the
    /// comparison type matches the column for non-`DateTime64` timestamps.
    auto time_series_metadata = time_series_storage->getInMemoryMetadataPtr(getContext(), false);
    auto timestamp_data_type
        = splitTimeSeriesType(time_series_metadata->columns.get(TimeSeriesColumnNames::TimeSeries).type).first;
    UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_data_type).value_or(0);

    /// Parse both bounds in a wider request representation so validation does not depend on the native
    /// precision or range of this particular storage table. Parsing at DateTime64(0), for example, would
    /// collapse `start=1000.9&end=1000.1` to two equal values and accept an inverted range. Retry at a
    /// lower common scale when a valid large Unix timestamp cannot fit at the initially requested scale,
    /// but never reduce below the storage scale.
    UInt32 request_timestamp_scale = std::max(timestamp_scale, LOOKBACK_DELTA_SCALE);
    std::optional<Decimal128> start_time;
    std::optional<Decimal128> end_time;

    auto parse_request_timestamp = [](const String & value, UInt32 scale)
    {
        PrometheusQueryParsingUtil::RequestTimestampType timestamp;
        String error_message;
        size_t error_pos = 0;
        if (PrometheusQueryParsingUtil::tryParsePrometheusRequestTimestamp(
                value, scale, timestamp, &error_message, &error_pos))
            return timestamp;

        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} at position {}", error_message, error_pos);
    };

    for (UInt32 candidate_timestamp_scale = request_timestamp_scale;; --candidate_timestamp_scale)
    {
        try
        {
            std::optional<Decimal128> candidate_start_time;
            std::optional<Decimal128> candidate_end_time;
            if (!start_param.empty())
                candidate_start_time = parse_request_timestamp(start_param, candidate_timestamp_scale);
            if (!end_param.empty())
                candidate_end_time = parse_request_timestamp(end_param, candidate_timestamp_scale);

            start_time = candidate_start_time;
            end_time = candidate_end_time;
            request_timestamp_scale = candidate_timestamp_scale;
            break;
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::BAD_ARGUMENTS || candidate_timestamp_scale == timestamp_scale)
                throw;
        }
    }

    if (start_time && end_time && *start_time > *end_time)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "start must not be greater than end");

    /// Clip the validated request bounds before reducing them to storage precision. This also handles
    /// values just outside the UInt32 domain, such as `end=-0.1` or `start=4294967295.9`, which would
    /// otherwise truncate into the representable domain before the check.
    if (!clipTimestampRangeToStorageType(
            start_time, end_time, timestamp_data_type, timestamp_scale, request_timestamp_scale))
    {
        /// Keep the query path uniform while making an out-of-domain range return no metadata rows.
        conditions.emplace_back("0");
        return;
    }

    /// Convert the already validated and clipped request bounds separately at storage precision. For
    /// unsigned timestamp types this conversion is safe only after the domain check above.
    std::optional<DateTime64> native_start_time;
    std::optional<DateTime64> native_end_time;
    if (start_time)
        native_start_time = rescaleTimestampBound(*start_time, timestamp_scale, request_timestamp_scale, /* round_up */ true);
    if (end_time)
        native_end_time = rescaleTimestampBound(*end_time, timestamp_scale, request_timestamp_scale, /* round_up */ false);

    /// If the request interval is narrower than the storage precision, directional rounding can leave no
    /// representable timestamp in the interval. Make the empty overlap explicit instead of relying on the
    /// query optimizer to simplify contradictory DateTime64 predicates.
    if (native_start_time && native_end_time && *native_start_time > *native_end_time)
    {
        conditions.emplace_back("0");
        return;
    }

    /// `StorageTimeSeriesSelector::readImpl` uses the tags-table bounds only when both settings are enabled;
    /// otherwise the query path falls back to exact samples-table filtering. The metadata endpoint has no
    /// samples-table fallback, so follow Prometheus's approximate /series contract and return a superset when
    /// trusted bounds are unavailable instead of rejecting an otherwise valid request with `bad_data`.
    auto time_series_settings = time_series_storage->getStorageSettings();
    if (!(*time_series_settings)[TimeSeriesSetting::filter_by_min_time_and_max_time]
        || !(*time_series_settings)[TimeSeriesSetting::store_min_time_and_max_time])
    {
        LOG_DEBUG(
            log,
            "Ignoring the /api/v1/series time range because min_time/max_time filtering is unavailable; returning an approximate superset");
        return;
    }

    auto tags_metadata = tags_table->getInMemoryMetadataPtr(getContext(), false);
    if (!tags_metadata->columns.has(TimeSeriesColumnNames::MinTime) || !tags_metadata->columns.has(TimeSeriesColumnNames::MaxTime))
    {
        LOG_DEBUG(
            log,
            "Ignoring the /api/v1/series time range because the tags table has no min_time/max_time columns; returning an approximate superset");
        return;
    }

    /// A series overlaps the requested range [start, end] when its `max_time >= start` and `min_time <= end`.
    /// This mirrors `StorageTimeSeriesSelector::makeWhereFilterForTagsTable`, which the real query path
    /// (`/api/v1/query`, `/api/v1/query_range`) uses on the `tags` table. There the comparisons are plain, so a
    /// `NULL` `min_time`/`max_time` makes the predicate evaluate to `NULL` and the row is dropped. An empty
    /// `time_series` row (a series with no samples) is stored with `NULL` bounds by
    /// `TimeSeriesSink::fillMinMaxTimeColumns`, and such a series never contributes to a ranged query. The
    /// metadata endpoints must fail closed the same way: keeping those rows with an `IS NULL OR ...` branch
    /// would let `/api/v1/series`, `/api/v1/labels`, and `/api/v1/label/<name>/values` report series and labels
    /// for an interval where `/api/v1/query` returns no data.
    if (native_start_time)
        conditions.push_back(fmt::format(
            "{0} >= {1}",
            TimeSeriesColumnNames::MaxTime,
            timeSeriesTimestampToAST(*native_start_time, timestamp_data_type)->formatWithSecretsOneLine()));

    if (native_end_time)
        conditions.push_back(fmt::format(
            "{0} <= {1}",
            TimeSeriesColumnNames::MinTime,
            timeSeriesTimestampToAST(*native_end_time, timestamp_data_type)->formatWithSecretsOneLine()));
}

/// Implements /api/v1/series: returns time series matching the supplied series selectors.
/// Queries the tags table and serializes each series as a JSON object with __name__ and all tag key-value pairs.
void PrometheusHTTPProtocolAPI::getSeries(
    WriteBuffer & response,
    const Strings & match_params,
    const String & start_param,
    const String & end_param,
    UInt64 limit,
    QueryFinishCallback query_finish_callback)
{
    /// Prometheus requires at least one `match[]` selector here; without it the endpoint would scan the whole tags table.
    if (match_params.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The Prometheus /api/v1/series endpoint requires at least one 'match[]' series selector");

    auto time_series_metadata = time_series_storage->getInMemoryMetadataPtr(getContext(), false);
    auto timestamp_data_type = splitTimeSeriesType(time_series_metadata->columns.get(TimeSeriesColumnNames::TimeSeries).type).first;
    UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_data_type).value_or(0);

    /// The optional `start` and `end` parameters are parsed the same way as on the query endpoints.
    std::optional<DateTime64> min_time;
    std::optional<DateTime64> max_time;
    if (!start_param.empty())
        min_time = parseTimeSeriesTimestamp(start_param, timestamp_scale);
    if (!end_param.empty())
        max_time = parseTimeSeriesTimestamp(end_param, timestamp_scale);
    if (min_time && max_time && (*max_time < *min_time))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'start' must not be greater than 'end'");

    /// Like the query path, filter by the [min_time, max_time] stored in the tags table; without stored bounds the range is ignored (a superset is allowed).
    auto time_series_settings = time_series_storage->getStorageSettings();
    if (!(*time_series_settings)[TimeSeriesSetting::filter_by_min_time_and_max_time]
        || !(*time_series_settings)[TimeSeriesSetting::store_min_time_and_max_time])
    {
        min_time.reset();
        max_time.reset();
    }

    auto tags_table = time_series_storage->getTargetTable(ViewTarget::Tags, getContext());
    auto tags_table_id = tags_table->getStorageID();
    bool tags_table_is_remote = tags_table->isRemote();

    /// Each `match[]` value must be an instant selector; the result is the union of the series matched by each selector.
    String inner_query;
    for (const auto & match_param : match_params)
    {
        PrometheusQueryTree selector;
        String error_message;
        if (!selector.tryParse(match_param, timestamp_scale, &error_message))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse the value {} of the 'match[]' parameter: {}",
                            quoteString(match_param), error_message);

        const auto * root = selector.getRoot();
        if (!root || (root->node_type != PrometheusQueryTree::NodeType::InstantSelector))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value {} of the 'match[]' parameter is not an instant selector",
                            quoteString(match_param));

        const auto & matchers = typeid_cast<const PrometheusQueryTree::InstantSelector &>(*root).matchers;
        if (matchers.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value {} of the 'match[]' parameter must contain at least one matcher",
                            quoteString(match_param));

        if (!inner_query.empty())
            inner_query += " UNION ALL ";
        inner_query += StorageTimeSeriesSelector::makeSelectIDsQuery(
            tags_table_id,
            matchers,
            *time_series_settings,
            min_time,
            max_time,
            timestamp_data_type,
            tags_table_is_remote)->formatWithSecretsOneLine();
    }

    /// timeSeriesIdToTags() returns the tags registered by the inner query (including `__name__`); `DISTINCT` dedups, and one extra row detects truncation.
    String sql_query = fmt::format(
        "SELECT DISTINCT timeSeriesIdToTags(series_id) AS {} FROM ({})", TimeSeriesColumnNames::Tags, inner_query);
    if (limit)
        sql_query += fmt::format(" LIMIT {}", limit + 1);

    LOG_TRACE(log, "SQL query to execute:\n{}", sql_query);

    /// Functions timeSeriesStoreTags() and timeSeriesIdToTags() are supported by the analyzer only.
    getContext()->setSetting("allow_experimental_analyzer", true);

    auto [ast, io] = executeQuery(sql_query, getContext(), {}, QueryProcessingStage::Complete);

    try
    {
        PullingAsyncPipelineExecutor executor(io.pipeline);

        /// Pull the first non-empty block before writing the header so an early exception still produces the correct error response.
        bool has_output = false;
        Block block;
        while (executor.pull(block))
        {
            if (block.rows() > 0)
            {
                has_output = true;
                break;
            }
        }

        writeString(R"({"status":"success","data":[)", response);

        UInt64 written = 0;
        bool truncated = false;

        auto write_block = [&](const Block & result_block)
        {
            for (size_t i = 0; i < result_block.rows(); ++i)
            {
                if (limit && (written == limit))
                {
                    truncated = true;
                    return;
                }
                if (written)
                    writeString(",", response);
                writeTags(response, result_block, i);
                ++written;
            }
        };

        if (has_output)
        {
            write_block(block);
            while (!truncated && executor.pull(block))
            {
                if (block.rows() > 0)
                    write_block(block);
            }
        }

        if (truncated)
            writeString(R"(],"warnings":["results truncated due to limit"]})", response);
        else
            writeString("]}", response);

        /// Finalize the query result cache write before the executor's destructor cancels the pipeline; a truncated (incomplete) result must not be cached.
        if (!truncated)
            io.pipeline.finalizeWriteInQueryResultCache();
    }
    catch (...)
    {
        io.onException();
        throw;
    }

    /// Release the query slot early, flush the response and record QueryFinish.
    finishExecutedQuery(io, query_finish_callback);
}

void PrometheusHTTPProtocolAPI::getLabels(
    WriteBuffer & response,
    const Strings & match_params,
    const String & start_param,
    const String & end_param,
    UInt64 limit,
    QueryFinishCallback query_finish_callback)
{
    auto tags_table = time_series_storage->getTargetTable(ViewTarget::Tags, getContext());
    const auto & time_series_table_id = time_series_storage->getStorageID();

    /// Read through the TimeSeries-owned table function so the configured TimeSeries table remains the
    /// authorization boundary. The physical tags target can be an inner implementation table without a
    /// separate SELECT grant.
    const String tags_table_expression = fmt::format(
        "timeSeriesTags({}, {})",
        quoteString(time_series_table_id.database_name),
        quoteString(time_series_table_id.table_name));

    /// Labels live in two places: keys of the `tags` Map column and, for tags configured via
    /// `tags_to_columns`, dedicated columns. A configured tag is reported only when at least one series
    /// has a non-empty value for it.
    auto tag_columns = getConfiguredTagColumns();

    /// An empty label value means the label is absent (the write path strips such entries, and
    /// `/api/v1/series` and the matcher translation treat them as the missing case), but a supported
    /// external `tags` table can still contain malformed rows. Keep non-empty values, while retaining
    /// empty-name entries until the API validation boundary so malformed data is rejected instead of hidden.
    String label_pairs_expr = fmt::format("arrayZip(mapKeys({0}), mapValues({0}))", TimeSeriesColumnNames::Tags);
    if (!tag_columns.empty())
    {
        String configured;
        for (size_t i = 0; i < tag_columns.size(); ++i)
        {
            if (i != 0)
                configured += ", ";
            configured += fmt::format(
                "({}, if({}, '', coalesce(toString({}), '')))",
                quoteString(tag_columns[i].first),
                makeTagCarrierConflictCheck(tag_columns[i].first, tag_columns[i].second),
                backQuoteIfNeed(tag_columns[i].second));
        }
        label_pairs_expr = fmt::format("arrayConcat({}, [{}])", label_pairs_expr, configured);
    }

    label_pairs_expr = fmt::format(
        "arrayConcat({}, [({}, if({}, '', {}))])",
        label_pairs_expr,
        quoteString(TimeSeriesTagNames::MetricName),
        makeTagCarrierConflictCheck(TimeSeriesTagNames::MetricName, TimeSeriesColumnNames::MetricName),
        TimeSeriesColumnNames::MetricName);

    String normalized_label_pairs_expr = fmt::format(
        "arraySort(arrayDistinct(arrayFilter(x -> (x.2 != '' OR x.1 = ''), {})))",
        label_pairs_expr);
    String duplicate_label_check = fmt::format(
        "throwIf(length({0}) != length(arrayDistinct(arrayMap(x -> x.1, {0}))), {1})",
        normalized_label_pairs_expr,
        quoteString("Found two labels with the same name but different values in a row of the 'tags' table"));
    String validated_metric_name_expr = makeLabelRowValidationExpression(
        TimeSeriesColumnNames::MetricName,
        label_pairs_expr);
    String validated_row_expr = fmt::format(
        "if({}, '', {})",
        duplicate_label_check,
        validated_metric_name_expr);
    String non_metric_label_pairs_expr = fmt::format(
        "arrayFilter(x -> x.1 != {}, {})",
        quoteString(TimeSeriesTagNames::MetricName),
        normalized_label_pairs_expr);
    String label_keys_expr = fmt::format(
        "arrayConcat([{}], arrayMap(x -> toString(x.1), {}))",
        validated_row_expr,
        non_metric_label_pairs_expr);

    auto tags_metadata = tags_table->getInMemoryMetadataPtr(getContext(), false);
    String label_key_alias = "__prometheus_label_key";
    while (tags_metadata->columns.has(label_key_alias))
        label_key_alias += "_";

    String label_array_alias = "__prometheus_label_keys";
    while (tags_metadata->columns.has(label_array_alias))
        label_array_alias += "_";

    /// Query distinct label keys. __name__ is included as a virtual label in every matching series' array,
    /// so a selector or time range matching nothing produces an empty list, while a matching series always
    /// contributes at least this one label. Use aliases that cannot collide with source columns: otherwise
    /// a `tags_to_columns` column with the same name would shadow the array-joined label in match conditions.
    /// The `ARRAY JOIN` clause is used instead of the `arrayJoin` function on purpose: `arrayJoin`
    /// reports `isDeterministic() = false` (it returns many rows for a single argument), so a query
    /// using it is rejected by the query result cache under the default
    /// `query_cache_nondeterministic_function_handling = 'throw'`, breaking `use_query_cache=1` on
    /// this endpoint. The clause form is semantically identical here and is not a function call.
    std::vector<String> conditions;
    std::unordered_map<String, String> column_name_by_tag_name(tag_columns.begin(), tag_columns.end());
    if (String match_condition = makeMatchCondition(match_params, column_name_by_tag_name); !match_condition.empty())
        conditions.push_back(match_condition);
    appendTimeRangeConditions(conditions, tags_table, start_param, end_param);

    String filtered_tags_query = fmt::format("SELECT * FROM {}", tags_table_expression);
    for (size_t i = 0; i < conditions.size(); ++i)
        filtered_tags_query += (i == 0 ? " WHERE " : " AND ") + conditions[i];

    String query = fmt::format(
        "SELECT DISTINCT {0} FROM (SELECT *, {1} AS {2} FROM ({3})) LEFT ARRAY JOIN {2} AS {0}",
        label_key_alias,
        label_keys_expr,
        label_array_alias,
        filtered_tags_query);

    query += " ORDER BY " + label_key_alias;

    /// `limit` is deliberately NOT pushed down as a SQL `LIMIT`: the conflicting-carrier `throwIf`
    /// check embedded in `label_keys_expr` must be evaluated on every matched row to fail closed, and
    /// a SQL-side `LIMIT` lets the scan stop early, so whether a corrupted row is detected would
    /// depend on the caller's `limit` and on row order. Like `/api/v1/query`, the limit is enforced
    /// in the emission loop instead.

    LOG_TRACE(log, "Prometheus labels query: {}", query);

    auto [ast, io] = executeQuery(query, getContext(), {}, QueryProcessingStage::Complete);

    try
    {
        PullingAsyncPipelineExecutor executor(io.pipeline);
        Block result_block;

        auto validate_label_name = [](const auto & label)
        {
            if (label.empty())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Found a tag with an empty name in a row of the 'tags' table");

            if (!UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(label.data()), label.size()))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Found invalid UTF-8 in a label name in a row of the 'tags' table");
        };

        std::vector<String> labels_to_write;
        UInt64 emitted = 0;
        bool truncated = false;
        while (executor.pull(result_block))
        {
            if (result_block.rows() == 0)
                continue;

            const auto & label_col = result_block.getByName(label_key_alias).column;
            for (size_t i = 0; i < result_block.rows(); ++i)
            {
                auto label = label_col->getDataAt(i);
                validate_label_name(label);
                if (limit && emitted == limit)
                {
                    truncated = true;
                    continue;
                }
                labels_to_write.emplace_back(label.data(), label.size());
                ++emitted;
            }
        }

        writeString(R"({"status":"success","data":[)", response);
        for (size_t i = 0; i < labels_to_write.size(); ++i)
        {
            if (i > 0)
                writeString(",", response);
            writeJSONString(std::string_view(labels_to_write[i]), response, format_settings);
        }

        writeMetadataResponseFooter(response, truncated);

        /// Store the buffered result in the query result cache now (no-op if no cache writers exist in the pipeline):
        /// the executor's destructor cancels the pipeline processors, after which the pending write would be discarded.
        io.pipeline.finalizeWriteInQueryResultCache();
    }
    catch (...)
    {
        io.onException();
        throw;
    }

    /// Release the query slot early and record QueryFinish in system.query_log, mirroring executePromQLQuery.
    /// BlockIO::~BlockIO only resets the pipeline and never runs the finish/exception callbacks, so without
    /// this a successful metadata request would never emit a QueryFinish entry and would keep the query slot
    /// occupied until the whole HTTP response is written instead of releasing it when the pipeline is exhausted.
    finishExecutedQuery(io, query_finish_callback);
}

void PrometheusHTTPProtocolAPI::getLabelValues(
    WriteBuffer & response,
    const String & /* label_name */,
    const String & /* match_param */,
    const String & /* start_param */,
    const String & /* end_param */)
{
    UNUSED(response);
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The label values endpoint is not implemented");
}


void PrometheusHTTPProtocolAPI::writeTags(WriteBuffer & response, const Block & result_block, size_t row_index)
{
    const auto & tags_column = result_block.getByName(TimeSeriesColumnNames::Tags).column;
    const auto & array_column = typeid_cast<const ColumnArray &>(*tags_column);
    const auto & offsets = array_column.getOffsets();
    const auto & tuple_column = typeid_cast<const ColumnTuple &>(array_column.getData());
    const auto & key_column = tuple_column.getColumn(0);
    const auto & value_column = tuple_column.getColumn(1);

    writeString("{", response);

    size_t start = (row_index == 0) ? 0 : offsets[row_index - 1];
    size_t end = offsets[row_index];

    for (size_t j = start; j < end; ++j)
    {
        if (j > start)
            writeString(",", response);

        auto key = key_column.getDataAt(j);
        writeJSONString(key, response, format_settings);

        writeString(":", response);

        auto value = value_column.getDataAt(j);
        writeJSONString(value, response, format_settings);
    }

    writeString("}", response);
}


void PrometheusHTTPProtocolAPI::writeTimestamp(WriteBuffer & response, DateTime64 value, UInt32 scale)
{
    writeText(value, scale, response);
}

void PrometheusHTTPProtocolAPI::writeScalar(WriteBuffer & response, Float64 value)
{
    if (std::isfinite(value))
    {
        writeFloatText(value, response);
    }
    else if (std::isinf(value))
    {
        response.write((value > 0) ? '+' : '-');
        writeString("Inf", response);
    }
    else
    {
        writeString("NaN", response);
    }
}


}
