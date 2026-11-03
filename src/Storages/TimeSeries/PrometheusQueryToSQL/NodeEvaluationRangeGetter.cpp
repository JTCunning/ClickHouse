#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRangeGetter.h>

#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationSettings.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/alignTimestampWithStep.h>


namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// By default the lookback period is 5 minutes.
    constexpr const Int64 DEFAULT_INSTANT_SELECTOR_WINDOW_SECONDS = 5 * 60;

    /// The default subquery step is 15 seconds.
    constexpr const Int64 DEFAULT_SUBQUERY_STEP_SECONDS = 15;

    bool needsTwoSamples(std::string_view function_name)
    {
        return function_name == "rate" || function_name == "increase" || function_name == "delta"
            || function_name == "irate" || function_name == "idelta" || function_name == "deriv"
            || function_name == "changes" || function_name == "resets" || function_name == "predict_linear";
    }
}

NodeEvaluationRangeGetter::NodeEvaluationRangeGetter(std::shared_ptr<const PrometheusQueryTree> promql_tree_,
                                                     const PrometheusQueryEvaluationSettings & settings_)
    : promql_tree(promql_tree_)
    , time_scale(settings_.time_scale)
    , downsampling_periods(settings_.downsampling_periods)
    , adjust_window_for_downsampling(settings_.adjust_window_for_downsampling)
    , downsampling_now_seconds(settings_.downsampling_now_seconds)
{
    if (promql_tree->getTimeScale() != time_scale)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL query was parsed with time scale {} but the evaluation settings use time scale {}",
                        promql_tree->getTimeScale(), time_scale);
    }

    /// By default the lookback period is 5 minutes.
    if (settings_.instant_selector_window)
        instant_selector_window = *settings_.instant_selector_window;
    else
        instant_selector_window = DEFAULT_INSTANT_SELECTOR_WINDOW_SECONDS * DecimalUtils::scaleMultiplier<DurationType>(time_scale);

    /// The default subquery step is 15 seconds.
    if (settings_.default_subquery_step)
        default_subquery_step = *settings_.default_subquery_step;
    else
        default_subquery_step = DEFAULT_SUBQUERY_STEP_SECONDS * DecimalUtils::scaleMultiplier<DurationType>(time_scale);

    const auto * root = promql_tree->getRoot();
    if (!root)
        return;

    NodeEvaluationRange range{};

    if (settings_.use_current_time)
    {
        range.start_time = DecimalUtils::getCurrentDateTime64(time_scale);
        range.end_time = range.start_time;
        range.step = 0;
    }
    else
    {
        if (!settings_.start_time)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "start_time is not specified");
        if (!settings_.end_time)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "end_time is not specified");
        if (*settings_.start_time > *settings_.end_time)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "start_time must not be greater than end_time");
        const bool has_range = *settings_.start_time < *settings_.end_time;
        const bool is_query_range = settings_.mode == PrometheusQueryEvaluationMode::QUERY_RANGE;
        if (has_range || is_query_range)
        {
            if (!settings_.step)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "step is not specified");
            if (*settings_.step <= 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "step must be positive");

            if (is_query_range && settings_.max_points_per_series)
            {
                UInt64 num_steps = (static_cast<UInt64>(settings_.end_time->value) - static_cast<UInt64>(settings_.start_time->value))
                    / static_cast<UInt64>(settings_.step->value);
                if (num_steps > settings_.max_points_per_series)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "exceeded maximum resolution of {} points per timeseries. "
                                    "Try decreasing the query resolution (?step=XX) or increasing the setting promql_max_points_per_series",
                                    settings_.max_points_per_series);
            }
        }
        range.start_time = *settings_.start_time;
        range.end_time = *settings_.end_time;
        range.step = (has_range || is_query_range) ? *settings_.step : DurationType{0};
    }

    query_start_time = range.start_time;
    query_end_time = range.end_time;

    visitNode(root, range);
    setWindows();
}


void NodeEvaluationRangeGetter::visitNode(const Node * node, const NodeEvaluationRange & range)
{
    map[node] = range;
    visitChildren(node, range);
}


void NodeEvaluationRangeGetter::visitChildren(const Node * node, const NodeEvaluationRange & range)
{
    switch (node->node_type)
    {
        case NodeType::Offset:
        {
            const auto * offset_node = static_cast<const PrometheusQueryTree::Offset *>(node);
            const auto * expression = offset_node->getExpression();
            NodeEvaluationRange expression_range = range;
            switch (offset_node->at_modifier)
            {
                case PrometheusQueryTree::Offset::AtModifier::None:
                    break;
                case PrometheusQueryTree::Offset::AtModifier::Timestamp:
                    chassert(offset_node->at_timestamp);
                    expression_range.start_time = *offset_node->at_timestamp;
                    expression_range.end_time = *offset_node->at_timestamp;
                    break;
                case PrometheusQueryTree::Offset::AtModifier::Start:
                    expression_range.start_time = query_start_time;
                    expression_range.end_time = query_start_time;
                    break;
                case PrometheusQueryTree::Offset::AtModifier::End:
                    expression_range.start_time = query_end_time;
                    expression_range.end_time = query_end_time;
                    break;
            }
            if (auto offset_value = offset_node->offset_value)
            {
                expression_range.start_time -= *offset_value;
                expression_range.end_time -= *offset_value;
            }
            visitNode(expression, expression_range);
            break;
        }

        case NodeType::Subquery:
        {
            const auto * subquery_node = static_cast<const PrometheusQueryTree::Subquery *>(node);
            auto subquery_range = subquery_node->range;

            DurationType step;
            if (auto subquery_step = subquery_node->step)
                step = *subquery_step;
            else
                step = default_subquery_step;

            const auto * expression = subquery_node->getExpression();
            NodeEvaluationRange expression_range = range;

            /// Subqueries use times that are aligned with the step.
            expression_range.start_time = alignTimestampWithStep(range.start_time - subquery_range + step, step);
            expression_range.end_time = alignTimestampWithStep(range.end_time, step);
            expression_range.step = step;

            visitNode(expression, expression_range);
            break;
        }

        default:
        {
            for (const auto * child : node->children)
                visitNode(child, range);
        }
    }
}


void NodeEvaluationRangeGetter::setWindows()
{
    /// Assign windows for instant selectors.
    for (auto & [node, node_range] : map)
    {
        if (node->node_type == NodeType::InstantSelector)
        {
            /// The following setting may be overwritten later if this instant selector node is a part of a range selector
            /// (see below).
            node_range.window = instant_selector_window;
        }
        else
        {
            /// If the window isn't used we set it to zero.
            node_range.window = 0;
        }
    }

    /// Assign windows for range selectors and subqueries, and propagate these ranges to other nodes.
    for (auto & [node, node_range] : map)
    {
        if (node->node_type == NodeType::RangeSelector)
        {
            const auto * range_selector_node = static_cast<const PrometheusQueryTree::RangeSelector *>(node);
            auto range = range_selector_node->range;
            node_range.window = range;
            const auto * instant_selector_node = range_selector_node->getInstantSelector();
            map.at(instant_selector_node).window = range;
            /// We propagate the range of a range selector up to its parents until we meet a range-vector function
            /// (e.g. avg_over_time) if any, so such function could user a proper window.
            propagateRangeToParents(node, range);
        }
        else if (node->node_type == NodeType::Subquery)
        {
            /// We propagate the range of a subquery up to its parents until we meet a range-vector function
            /// (e.g. avg_over_time) if any, so such function could user a proper window.
            const auto * subquery_node = static_cast<const PrometheusQueryTree::Subquery *>(node);
            auto range = subquery_node->range;
            node_range.window = range;
            propagateRangeToParents(node, range);
        }
    }

    widenWindowsForDownsampling();
}


void NodeEvaluationRangeGetter::widenWindowsForDownsampling()
{
    if (!adjust_window_for_downsampling || downsampling_periods.levels.empty() || downsampling_now_seconds == 0)
        return;

    const Int64 scale_multiplier = DecimalUtils::scaleMultiplier<Int64>(time_scale);
    const Int64 now_scaled = static_cast<Int64>(downsampling_now_seconds) * scale_multiplier;

    for (size_t pass = 0; pass < downsampling_periods.levels.size(); ++pass)
    {
        bool changed = false;
        for (auto & [node, node_range] : map)
        {
            if (node->node_type != NodeType::InstantSelector)
                continue;

            const Int64 oldest = node_range.start_time.value - node_range.window.value;
            const Int64 age_scaled = now_scaled - oldest;
            const UInt64 age_seconds = age_scaled > 0 ? static_cast<UInt64>(age_scaled / scale_multiplier) : 0;
            const auto * level = downsampling_periods.levelForAge(age_seconds);
            if (!level)
                continue;

            const Node * range_selector = nullptr;
            const Node * cursor = node->parent;
            while (cursor && (cursor->node_type == NodeType::Offset || cursor->node_type == NodeType::RangeSelector))
            {
                if (cursor->node_type == NodeType::RangeSelector)
                    range_selector = cursor;
                cursor = cursor->parent;
            }

            UInt64 samples_needed = 1;
            if (range_selector && cursor && cursor->node_type == NodeType::Function
                && needsTwoSamples(static_cast<const PrometheusQueryTree::Function *>(cursor)->function_name))
                samples_needed = 2;

            const DurationType required{
                static_cast<Int64>(level->interval_seconds) * static_cast<Int64>(samples_needed) * scale_multiplier};
            if (!(node_range.window < required))
                continue;

            node_range.window = required;
            changed = true;
            if (range_selector)
            {
                map.at(range_selector).window = required;
                propagateRangeToParents(range_selector, required);
            }
        }
        if (!changed)
            break;
    }
}


void NodeEvaluationRangeGetter::propagateRangeToParents(const PrometheusQueryTree::Node * node, Decimal64 range)
{
    chassert(node->result_type == ResultType::RANGE_VECTOR);
    const auto * parent = node->parent;
    while (parent)
    {
        map.at(parent).window = range;
        if (parent->result_type != ResultType::RANGE_VECTOR)
            break;
        parent = parent->parent;
    }
}


const NodeEvaluationRange & NodeEvaluationRangeGetter::get(const Node * node) const
{
    auto it = map.find(node);
    if (it == map.end())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Not found node {} in NodeEvaluationRangeGetter", node->node_type);
    }
    return it->second;
}

}
