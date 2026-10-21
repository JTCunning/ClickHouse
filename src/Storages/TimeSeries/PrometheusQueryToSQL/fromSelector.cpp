#include <Storages/TimeSeries/PrometheusQueryToSQL/fromSelector.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFusedAggregationBinaryOperator.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>
#include <base/defines.h>

#include <fmt/format.h>


namespace DB::PrometheusQueryToSQL
{

namespace
{
    constexpr UInt64 STALE_NAN_BITS = 0x7ff0000000000002ULL;

    /// Two instant selectors share a grid only if they have the same text and the same evaluation range.
    /// The range already includes the effect of `offset` and `@` modifiers.
    String makeInstantSelectorKey(std::string_view instant_selector_text, const Node * node, const ConverterContext & context)
    {
        const auto & node_range = context.node_range_getter.get(node);
        return fmt::format(
            "{}\n{}\n{}\n{}\n{}",
            instant_selector_text,
            node_range.start_time.value,
            node_range.end_time.value,
            node_range.step.value,
            node_range.window.value);
    }

    void countInstantSelectorUses(const Node * node, ConverterContext & context)
    {
        /// The instant selector inside a range selector is converted by fromRangeSelector(), not by fromSelector().
        if (node->node_type == NodeType::RangeSelector)
            return;

        if (node->node_type == NodeType::InstantSelector)
        {
            auto instant_selector_text = node->toString(*context.promql_tree);
            ++context.instant_selector_use_count[makeInstantSelectorKey(instant_selector_text, node, context)];
            return;
        }

        /// A fused aggregation binary operator converts only the argument of its left aggregation.
        if (node->node_type == NodeType::BinaryOperator)
        {
            const auto * binary_operator = static_cast<const PrometheusQueryTree::BinaryOperator *>(node);
            if (canFuseAggregationBinaryOperator(binary_operator, context))
            {
                const auto * aggregation
                    = static_cast<const PrometheusQueryTree::AggregationOperator *>(binary_operator->getLeftArgument());
                countInstantSelectorUses(aggregation->getArguments().at(0), context);
                return;
            }
        }

        for (const auto * child : node->children)
            countInstantSelectorUses(child, context);
    }

    /// SELECT group, values FROM <shared grid>
    SQLQueryPiece readSharedGrid(const SharedInstantSelectorGrid & grid, const Node * node)
    {
        SQLQueryPiece res{node, ResultType::INSTANT_VECTOR, StoreMethod::VECTOR_GRID};
        res.metric_name_dropped = grid.metric_name_dropped;
        res.start_time = grid.start_time;
        res.end_time = grid.end_time;
        res.step = grid.step;

        SelectQueryBuilder builder;
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
        builder.from_table = grid.subquery_name;
        res.select_query = builder.getSelectQuery();
        return res;
    }

    ASTPtr isStaleMarker(ASTPtr value)
    {
        return makeASTFunction(
            "equals",
            makeASTFunction("reinterpretAsUInt64", std::move(value)),
            make_intrusive<ASTLiteral>(STALE_NAN_BITS));
    }

    SQLQueryPiece fromRangeSelector(std::string_view instant_selector_text,
                                    const Node * node,
                                    bool filter_stale_markers,
                                    ConverterContext & context)
    {
        auto node_range = context.node_range_getter.get(node);
        if (node_range.empty())
            return SQLQueryPiece{node, ResultType::RANGE_VECTOR, StoreMethod::EMPTY};

        SQLQueryPiece res{node, ResultType::RANGE_VECTOR, StoreMethod::RAW_DATA};

        /// SELECT timeSeriesIdToGroup(id) AS group, timestamp, value
        /// FROM timeSeriesSelector(<database>, <time_series_table>, <selector>, <min_time>, <max_time>)
        SelectQueryBuilder builder;

        builder.select_list.push_back(makeASTFunction("timeSeriesIdToGroup", make_intrusive<ASTIdentifier>(ColumnNames::ID)));
        builder.select_list.back()->setAlias(ColumnNames::Group);

        /// The columns `timestamp` and `value` keep the types they have in the table, see the comment for StoreMethod::RAW_DATA.
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp));
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Value));

        /// The range is (start_time - window, end_time] at the result scale. The table function converts the bounds to the scale
        /// of the table itself, rounding them towards the inside of the range.
        TimestampType min_time = node_range.start_time - node_range.window + 1;
        TimestampType max_time = node_range.end_time;

        builder.from_table_function = makeASTFunction(
            "timeSeriesSelector",
            make_intrusive<ASTLiteral>(context.time_series_storage_id.getDatabaseName()),
            make_intrusive<ASTLiteral>(context.time_series_storage_id.getTableName()),
            make_intrusive<ASTLiteral>(String{instant_selector_text}),
            timeSeriesTimestampToAST(min_time, context.result_timestamp_type),
            timeSeriesTimestampToAST(max_time, context.result_timestamp_type));

        /// Prometheus range selectors omit the dedicated stale-NaN payload while preserving
        /// ordinary NaN samples as data.
        if (filter_stale_markers)
        {
            builder.where = makeASTFunction(
                "not",
                isStaleMarker(make_intrusive<ASTIdentifier>(ColumnNames::Value)));
        }

        res.select_query = builder.getSelectQuery();
        return res;
    }

    SQLQueryPiece replaceStaleMarkersWithNulls(SQLQueryPiece && vector_grid, ConverterContext & context)
    {
        if (vector_grid.store_method != StoreMethod::VECTOR_GRID)
            return std::move(vector_grid);

        /// Instant selectors must let last_over_time observe the stale marker first.
        /// Once it is selected as the newest sample, turn it into absence before
        /// aggregations, vector matching, scalar(), absent(), or finalization see it.
        SelectQueryBuilder builder;
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

        const String iterator_name = "x";
        ASTPtr is_stale_marker = makeASTFunction(
            "and",
            makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>(iterator_name)),
            isStaleMarker(
                makeASTFunction(
                    "assumeNotNull",
                    make_intrusive<ASTIdentifier>(iterator_name))));

        ASTPtr value = makeASTFunction(
            "if",
            std::move(is_stale_marker),
            make_intrusive<ASTLiteral>(Field{}),
            make_intrusive<ASTIdentifier>(iterator_name));

        auto values = makeASTFunction(
            "arrayMap",
            makeASTFunction(
                "lambda",
                makeASTFunction("tuple", make_intrusive<ASTIdentifier>(iterator_name)),
                std::move(value)),
            make_intrusive<ASTIdentifier>(ColumnNames::Values));
        values->setAlias(ColumnNames::Values);
        builder.select_list.push_back(std::move(values));

        context.subqueries.emplace_back(
            context.subqueries.size(),
            std::move(vector_grid.select_query),
            SQLSubqueryType::TABLE);
        builder.from_table = context.subqueries.back().name;

        /// A vector-grid row whose every step is NULL represents no series at all.
        /// Drop it here so downstream operators which validate uniqueness by row count
        /// don't mistake a stale series for a duplicate.
        context.subqueries.emplace_back(
            context.subqueries.size(),
            builder.getSelectQuery(),
            SQLSubqueryType::TABLE);

        SelectQueryBuilder filter_builder;
        filter_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
        filter_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
        filter_builder.from_table = context.subqueries.back().name;

        const String filter_iterator_name = "x";
        filter_builder.where = makeASTFunction(
            "arrayExists",
            makeASTFunction(
                "lambda",
                makeASTFunction("tuple", make_intrusive<ASTIdentifier>(filter_iterator_name)),
                makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>(filter_iterator_name))),
            make_intrusive<ASTIdentifier>(ColumnNames::Values));

        vector_grid.select_query = filter_builder.getSelectQuery();
        return std::move(vector_grid);
    }
}


void countInstantSelectorUses(ConverterContext & context)
{
    countInstantSelectorUses(context.promql_tree->getRoot(), context);
}


SQLQueryPiece fromSelector(const PrometheusQueryTree::InstantSelector * instant_selector_node, ConverterContext & context)
{
    auto instant_selector_text = instant_selector_node->toString(*context.promql_tree);

    auto make_vector_grid = [&]
    {
        auto range_selector = fromRangeSelector(
            instant_selector_text, instant_selector_node, /* filter_stale_markers = */ false, context);
        auto vector_grid = applyFunctionOverRange(
            instant_selector_node, "last_over_time", {std::move(range_selector)}, context);
        return replaceStaleMarkersWithNulls(std::move(vector_grid), context);
    };

    auto key = makeInstantSelectorKey(instant_selector_text, instant_selector_node, context);
    auto use_count_it = context.instant_selector_use_count.find(key);
    if (use_count_it == context.instant_selector_use_count.end() || use_count_it->second <= 1)
        return make_vector_grid();

    auto shared_it = context.shared_instant_selector_grids.find(key);
    if (shared_it != context.shared_instant_selector_grids.end())
        return readSharedGrid(shared_it->second, instant_selector_node);

    auto vector_grid = make_vector_grid();
    if (vector_grid.store_method != StoreMethod::VECTOR_GRID)
        return vector_grid;

    context.subqueries.emplace_back(
        context.subqueries.size(),
        std::move(vector_grid.select_query),
        SQLSubqueryType::MATERIALIZED_TABLE);

    SharedInstantSelectorGrid grid;
    grid.subquery_name = context.subqueries.back().name;
    grid.metric_name_dropped = vector_grid.metric_name_dropped;
    grid.start_time = vector_grid.start_time;
    grid.end_time = vector_grid.end_time;
    grid.step = vector_grid.step;

    auto [it, inserted] = context.shared_instant_selector_grids.emplace(key, std::move(grid));
    chassert(inserted);
    return readSharedGrid(it->second, instant_selector_node);
}


SQLQueryPiece fromSelectorSampleTimestamps(const PrometheusQueryTree::InstantSelector * instant_selector_node, ConverterContext & context)
{
    auto instant_selector_text = instant_selector_node->toString(*context.promql_tree);
    auto range_selector = fromRangeSelector(
        instant_selector_text, instant_selector_node, /* filter_stale_markers = */ false, context);

    if (range_selector.store_method == StoreMethod::RAW_DATA)
    {
        /// SELECT group, timestamp,
        ///        if(<value is a stale marker>, CAST(value, 'Float64'), CAST(timestamp, 'Float64')) AS value
        /// FROM <raw_data>
        /// Stale markers are kept so that last_over_time() can still select them and they are turned into absence below,
        /// exactly as for a plain instant selector.
        SelectQueryBuilder builder;
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp));

        ASTPtr value = makeASTFunction(
            "if",
            isStaleMarker(make_intrusive<ASTIdentifier>(ColumnNames::Value)),
            makeASTFunction("CAST", make_intrusive<ASTIdentifier>(ColumnNames::Value), make_intrusive<ASTLiteral>("Float64")),
            makeASTFunction("CAST", make_intrusive<ASTIdentifier>(ColumnNames::Timestamp), make_intrusive<ASTLiteral>("Float64")));
        value->setAlias(ColumnNames::Value);
        builder.select_list.push_back(std::move(value));

        context.subqueries.emplace_back(
            context.subqueries.size(),
            std::move(range_selector.select_query),
            SQLSubqueryType::TABLE);
        builder.from_table = context.subqueries.back().name;

        range_selector.select_query = builder.getSelectQuery();
    }

    auto vector_grid = applyFunctionOverRange(
        instant_selector_node, "last_over_time", {std::move(range_selector)}, context);
    return replaceStaleMarkersWithNulls(std::move(vector_grid), context);
}


SQLQueryPiece fromSelector(const PrometheusQueryTree::RangeSelector * range_selector_node, ConverterContext & context)
{
    auto instant_selector_text = range_selector_node->getInstantSelector()->toString(*context.promql_tree);
    return fromRangeSelector(
        instant_selector_text, range_selector_node, /* filter_stale_markers = */ true, context);
}

}
