#include <algorithm>
#include <atomic>
#include <unordered_set>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Columns/IColumn.h>
#include <Core/Settings.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Interpreters/Context.h>
#include <Interpreters/misc.h>
#include <Interpreters/PreparedSets.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/MaterializingCTEStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

namespace CurrentMetrics
{
    extern const Metric QueryPlanKeySetBuildThreads;
    extern const Metric QueryPlanKeySetBuildThreadsActive;
    extern const Metric QueryPlanKeySetBuildThreadsScheduled;
}

namespace DB
{
namespace Setting
{
    extern const SettingsBool use_index_for_in_with_subqueries;
}

namespace QueryPlanOptimizations
{

namespace
{

struct SetToBuild
{
    FutureSetFromSubqueryPtr set;
    ContextPtr context;
};

bool planContainsMaterializingCTE(const QueryPlan & plan)
{
    std::unordered_set<const QueryPlan *> seen_plans;
    std::vector<const QueryPlan *> pending{&plan};

    while (!pending.empty())
    {
        const QueryPlan * current = pending.back();
        pending.pop_back();
        if (!current || !seen_plans.insert(current).second)
            continue;

        std::vector<const QueryPlan::Node *> nodes;
        if (current->getRootNode())
            nodes.push_back(current->getRootNode());

        while (!nodes.empty())
        {
            const auto * node = nodes.back();
            nodes.pop_back();
            if (!node || !node->step)
                continue;

            if (typeid_cast<const DelayedMaterializingCTEsStep *>(node->step.get())
                || typeid_cast<const MaterializingCTEStep *>(node->step.get())
                || typeid_cast<const MaterializingCTEsStep *>(node->step.get()))
                return true;

            for (const auto * child : node->children)
                nodes.push_back(child);

            for (const auto * child_plan : node->step->getChildPlans(/*for_explain=*/ false))
                pending.push_back(child_plan);
        }
    }

    return false;
}

const ActionsDAG::Node * unwrapAlias(const ActionsDAG::Node * node)
{
    if (!node)
        return nullptr;
    return node->getWithoutAlias();
}

bool isPrimaryKeyInput(const ActionsDAG::Node * node, const std::unordered_set<std::string> & key_names)
{
    node = unwrapAlias(node);
    if (!node || node->type != ActionsDAG::ActionType::INPUT)
        return false;
    if (key_names.contains(node->result_name))
        return true;
    /// The analyzer qualifies the column as `__tableN.id` before the name is resolved to the key.
    const auto dot = node->result_name.rfind('.');
    return dot != std::string::npos && key_names.contains(node->result_name.substr(dot + 1));
}

bool leftArgumentUsesPrimaryKey(const ActionsDAG::Node * node, const std::unordered_set<std::string> & key_names)
{
    node = unwrapAlias(node);
    if (!node)
        return false;

    if (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base && node->function_base->getName() == "tuple")
    {
        if (node->children.empty())
            return false;
        for (const auto * child : node->children)
        {
            if (!isPrimaryKeyInput(child, key_names))
                return false;
        }
        return true;
    }

    /// `CAST(id, ...)` and similar one-argument wrappers still name the key column.
    if (node->type == ActionsDAG::ActionType::FUNCTION && node->children.size() == 1)
        return leftArgumentUsesPrimaryKey(node->children[0], key_names);

    return isPrimaryKeyInput(node, key_names);
}

void considerInNode(
    const ActionsDAG::Node * node,
    const std::unordered_set<std::string> & key_names,
    const ContextPtr & context,
    std::vector<SetToBuild> & sets,
    std::unordered_set<const FutureSetFromSubquery *> & seen)
{
    node = unwrapAlias(node);
    if (!node || node->type != ActionsDAG::ActionType::FUNCTION || !node->function_base)
        return;
    if (!functionIsInOrGlobalInOperator(node->function_base->getName()) || node->children.size() != 2)
        return;
    if (!leftArgumentUsesPrimaryKey(node->children[0], key_names))
        return;

    const auto * set_node = unwrapAlias(node->children[1]);
    if (!set_node || set_node->type != ActionsDAG::ActionType::COLUMN || !set_node->column)
        return;

    const auto * column_set = checkAndGetColumn<const ColumnSet>(&set_node->column->getDataColumn());
    if (!column_set)
        return;

    auto future_set = column_set->getData();
    auto set_from_subquery = std::dynamic_pointer_cast<FutureSetFromSubquery>(future_set);
    if (!set_from_subquery || set_from_subquery->get() || !set_from_subquery->getQueryPlan())
        return;
    if (!seen.insert(set_from_subquery.get()).second)
        return;

    const auto * source = set_from_subquery->getQueryPlan();
    if (planHasCorrelatedExpressions(*source) || planContainsMaterializingCTE(*source))
    {
        seen.erase(set_from_subquery.get());
        return;
    }

    sets.push_back(SetToBuild{.set = std::move(set_from_subquery), .context = context});
}

void collectInNodes(
    const ActionsDAG::Node * node,
    const std::unordered_set<std::string> & key_names,
    const ContextPtr & context,
    std::vector<SetToBuild> & sets,
    std::unordered_set<const FutureSetFromSubquery *> & seen,
    std::unordered_set<const ActionsDAG::Node *> & visited)
{
    node = unwrapAlias(node);
    if (!node || !visited.insert(node).second)
        return;

    considerInNode(node, key_names, context, sets, seen);
    for (const auto * child : node->children)
        collectInNodes(child, key_names, context, sets, seen, visited);
}

void collectSetsForRead(
    const Stack & stack,
    std::vector<SetToBuild> & sets,
    std::unordered_set<const FutureSetFromSubquery *> & seen)
{
    auto * read = typeid_cast<ReadFromMergeTree *>(stack.back().node->step.get());
    if (!read)
        return;
    if (read->getQueryInfo().isStream() || read->isQueryWithFinal() || read->getIndexes().has_value())
        return;

    const auto context = read->getContext();
    if (!context || !context->getSettingsRef()[Setting::use_index_for_in_with_subqueries])
        return;

    std::unordered_set<std::string> key_names;
    for (const auto & name : read->getStorageMetadata()->getPrimaryKey().column_names)
        key_names.insert(name);
    if (key_names.empty())
        return;

    forEachIndexAnalysisFilter(
        stack,
        [&](ActionsDAG dag, const String & filter_name)
        {
            std::unordered_set<const ActionsDAG::Node *> visited;
            collectInNodes(&dag.findInOutputs(filter_name), key_names, context, sets, seen, visited);
        },
        /*include_storage_filters=*/ false);
}

void buildSets(std::vector<SetToBuild> sets, size_t num_threads)
{
    std::atomic<size_t> next = 0;
    ThreadPool pool(
        CurrentMetrics::QueryPlanKeySetBuildThreads,
        CurrentMetrics::QueryPlanKeySetBuildThreadsActive,
        CurrentMetrics::QueryPlanKeySetBuildThreadsScheduled,
        num_threads);

    for (size_t job = 0; job < num_threads; ++job)
    {
        pool.scheduleOrThrowOnError(
            [&, thread_group = getCurrentThreadGroup(), component_name = Coordination::getCurrentComponent()]
            {
                ThreadGroupSwitcher switcher(thread_group, ThreadName::MERGETREE_INDEX);
                auto local_component_guard = Coordination::setCurrentComponent(component_name);

                while (!pool.isFinished())
                {
                    const size_t index = next.fetch_add(1, std::memory_order_relaxed);
                    if (index >= sets.size())
                        break;
                    sets[index].set->buildOrderedSetInplace(sets[index].context);
                }
            });
    }

    pool.wait();
}

bool nodeContainsMaterializingCTE(const QueryPlan::Node * node)
{
    std::vector<const QueryPlan::Node *> pending{node};
    while (!pending.empty())
    {
        const auto * current = pending.back();
        pending.pop_back();
        if (!current || !current->step)
            continue;

        if (typeid_cast<const DelayedMaterializingCTEsStep *>(current->step.get())
            || typeid_cast<const MaterializingCTEStep *>(current->step.get())
            || typeid_cast<const MaterializingCTEsStep *>(current->step.get()))
            return true;

        for (const auto * child : current->children)
            pending.push_back(child);
    }
    return false;
}

struct SetStep
{
    QueryPlan::Node * node = nullptr;
    size_t depth = 0;
};

}

void liftIndependentSetBuilds(QueryPlan::Node & root, QueryPlan::Nodes & nodes, size_t max_threads)
{
    if (max_threads < 2)
        return;

    std::vector<QueryPlan::Node *> set_plans;
    std::vector<SetStep> set_steps;

    std::vector<std::pair<QueryPlan::Node *, size_t>> pending{{&root, 0}};
    while (!pending.empty())
    {
        auto [node, depth] = pending.back();
        pending.pop_back();
        if (!node || !node->step)
            continue;

        if (typeid_cast<CreatingSetsStep *>(node->step.get()))
        {
            bool keep_on_branch = false;
            size_t movable = 0;
            for (size_t i = 1; i < node->children.size(); ++i)
            {
                const auto * header = node->children[i]->step ? node->children[i]->step->getOutputHeader().get() : nullptr;
                if (!header || !header->empty() || nodeContainsMaterializingCTE(node->children[i]))
                    keep_on_branch = true;
                else
                    ++movable;
            }

            if (!keep_on_branch && movable > 0)
            {
                for (size_t i = 1; i < node->children.size(); ++i)
                    set_plans.push_back(node->children[i]);
                set_steps.push_back(SetStep{node, depth});
            }

            if (!node->children.empty())
                pending.push_back({node->children[0], depth + 1});
            continue;
        }

        for (auto * child : node->children)
            pending.push_back({child, depth + 1});
    }

    if (set_plans.size() < 2 || set_steps.size() < 2)
        return;

    std::sort(set_steps.begin(), set_steps.end(), [](const SetStep & left, const SetStep & right) { return left.depth > right.depth; });

    for (const auto & set_step : set_steps)
    {
        QueryPlan::Node * node = set_step.node;
        QueryPlan::Node * main = node->children[0];
        auto main_step = std::move(main->step);
        auto main_children = std::move(main->children);
        node->step = std::move(main_step);
        node->children = std::move(main_children);
    }

    SharedHeaders headers;
    headers.reserve(1 + set_plans.size());
    headers.push_back(root.step->getOutputHeader());
    for (const auto * set_plan : set_plans)
        headers.push_back(set_plan->step->getOutputHeader());

    auto step = std::make_unique<CreatingSetsStep>(std::move(headers));
    step->setStepDescription("Create sets before main query execution");

    auto * main_child = &nodes.emplace_back(std::move(root));
    std::vector<QueryPlan::Node *> children;
    children.reserve(1 + set_plans.size());
    children.push_back(main_child);
    for (auto * set_plan : set_plans)
        children.push_back(set_plan);
    root = QueryPlan::Node{std::move(step), std::move(children)};
}

void collectKeySets(
    QueryPlan::Node & root,
    std::vector<SetToBuild> & sets,
    std::unordered_set<const FutureSetFromSubquery *> & seen)
{
    Stack stack;
    stack.push_back({.node = &root});

    while (!stack.empty())
    {
        auto & frame = stack.back();
        if (frame.next_child == 0)
            collectSetsForRead(stack, sets, seen);

        if (frame.next_child < frame.node->children.size())
        {
            auto * child = frame.node->children[frame.next_child];
            ++frame.next_child;
            stack.push_back({.node = child});
            continue;
        }

        stack.pop_back();
    }
}

void buildCollectedKeySets(std::vector<SetToBuild> sets, const QueryPlanOptimizationSettings & optimization_settings)
{
    if (sets.size() < 2 || optimization_settings.max_threads < 2)
        return;

    const size_t num_threads = std::min(sets.size(), optimization_settings.max_threads);
    buildSets(std::move(sets), num_threads);
}

void collectKeySetsFromFilters(
    QueryPlan::Node & root,
    std::vector<SetToBuild> & sets,
    std::unordered_set<const FutureSetFromSubquery *> & seen);

void collectKeySetsFromDelayedCTEs(
    QueryPlan::Node & root,
    std::vector<SetToBuild> & sets,
    std::unordered_set<const FutureSetFromSubquery *> & seen,
    std::unordered_set<const QueryPlan *> & seen_plans)
{
    std::vector<QueryPlan::Node *> pending{&root};
    while (!pending.empty())
    {
        auto * node = pending.back();
        pending.pop_back();
        if (!node || !node->step)
            continue;

        if (auto * delayed = typeid_cast<DelayedMaterializingCTEsStep *>(node->step.get()))
        {
            for (const auto & cte : delayed->getMaterializedCTEs())
            {
                if (!cte || !cte->plan || cte->is_plan_optimized.load() || !cte->plan->getRootNode())
                    continue;
                if (!seen_plans.insert(cte->plan.get()).second)
                    continue;
                collectKeySetsFromFilters(*cte->plan->getRootNode(), sets, seen);
                collectKeySets(*cte->plan->getRootNode(), sets, seen);
                collectKeySetsFromDelayedCTEs(*cte->plan->getRootNode(), sets, seen, seen_plans);
            }
        }

        for (auto * child : node->children)
            pending.push_back(child);
    }
}

void buildKeySetsConcurrently(QueryPlan::Node & root, const QueryPlanOptimizationSettings & optimization_settings)
{
    std::vector<SetToBuild> sets;
    std::unordered_set<const FutureSetFromSubquery *> seen;
    collectKeySetsFromFilters(root, sets, seen);
    collectKeySets(root, sets, seen);
    std::unordered_set<const QueryPlan *> seen_plans;
    collectKeySetsFromDelayedCTEs(root, sets, seen, seen_plans);
    buildCollectedKeySets(std::move(sets), optimization_settings);
}

/// `collectKeySets` only sees a filter that already sits next to the read.
/// A materialized CTE plan still has the `IN` above aggregation at this point.
/// Predicate pushdown moves it later, inside that plan's own `optimize`, so the
/// adjacent walk finds one set at a time. Scan every filter in the plan first.
void collectKeySetsFromFilters(
    QueryPlan::Node & root,
    std::vector<SetToBuild> & sets,
    std::unordered_set<const FutureSetFromSubquery *> & seen)
{
    std::unordered_set<std::string> key_names;
    ContextPtr context;
    std::vector<std::pair<const ActionsDAG *, String>> filters;
    std::vector<QueryPlan::Node *> pending{&root};

    while (!pending.empty())
    {
        auto * node = pending.back();
        pending.pop_back();
        if (!node || !node->step)
            continue;

        if (auto * read = typeid_cast<ReadFromMergeTree *>(node->step.get()))
        {
            if (!read->getQueryInfo().isStream() && !read->isQueryWithFinal())
            {
                const auto read_context = read->getContext();
                if (read_context && read_context->getSettingsRef()[Setting::use_index_for_in_with_subqueries])
                {
                    for (const auto & name : read->getStorageMetadata()->getPrimaryKey().column_names)
                        key_names.insert(name);
                    if (!context)
                        context = read_context;
                }
            }
        }

        if (const auto * filter = typeid_cast<const FilterStep *>(node->step.get()))
            filters.emplace_back(&filter->getExpression(), filter->getFilterColumnName());
        else if (const auto * expression = typeid_cast<const ExpressionStep *>(node->step.get()))
        {
            for (const auto & output : expression->getExpression().getOutputs())
                filters.emplace_back(&expression->getExpression(), output->result_name);
        }

        for (auto * child : node->children)
            pending.push_back(child);
    }

    if (!context || key_names.empty())
        return;

    for (const auto & [dag, name] : filters)
    {
        std::unordered_set<const ActionsDAG::Node *> visited;
        collectInNodes(&dag->findInOutputs(name), key_names, context, sets, seen, visited);
    }
}

void buildKeySetsConcurrentlyForPlans(
    const std::vector<QueryPlan *> & plans,
    const QueryPlanOptimizationSettings & optimization_settings)
{
    std::vector<SetToBuild> sets;
    std::unordered_set<const FutureSetFromSubquery *> seen;
    for (auto * plan : plans)
    {
        if (!plan || !plan->getRootNode())
            continue;
        collectKeySetsFromFilters(*plan->getRootNode(), sets, seen);
        collectKeySets(*plan->getRootNode(), sets, seen);
    }
    buildCollectedKeySets(std::move(sets), optimization_settings);
}

}
}
