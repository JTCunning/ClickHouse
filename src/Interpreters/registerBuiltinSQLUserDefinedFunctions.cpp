#include <Interpreters/registerBuiltinSQLUserDefinedFunctions.h>

#include <Core/Settings.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateSQLFunctionQuery.h>
#include <Parsers/ParserCreateFunctionQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/Exception.h>


namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_parser_backtracks;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{
constexpr const char * locality_function_name = "timeSeriesMetricLocalityId";

const String & canonicalTimeSeriesMetricLocalityIdCreateQuery()
{
    static const String query
        = "CREATE OR REPLACE FUNCTION timeSeriesMetricLocalityId AS x -> toUInt32(sipHash64(x))";
    return query;
}

ASTPtr parseCanonicalTimeSeriesMetricLocalityIdCreateQuery(const ContextPtr & context)
{
    const auto & query = canonicalTimeSeriesMetricLocalityIdCreateQuery();
    const auto & settings = context->getSettingsRef();
    ParserCreateFunctionQuery parser;
    return parseQuery(
        parser,
        query.data(),
        query.data() + query.size(),
        "registerBuiltinSQLUserDefinedFunctions",
        0,
        settings[Setting::max_parser_depth],
        settings[Setting::max_parser_backtracks]);
}

/// Register the canonical UDF if missing; if present, require normalized \c function_core AST to match exactly
/// (same as built-in `x -> toUInt32(sipHash64(x))`) or throw \c BAD_ARGUMENTS.
void registerOrValidateTimeSeriesMetricLocalityId(ContextMutablePtr context)
{
    ASTPtr expected_ast = parseCanonicalTimeSeriesMetricLocalityIdCreateQuery(context);
    if (!expected_ast->as<ASTCreateSQLFunctionQuery>())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected AST for builtin UDF: {}", expected_ast->formatForLogging());

    if (!UserDefinedSQLFunctionFactory::instance().has(locality_function_name))
    {
        UserDefinedSQLFunctionFactory::instance().registerFunction(
            context, locality_function_name, expected_ast, /* throw_if_exists */ false, /* replace_if_exists */ true);
        return;
    }

    ASTPtr existing_ast = UserDefinedSQLFunctionFactory::instance().tryGet(locality_function_name);
    if (!existing_ast)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} is registered but could not be loaded", locality_function_name);

    const auto * existing_create = existing_ast->as<ASTCreateSQLFunctionQuery>();
    if (!existing_create)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Existing function {} must be a SQL UDF with definition `x -> toUInt32(sipHash64(x))` "
            "(TimeSeries metric locality compatibility). Found a non-SQL function with the same name.",
            locality_function_name);

    ASTPtr normalized_expected = normalizeCreateFunctionQuery(*expected_ast, context);
    ASTPtr normalized_existing = normalizeCreateFunctionQuery(*existing_ast, context);

    const auto * expected_q = normalized_expected->as<ASTCreateSQLFunctionQuery>();
    const auto * existing_q = normalized_existing->as<ASTCreateSQLFunctionQuery>();
    if (!expected_q || !existing_q || !expected_q->function_core || !existing_q->function_core)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected AST for SQL function {}", locality_function_name);

    if (expected_q->function_core->getTreeHash(/* ignore_aliases= */ true) != existing_q->function_core->getTreeHash(/* ignore_aliases= */ true))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The SQL user-defined function {} must be exactly `x -> toUInt32(sipHash64(x))` "
            "(TimeSeries metric locality). Drop it or replace it with that definition.",
            locality_function_name);
}
}

void ensureTimeSeriesMetricLocalityIdUserDefinedFunction(ContextMutablePtr context)
{
    registerOrValidateTimeSeriesMetricLocalityId(std::move(context));
}

void registerBuiltinSQLUserDefinedFunctions(ContextMutablePtr context)
{
    registerOrValidateTimeSeriesMetricLocalityId(std::move(context));
}

}
