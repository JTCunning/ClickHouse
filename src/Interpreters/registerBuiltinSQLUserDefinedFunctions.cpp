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
    extern const int LOGICAL_ERROR;
}

void registerBuiltinSQLUserDefinedFunctions(ContextMutablePtr context)
{
    static constexpr const char * locality_function_name = "timeSeriesMetricLocalityId";

    if (UserDefinedSQLFunctionFactory::instance().has(locality_function_name))
        return;

    /// Same semantics as `timeSeriesMetricLocalityIdFromMetricName` in `TimeSeriesMetricLocality.h`
    /// and as the SQL expression `toUInt32(sipHash64(metric_name))`.
    const String query = "CREATE OR REPLACE FUNCTION timeSeriesMetricLocalityId AS x -> toUInt32(sipHash64(x))";

    const auto & settings = context->getSettingsRef();
    ParserCreateFunctionQuery parser;
    ASTPtr ast = parseQuery(
        parser,
        query.data(),
        query.data() + query.size(),
        "registerBuiltinSQLUserDefinedFunctions",
        0,
        settings[Setting::max_parser_depth],
        settings[Setting::max_parser_backtracks]);

    if (!ast->as<ASTCreateSQLFunctionQuery>())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected AST for builtin UDF: {}", ast->formatForLogging());

    UserDefinedSQLFunctionFactory::instance().registerFunction(
        context, locality_function_name, ast, /* throw_if_exists */ false, /* replace_if_exists */ true);
}

}
