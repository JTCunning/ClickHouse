#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Storages/TimeSeries/TimeSeriesMetricLocalityId.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
class FunctionTimeSeriesMetricLocalityId final : public IFunction
{
public:
    static constexpr auto name = "timeSeriesMetricLocalityId";

    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<FunctionTimeSeriesMetricLocalityId>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isDeterministic() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} expects one argument", name);
        const auto * nested = removeNullable(removeLowCardinality(arguments[0])).get();
        if (!isString(nested))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument of function {} must be String-like, got {}", name, arguments[0]->getName());
        return std::make_shared<DataTypeUInt32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        ColumnPtr col = arguments[0].column->convertToFullColumnIfLowCardinality();
        col = col->convertToFullColumnIfConst();
        const auto * str = checkAndGetColumn<ColumnString>(col.get());
        if (!str)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected column type for function {}", name);

        auto res = ColumnUInt32::create();
        auto & out = res->getData();
        out.resize(input_rows_count);
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto ref = str->getDataAt(i);
            out[i] = computeTimeSeriesMetricLocalityId(ref);
        }
        return res;
    }
};
}

REGISTER_FUNCTION(TimeSeriesMetricLocalityId)
{
    FunctionDocumentation::Description description = R"(
Returns a UInt32 for MergeTree prefix locality in the TimeSeries data table, derived only from `metric_name`.
Up to six initials are taken: split on ASCII `'_'` into segments, then split each segment on camelCase boundaries
(ASCII uppercase immediately after lowercase or digit). Each initial must be ASCII `a–z` / `A–Z` (folded to lowercase);
digits and other symbols are skipped for initials.
Letters are packed in base 27 (digit 0 = unused tail slot, digits 1..26 = `a`..`z`), mixed big-endian into 32 bits.
Row identity stays in the `id` column.
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesMetricLocalityId(metric_name)";
    FunctionDocumentation::Arguments arguments = {
        {"metric_name", "Metric name (`__name__`).", {"String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"A UInt32 locality code.", {"UInt32"}};
    FunctionDocumentation::Examples examples = {
        {"Example",
         "SELECT "
         "hex(timeSeriesMetricLocalityId('kube_node_cpu_seconds_total')) AS kube, "
         "hex(timeSeriesMetricLocalityId('ClickHouseAsyncMetrics_BlockActiveTime_sdan')) AS ch_async, "
         "hex(timeSeriesMetricLocalityId('ClickHouseProfileEvents_AddressesDiscovered')) AS ch_profile",
         R"(
┌─kube───────┬─ch_async────┬─ch_profile────┐
│ 09DB10A7   │ 02D228E8    │ 02D69355      │
└────────────┴─────────────┴───────────────┘
        )"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionTimeSeriesMetricLocalityId>(documentation);
}

}
