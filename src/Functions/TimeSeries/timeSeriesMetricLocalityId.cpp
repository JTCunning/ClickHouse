#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Storages/TimeSeries/TimeSeriesMetricLocality.h>

#include <memory>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Stable UInt32 locality key from metric name string. Matches `sipHash64` folding used for MergeTree `metric_locality_id`.
class FunctionTimeSeriesMetricLocalityId : public IFunction
{
public:
    static constexpr auto name = "timeSeriesMetricLocalityId";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesMetricLocalityId>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isDeterministic() const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool useDefaultImplementationForNulls() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} must be called with one argument (metric name string)",
                name);

        const auto arg_type = removeNullable(recursiveRemoveLowCardinality(arguments[0].type));
        WhichDataType which(arg_type);
        if (!which.isString() && !which.isFixedString())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument of function {} must be String or FixedString, got {}",
                name,
                arguments[0].type->getName());

        return std::make_shared<DataTypeUInt32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        const auto & col = arguments[0];
        const ColumnPtr & column = col.column;

        auto res = ColumnUInt32::create();
        ColumnUInt32::Container & out = res->getData();
        out.resize(input_rows_count);

        if (const auto * col_str = checkAndGetColumn<ColumnString>(column.get()))
        {
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                auto ref = col_str->getDataAt(i);
                out[i] = timeSeriesMetricLocalityIdFromMetricName({reinterpret_cast<const char *>(ref.data), ref.size});
            }
        }
        else if (const auto * col_fixed = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                auto ref = col_fixed->getDataAt(i);
                out[i] = timeSeriesMetricLocalityIdFromMetricName({reinterpret_cast<const char *>(ref.data), ref.size});
            }
        }
        else
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal column {} of argument of function {}",
                column->getName(),
                name);

        return res;
    }
};


REGISTER_FUNCTION(TimeSeriesMetricLocalityId)
{
    FunctionDocumentation::Description description = R"(
Returns a UInt32 locality key for a metric name string.
The result matches the `metric_locality_id` column layout used by the `TimeSeries` table engine
(`toUInt32` bit pattern of `sipHash64(metric_name)`).
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesMetricLocalityId(metric_name)";
    FunctionDocumentation::Arguments arguments = {
        {"metric_name", "Metric name.", {"String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"UInt32 locality key.", {"UInt32"}};
    FunctionDocumentation::Examples examples = {};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {
        description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesMetricLocalityId>(documentation);
}

}
