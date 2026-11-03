#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeSeriesDownsample.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>

#include <limits>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

UInt64 intervalSecondsParameter(const String & name, const Array & parameters)
{
    if (parameters.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} requires one parameter, the interval in seconds, got {}", name, parameters.size());

    Int64 seconds = 0;
    if (parameters[0].getType() == Field::Types::UInt64)
        seconds = static_cast<Int64>(parameters[0].safeGet<UInt64>());
    else if (parameters[0].getType() == Field::Types::Int64)
        seconds = parameters[0].safeGet<Int64>();
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Interval parameter of aggregate function {} must be an integer number of seconds, got {}",
            name, parameters[0].getTypeName());

    if (seconds <= 0 || static_cast<UInt64>(seconds) > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Interval parameter of aggregate function {} must be in the range [1, {}], got {}",
            name, std::numeric_limits<UInt32>::max(), seconds);

    return static_cast<UInt64>(seconds);
}

Int64 scaleInterval(UInt64 seconds, const DataTypePtr & timestamp_type)
{
    UInt32 scale = 0;
    if (const auto * date_time64 = typeid_cast<const DataTypeDateTime64 *>(timestamp_type.get()))
        scale = date_time64->getScale();
    return static_cast<Int64>(seconds) * DecimalUtils::scaleMultiplier<Int64>(scale);
}

template <typename ValueType>
AggregateFunctionPtr createWithValueType(
    const String & name,
    const DataTypes & argument_types,
    const Array & parameters,
    const DataTypePtr & timestamp_type,
    Int64 interval_scaled)
{
    AggregateFunctionPtr result;
    if (isDateTime64(timestamp_type))
        result = std::make_shared<AggregateFunctionTimeSeriesDownsample<DateTime64, ValueType>>(argument_types, parameters, interval_scaled);
    else if (isDateTime(timestamp_type) || isUInt32(timestamp_type))
        result = std::make_shared<AggregateFunctionTimeSeriesDownsample<UInt32, ValueType>>(argument_types, parameters, interval_scaled);

    if (!result)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of timestamp for aggregate function {}", timestamp_type->getName(), name);

    return result;
}

AggregateFunctionPtr createWithTimestampAndValueTypes(
    const String & name,
    const DataTypes & argument_types,
    const Array & parameters,
    const DataTypePtr & timestamp_type,
    const DataTypePtr & value_type,
    UInt64 interval_seconds)
{
    const Int64 interval_scaled = scaleInterval(interval_seconds, timestamp_type);
    if (value_type->getTypeId() == TypeIndex::Float64)
        return createWithValueType<Float64>(name, argument_types, parameters, timestamp_type, interval_scaled);
    if (value_type->getTypeId() == TypeIndex::Float32)
        return createWithValueType<Float32>(name, argument_types, parameters, timestamp_type, interval_scaled);

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
        "Illegal type {} of value for aggregate function {}", value_type->getName(), name);
}

AggregateFunctionPtr createAggregateFunctionTimeSeriesDownsample(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    /// Not gated by `enable_time_series_aggregate_functions`: a TimeSeries table uses it in a TTL expression.
    const UInt64 interval_seconds = intervalSecondsParameter(name, parameters);

    if (argument_types.size() == 1)
    {
        const auto * array_type = typeid_cast<const DataTypeArray *>(argument_types[0].get());
        const auto * tuple_type = array_type ? typeid_cast<const DataTypeTuple *>(array_type->getNestedType().get()) : nullptr;
        if (!tuple_type || tuple_type->getElements().size() != 2)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument for aggregate function {}, expected Array(Tuple(timestamp, value))",
                argument_types[0]->getName(), name);

        return createWithTimestampAndValueTypes(
            name, argument_types, parameters, tuple_type->getElements()[0], tuple_type->getElements()[1], interval_seconds);
    }

    assertBinary(name, argument_types);
    if (argument_types[0]->getTypeId() == TypeIndex::Array || argument_types[1]->getTypeId() == TypeIndex::Array)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Aggregate function {} accepts one array of pairs or two scalar arguments, got {} and {}",
            name, argument_types[0]->getName(), argument_types[1]->getName());

    return createWithTimestampAndValueTypes(
        name, argument_types, parameters, argument_types[0], argument_types[1], interval_seconds);
}

}

void registerAggregateFunctionTimeSeriesDownsample(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeSeriesDownsample(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Keeps the last sample of each time interval.

The interval is the function parameter, in seconds. An interval is right-closed: a sample with timestamp `T` belongs to the interval that ends at `T + interval - 1`, rounded down to a multiple of the interval. A sample that falls exactly on a boundary belongs to the interval that ends at that boundary.

The function keeps the original timestamp of the chosen sample. When several samples have the same timestamp, the greatest value wins. A NaN value loses to any other value.

Samples can be passed as two arguments, `timestamp` and `value`, or as one array of `(timestamp, value)` tuples. The array form has the same type as the result. It can also read a `SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp, value)))` column and return that same type.

Applying the function again with the same interval does not change the result. Applying it with a multiple of that interval gives the same result as applying only the coarser interval.

<Note>
This function is in private preview.
</Note>
    )";
    FunctionDocumentation::Syntax syntax = R"(
timeSeriesDownsample(interval_seconds)(timestamp, value)
timeSeriesDownsample(interval_seconds)(samples)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"timestamp", "Timestamp of one sample.", {"UInt32", "DateTime", "DateTime64"}},
        {"value", "Value of one sample.", {"Float32", "Float64"}},
        {"samples", "Samples of one series, as an array of `(timestamp, value)` tuples.", {"Array(Tuple(T1, T2))"}},
    };
    FunctionDocumentation::Parameters parameters = {
        {"interval_seconds", "Interval length in seconds. The length is scaled to the scale of the timestamp type.", {"UInt64"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the kept samples as an array of `(timestamp, value)` tuples, sorted by timestamp.",
        {"Array(Tuple(T1, T2))"}};
    FunctionDocumentation::Examples examples = {
    {
        "Right-closed intervals",
        R"(
SELECT timeSeriesDownsample(60)([(toDateTime(0), 1.), (toDateTime(30), 2.), (toDateTime(60), 3.)])
        )",
        R"(
┌─timeSeriesDownsample(60)([(toDateTime(0), 1.), (toDateTime(30), 2.), (toDateTime(60), 3.)])─┐
│ [(0,1),(60,3)]                                                                              │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction("timeSeriesDownsample", {createAggregateFunctionTimeSeriesDownsample, documentation});
}

}
