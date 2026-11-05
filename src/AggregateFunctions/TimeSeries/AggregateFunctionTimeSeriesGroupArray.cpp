#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeSeriesGroupArray.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypesDecimal.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


namespace
{
    template <typename TimestampType, typename ValueType>
    AggregateFunctionPtr createWithTypes(
        const DataTypes & argument_types, const Array & parameters, TimestampType rollup_step)
    {
        return std::make_shared<AggregateFunctionTimeSeriesGroupArray<TimestampType, ValueType>>(argument_types, parameters, rollup_step);
    }

    template <typename ValueType>
    AggregateFunctionPtr createWithValueType(
        const String & name,
        const DataTypes & argument_types,
        const Array & parameters,
        const DataTypePtr & timestamp_type,
        UInt64 rollup_step_seconds)
    {
        AggregateFunctionPtr res;
        if (isDateTime64(timestamp_type))
        {
            const UInt32 scale = getDecimalScale(*timestamp_type);
            const DateTime64 rollup_step = rollup_step_seconds
                ? DecimalUtils::decimalFromComponents<DateTime64>(static_cast<Int64>(rollup_step_seconds), 0, scale)
                : DateTime64{0};
            res = createWithTypes<DateTime64, ValueType>(argument_types, parameters, rollup_step);
        }
        else if (isDateTime(timestamp_type) || isUInt32(timestamp_type))
        {
            res = createWithTypes<UInt32, ValueType>(argument_types, parameters, static_cast<UInt32>(rollup_step_seconds));
        }

        if (!res)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of 1st argument (timestamp) for aggregate function {}",
                            timestamp_type->getName(), name);

        return res;
    }

    AggregateFunctionPtr createWithTimestampAndValueTypes(
        const String & name,
        const DataTypes & argument_types,
        const Array & parameters,
        const DataTypePtr & timestamp_type,
        const DataTypePtr & value_type,
        UInt64 rollup_step_seconds)
    {
        if (value_type->getTypeId() == TypeIndex::Float64)
            return createWithValueType<Float64>(name, argument_types, parameters, timestamp_type, rollup_step_seconds);
        if (value_type->getTypeId() == TypeIndex::Float32)
            return createWithValueType<Float32>(name, argument_types, parameters, timestamp_type, rollup_step_seconds);

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of 2nd argument (value) for aggregate function {}", value_type->getName(), name);
    }

    AggregateFunctionPtr createAggregateFunctionTimeseriesGroupArray(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        /// The function is not gated by the `enable_time_series_aggregate_functions` setting: the TimeSeries table engine
        /// uses it in the `SimpleAggregateFunction` type of its inner tables, so the type must be parsed in any context.

        UInt64 rollup_step_seconds = 0;
        if (parameters.size() > 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Aggregate function {} requires zero or one parameter (rollup step in seconds), got {}", name, parameters.size());
        if (parameters.size() == 1)
        {
            const auto type = parameters[0].getType();
            if (type != Field::Types::UInt64 && type != Field::Types::Int64)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Aggregate function {} parameter must be an integer number of seconds", name);
            if (type == Field::Types::Int64)
            {
                const Int64 value = parameters[0].safeGet<Int64>();
                if (value <= 0)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Aggregate function {} parameter must be a positive number of seconds", name);
                rollup_step_seconds = static_cast<UInt64>(value);
            }
            else
            {
                rollup_step_seconds = parameters[0].safeGet<UInt64>();
                if (rollup_step_seconds == 0)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Aggregate function {} parameter must be a positive number of seconds", name);
            }
        }

        if (argument_types.size() == 1)
        {
            /// The single argument form: Array(Tuple(timestamp, value)), which is the same as the result type.
            const auto * array_type = typeid_cast<const DataTypeArray *>(argument_types[0].get());
            const auto * tuple_type = array_type ? typeid_cast<const DataTypeTuple *>(array_type->getNestedType().get()) : nullptr;
            if (!tuple_type || tuple_type->getElements().size() != 2)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument for aggregate function {}, expected Array(Tuple(timestamp, value))",
                    argument_types[0]->getName(), name);

            return createWithTimestampAndValueTypes(
                name, argument_types, parameters, tuple_type->getElements()[0], tuple_type->getElements()[1], rollup_step_seconds);
        }

        assertBinary(name, argument_types);

        if ((argument_types[0]->getTypeId() == TypeIndex::Array) != (argument_types[1]->getTypeId() == TypeIndex::Array))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal combination of argument type {} and {} for aggregate function {}, expected both arguments to be arrays or not arrays",
                argument_types[0]->getName(), argument_types[1]->getName(), name);

        if (argument_types[1]->getTypeId() == TypeIndex::Array)
        {
            const auto & timestamp_type = typeid_cast<const DataTypeArray *>(argument_types[0].get())->getNestedType();
            const auto & value_type = typeid_cast<const DataTypeArray *>(argument_types[1].get())->getNestedType();
            return createWithTimestampAndValueTypes(name, argument_types, parameters, timestamp_type, value_type, rollup_step_seconds);
        }

        return createWithTimestampAndValueTypes(name, argument_types, parameters, argument_types[0], argument_types[1], rollup_step_seconds);
    }
}

void registerAggregateFunctionTimeseriesGroupArray(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesGroupArray(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Sorts time series data by timestamp in ascending order.

The samples can be passed in one of three forms:
- as two arguments `timestamp` and `value`, where each row holds a single sample;
- as two arrays of timestamps and values, where each row holds a whole time series;
- as a single array of `(timestamp, value)` tuples, where each row holds a whole time series. This form has the same type as the result, which allows using the function in the `SimpleAggregateFunction` data type.

If several samples have the same timestamp, only one of them is used: the sample with the greatest value. A NaN value loses to any other value, so a NaN value is used only if all samples at this timestamp are NaN.

When called with a positive `rollup_step_seconds` parameter, the function keeps only the last sample of each
rollup interval (`floor(timestamp / rollup_step)`), preserving that sample's original timestamp. Equal
timestamps within an interval still keep the greatest value.

<Note>
This function is in private preview.
</Note>
    )";
    FunctionDocumentation::Syntax syntax = R"(
timeSeriesGroupArray(timestamp, value)
timeSeriesGroupArray(samples)
timeSeriesGroupArray(rollup_step_seconds)(timestamp, value)
timeSeriesGroupArray(rollup_step_seconds)(samples)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"timestamp", "Timestamp of the sample. Can be an individual value or an array.", {"UInt32", "DateTime", "DateTime64", "Array(UInt32)", "Array(DateTime)", "Array(DateTime64)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be an individual value or an array.", {"Float*", "Array(Float*)"}},
        {"samples", "Samples of the time series passed as an array of tuples `(timestamp, value)`, where the tuple elements have the timestamp and value types listed above.", {"Array(Tuple(T1, T2))"}},
    };
    FunctionDocumentation::Parameters parameters = {
        {"rollup_step_seconds", "Optional. Positive integer number of seconds. When set, collapses samples that fall into the same rollup interval to the last sample of that interval."},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array of tuples `(timestamp, value)` sorted by timestamp in ascending order.", {"Array(Tuple(T1, T2))"}};
    FunctionDocumentation::Examples examples = {
    {
        "Basic usage with individual values",
        R"(
WITH
    [110, 120, 130, 140, 140, 100]::Array(UInt32) AS timestamps,
    [1, 6, 8, 17, 19, 5]::Array(Float32) AS values
SELECT timeSeriesGroupArray(timestamp, value)
FROM
(
    SELECT
        arrayJoin(arrayZip(timestamps, values)) AS ts_and_val,
        ts_and_val.1 AS timestamp,
        ts_and_val.2 AS value
);
        )",
        R"(
┌─timeSeriesGroupArray(timestamp, value)─────┐
│ [(100,5),(110,1),(120,6),(130,8),(140,19)] │
└────────────────────────────────────────────┘
        )"
    },
    {
        "Passing multiple samples of timestamps and values as arrays of equal size",
        R"(
WITH
    [110, 120, 130, 140, 140, 100]::Array(UInt32) AS timestamps,
    [1, 6, 8, 17, 19, 5]::Array(Float32) AS values
SELECT timeSeriesGroupArray(timestamps, values);
        )",
        R"(
┌─timeSeriesGroupArray(timestamps, values)───┐
│ [(100,5),(110,1),(120,6),(130,8),(140,19)] │
└────────────────────────────────────────────┘
        )"
    },
    {
        "Keeping the last sample of each 60-second rollup interval",
        R"(
WITH
    [10, 50, 70, 110]::Array(UInt32) AS timestamps,
    [1, 2, 3, 4]::Array(Float32) AS values
SELECT timeSeriesGroupArray(60)(timestamps, values);
        )",
        R"(
┌─timeSeriesGroupArray(60)(timestamps, values)─┐
│ [(50,2),(110,4)]                             │
└──────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction("timeSeriesGroupArray", {createAggregateFunctionTimeseriesGroupArray, documentation});
}

}
