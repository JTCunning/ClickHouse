#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#include <algorithm>
#include <bit>
#include <cstring>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
    constexpr UInt64 STALE_NAN_BITS = 0x7ff0000000000002ULL;

    size_t findFirstStaleMarker(const Float64 * __restrict values, size_t count)
    {
        constexpr size_t block_size = 16;
        size_t i = 0;
        while (i + block_size <= count)
        {
            UInt8 has_stale = 0;
            for (size_t j = 0; j < block_size; ++j)
                has_stale |= static_cast<UInt8>(std::bit_cast<UInt64>(values[i + j]) == STALE_NAN_BITS);
            if (has_stale)
                break;
            i += block_size;
        }
        while (i < count)
        {
            if (std::bit_cast<UInt64>(values[i]) == STALE_NAN_BITS)
                return i;
            ++i;
        }
        return count;
    }
}

/// Internal function __timeSeriesRemoveStaleMarkers(samples) removes elements whose value has the Prometheus
/// stale-NaN bit pattern (0x7ff0000000000002) from an Array(Tuple(timestamp, value)) column without copying
/// when no stale marker is present in the block.
class FunctionTimeSeriesRemoveStaleMarkers final : public IFunction
{
public:
    static constexpr auto name = "__timeSeriesRemoveStaleMarkers";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesRemoveStaleMarkers>(); }

    [[nodiscard]] String getName() const override { return name; }
    [[nodiscard]] size_t getNumberOfArguments() const override { return 1; }
    [[nodiscard]] bool isDeterministic() const override { return true; }
    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto & samples_type = arguments[0].type;
        const auto * array_type = typeid_cast<const DataTypeArray *>(samples_type.get());
        const auto * tuple_type = array_type ? typeid_cast<const DataTypeTuple *>(array_type->getNestedType().get()) : nullptr;
        if (!tuple_type || (tuple_type->getElements().size() != 2))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument #1 (samples) of function {} has wrong type {}, it must be Array(Tuple(timestamp, value))",
                name, samples_type->getName());

        const WhichDataType which_value{tuple_type->getElements()[1]};
        if (!which_value.isFloat64() && !which_value.isFloat32())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument #1 (samples) of function {} has wrong type {}, the type of the values must be Float64 or Float32",
                name, samples_type->getName());

        if (tuple_type->hasExplicitNames())
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(tuple_type->getElements(), tuple_type->getElementNames()));
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(tuple_type->getElements()));
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        ColumnPtr samples_column_holder = arguments[0].column->convertToFullColumnIfConst();
        const auto * samples_column = checkAndGetColumn<ColumnArray>(samples_column_holder.get());
        if (!samples_column)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument #1 (samples) of function {} must be an array, got {}",
                name, arguments[0].column->getName());

        const auto * tuples_column = checkAndGetColumn<ColumnTuple>(&samples_column->getData());
        if (!tuples_column || (tuples_column->tupleSize() != 2))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument #1 (samples) of function {} must be an array of tuples (timestamp, value), got {}",
                name, arguments[0].column->getName());

        const IColumn & value_column = tuples_column->getColumn(1);
        if (checkAndGetColumn<ColumnFloat32>(&value_column))
            return samples_column_holder;

        const auto * float64_values = checkAndGetColumn<ColumnFloat64>(&value_column);
        if (!float64_values)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected column {} of values in function {}",
                value_column.getName(), name);

        const auto & values_data = float64_values->getData();
        const size_t total_samples = values_data.size();
        const size_t first_stale = findFirstStaleMarker(values_data.data(), total_samples);
        if (first_stale == total_samples)
            return samples_column_holder;

        size_t stale_count = 1;
        for (size_t i = first_stale + 1; i < total_samples; ++i)
            stale_count += static_cast<size_t>(std::bit_cast<UInt64>(values_data[i]) == STALE_NAN_BITS);

        const size_t res_size = total_samples - stale_count;
        const auto & source_tuples = samples_column->getData();
        const auto & offsets = samples_column->getOffsets();

        auto res_tuples = source_tuples.cloneEmpty();
        res_tuples->reserve(res_size);

        auto res_offsets = ColumnArray::ColumnOffsets::create(input_rows_count);
        auto & res_offsets_data = res_offsets->getData();

        const size_t first_stale_row = static_cast<size_t>(
            std::upper_bound(offsets.begin(), offsets.end(), first_stale) - offsets.begin());

        if (first_stale_row > 0)
            memcpy(res_offsets_data.data(), offsets.data(), first_stale_row * sizeof(ColumnArray::Offset));

        size_t copy_begin = 0;
        size_t out_offset = (first_stale_row == 0) ? 0 : offsets[first_stale_row - 1];

        for (size_t row = first_stale_row; row < input_rows_count; ++row)
        {
            const size_t begin = (row == 0) ? 0 : offsets[row - 1];
            const size_t end = offsets[row];

            for (size_t idx = begin; idx < end; ++idx)
            {
                if (std::bit_cast<UInt64>(values_data[idx]) == STALE_NAN_BITS)
                {
                    if (copy_begin < idx)
                        res_tuples->insertRangeFrom(source_tuples, copy_begin, idx - copy_begin);
                    copy_begin = idx + 1;
                }
                else
                {
                    ++out_offset;
                }
            }
            res_offsets_data[row] = out_offset;
        }

        if (copy_begin < total_samples)
            res_tuples->insertRangeFrom(source_tuples, copy_begin, total_samples - copy_begin);

        chassert(res_tuples->size() == res_size);
        chassert(input_rows_count == 0 || res_offsets_data[input_rows_count - 1] == res_size);

        return ColumnArray::create(std::move(res_tuples), std::move(res_offsets));
    }
};


REGISTER_FUNCTION(TimeSeriesRemoveStaleMarkers)
{
    factory.registerFunction<FunctionTimeSeriesRemoveStaleMarkers>(FunctionDocumentation::INTERNAL_FUNCTION_DOCS);
}

}
