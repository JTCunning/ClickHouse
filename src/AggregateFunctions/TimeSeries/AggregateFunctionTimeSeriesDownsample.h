#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/TimeSeries/timeseriesMaxValueForDuplicateTimestamp.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Common/Arena.h>
#include <Common/ArenaAllocator.h>
#include <Common/PODArray.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/TimeSeries/TimeSeriesDownsamplingPeriods.h>

#include <base/sort.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

/// Keeps the last sample of each right-closed interval `(k * I, (k + 1) * I]`.
/// The stored timestamp is the original timestamp of that sample.
/// Equal timestamps use `timeseriesMaxValueForDuplicateTimestamp` (a NaN loses).
template <typename TimestampType, typename ValueType>
class AggregateFunctionTimeSeriesDownsample final :
    public IAggregateFunctionHelper<AggregateFunctionTimeSeriesDownsample<TimestampType, ValueType>>
{
public:
    using Base = IAggregateFunctionHelper<AggregateFunctionTimeSeriesDownsample<TimestampType, ValueType>>;

    using TimestampColumnType = ColumnVectorOrDecimal<TimestampType>;
    using ValueColumnType = ColumnVectorOrDecimal<ValueType>;

    String getName() const override
    {
        return "timeSeriesDownsample";
    }

    struct Element
    {
        TimestampType interval_end;
        TimestampType timestamp;
        ValueType value;
    };

    using ElementsAllocator = MixedAlignedArenaAllocator<alignof(Element), 4096>;
    using Elements = PODArray<Element, 32, ElementsAllocator>;

    struct Data
    {
        Elements elements;
        bool sorted = true;

        static TimestampType intervalEnd(TimestampType timestamp, Int64 scaled_interval)
        {
            if constexpr (is_decimal<TimestampType>)
                return TimestampType(timeSeriesDownsampleIntervalEnd(timestamp.value, scaled_interval));
            else
            {
                const UInt64 end = UInt64(timestamp) + UInt64(scaled_interval) - 1;
                return TimestampType(end - end % UInt64(scaled_interval));
            }
        }

        static void keepWinner(Element & current, const Element & candidate)
        {
            if (candidate.timestamp > current.timestamp)
                current = candidate;
            else if (candidate.timestamp == current.timestamp)
                current.value = timeseriesMaxValueForDuplicateTimestamp(current.value, candidate.value);
        }

        void add(TimestampType timestamp, ValueType value, Int64 scaled_interval, Arena * arena)
        {
            Element candidate{
                .interval_end = intervalEnd(timestamp, scaled_interval),
                .timestamp = timestamp,
                .value = value};

            if (!elements.empty() && elements.back().interval_end == candidate.interval_end)
            {
                keepWinner(elements.back(), candidate);
                return;
            }

            if (!elements.empty() && candidate.interval_end < elements.back().interval_end)
                sorted = false;

            elements.push_back(candidate, arena);
        }

        void merge(const Data & rhs, Int64 scaled_interval, Arena * arena)
        {
            if (rhs.elements.empty())
                return;

            if (elements.empty())
            {
                elements.assign(rhs.elements.begin(), rhs.elements.end(), arena);
                sorted = rhs.sorted;
                sort(scaled_interval, arena);
                return;
            }

            sort(scaled_interval, arena);

            const Elements * rhs_elements = &rhs.elements;
            Elements sorted_rhs_elements;
            if (!rhs.sorted) [[unlikely]]
            {
                sorted_rhs_elements.assign(rhs.elements.begin(), rhs.elements.end(), arena);
                sorted_rhs_elements.resize_exact(
                    sortElements(sorted_rhs_elements.data(), sorted_rhs_elements.size()), arena);
                rhs_elements = &sorted_rhs_elements;
            }

            if (elements.back().interval_end < rhs_elements->front().interval_end)
            {
                elements.insert(rhs_elements->begin(), rhs_elements->end(), arena);
                return;
            }

            if (rhs_elements->back().interval_end < elements.front().interval_end)
            {
                Elements prepended;
                prepended.reserve_exact(rhs_elements->size() + elements.size(), arena);
                prepended.insert_assume_reserved(rhs_elements->begin(), rhs_elements->end());
                prepended.insert_assume_reserved(elements.begin(), elements.end());
                elements.swap(prepended, arena);
                return;
            }

            Elements merged;
            merged.resize_exact(elements.size() + rhs_elements->size(), arena);
            std::merge(
                elements.begin(), elements.end(), rhs_elements->begin(), rhs_elements->end(), merged.begin(), lessByIntervalEnd);
            merged.resize_exact(deduplicateSorted(merged.data(), merged.size()), arena);
            elements.swap(merged, arena);
        }

        void sort(Int64 /*interval_scaled*/, Arena * arena)
        {
            if (sorted)
                return;
            elements.resize_exact(sortElements(elements.data(), elements.size()), arena);
            sorted = true;
        }

        static bool lessByIntervalEnd(const Element & lhs, const Element & rhs)
        {
            return lhs.interval_end < rhs.interval_end;
        }

        static size_t deduplicateSorted(Element * elements, size_t size)
        {
            if (size == 0)
                return 0;

            size_t last_unique = 0;
            for (size_t i = 1; i < size; ++i)
            {
                if (elements[i].interval_end == elements[last_unique].interval_end)
                    keepWinner(elements[last_unique], elements[i]);
                else
                    elements[++last_unique] = elements[i];
            }
            return last_unique + 1;
        }

        static size_t sortElements(Element * elements, size_t size)
        {
            ::sort(elements, elements + size, lessByIntervalEnd);
            return deduplicateSorted(elements, size);
        }
    };

    AggregateFunctionTimeSeriesDownsample(const DataTypes & argument_types_, const Array & parameters_, Int64 interval_scaled_)
        : Base(argument_types_, parameters_, createResultType(argument_types_))
        , array_of_pairs_argument(argument_types_.size() == 1)
        , interval_scaled(interval_scaled_)
    {
    }

    static DataTypePtr createResultType(const DataTypes & argument_types_)
    {
        if (argument_types_.size() == 1)
        {
            /// A `SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(...)))` argument keeps that type,
            /// so a TTL `SET` can write the result back into the same column.
            if (argument_types_[0]->getCustomName())
                return argument_types_[0];

            const auto & array_type = typeid_cast<const DataTypeArray &>(*argument_types_[0]);
            const auto & tuple_type = typeid_cast<const DataTypeTuple &>(*array_type.getNestedType());
            if (tuple_type.hasExplicitNames())
                return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(tuple_type.getElements(), tuple_type.getElementNames()));
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(tuple_type.getElements()));
        }

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{argument_types_[0], argument_types_[1]}));
    }

    bool allocatesMemoryInArena() const override { return true; }

    bool hasTrivialDestructor() const override
    {
        return std::is_trivially_destructible_v<Data>;
    }

    size_t alignOfData() const override
    {
        return alignof(Data);
    }

    size_t sizeOfData() const override
    {
        return sizeof(Data);
    }

    void create(AggregateDataPtr __restrict place) const override /// NOLINT(readability-non-const-parameter)
    {
        new (place) Data{};
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        data(place).~Data();
    }

    static Data & data(AggregateDataPtr __restrict place) /// NOLINT(readability-non-const-parameter)
    {
        return *reinterpret_cast<Data *>(place);
    }

    static const Data & data(ConstAggregateDataPtr __restrict place)
    {
        return *reinterpret_cast<const Data *>(place);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        Data & state = data(place);
        if (!array_of_pairs_argument)
        {
            const auto & timestamp_column = typeid_cast<const TimestampColumnType &>(*columns[0]);
            const auto & value_column = typeid_cast<const ValueColumnType &>(*columns[1]);
            state.add(timestamp_column.getData()[row_num], value_column.getData()[row_num], interval_scaled, arena);
            return;
        }

        const auto & array_column = typeid_cast<const ColumnArray &>(*columns[0]);
        const auto & tuple_column = typeid_cast<const ColumnTuple &>(array_column.getData());
        const auto & offsets = array_column.getOffsets();
        const size_t start = row_num == 0 ? 0 : offsets[row_num - 1];
        const size_t end = offsets[row_num];
        const auto & timestamps = typeid_cast<const TimestampColumnType &>(tuple_column.getColumn(0)).getData();
        const auto & values = typeid_cast<const ValueColumnType &>(tuple_column.getColumn(1)).getData();
        for (size_t i = start; i < end; ++i)
            state.add(timestamps[i], values[i], interval_scaled, arena);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        data(place).merge(data(rhs), interval_scaled, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const Data & state = data(place);
        if (!state.sorted) [[unlikely]]
        {
            PODArray<Element> sorted_elements;
            sorted_elements.assign(state.elements.begin(), state.elements.end());
            sorted_elements.resize_exact(Data::sortElements(sorted_elements.data(), sorted_elements.size()));
            writeElements(sorted_elements, buf);
            return;
        }

        writeElements(state.elements, buf);
    }

    template <typename Container>
    static void writeElements(const Container & elements, WriteBuffer & buf)
    {
        writeBinaryLittleEndian(FORMAT_VERSION, buf);
        writeBinaryLittleEndian(elements.size(), buf);
        for (const Element & element : elements)
            writeBinaryLittleEndian(element.timestamp, buf);
        for (const Element & element : elements)
            writeBinaryLittleEndian(element.value, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        UInt16 format_version = 0;
        readBinaryLittleEndian(format_version, buf);
        if (format_version != FORMAT_VERSION)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Cannot deserialize data with different format version, expected {}, got {}",
                FORMAT_VERSION, format_version);

        Data & state = data(place);
        state.elements.clear();
        state.sorted = true;

        size_t size = 0;
        readBinaryLittleEndian(size, buf);
        state.elements.reserve(std::min(size, MAX_ELEMENTS_TO_RESERVE), arena);

        for (size_t i = 0; i < size; ++i)
        {
            TimestampType timestamp{};
            readBinaryLittleEndian(timestamp, buf);
            state.elements.push_back(Element{
                .interval_end = Data::intervalEnd(timestamp, interval_scaled),
                .timestamp = timestamp,
                .value = ValueType{}}, arena);
            if (i > 0 && !(state.elements[state.elements.size() - 2].interval_end < state.elements.back().interval_end))
                state.sorted = false;
        }

        for (size_t i = 0; i < size; ++i)
            readBinaryLittleEndian(state.elements[i].value, buf);

        state.sort(interval_scaled, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        ColumnArray & array_to = typeid_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = array_to.getOffsets();
        ColumnTuple & tuple = typeid_cast<ColumnTuple &>(array_to.getData());

        if (tuple.tupleSize() != 2)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected tuple size 2, got {}", tuple.tupleSize());

        auto & timestamps_to = typeid_cast<TimestampColumnType &>(tuple.getColumn(0));
        auto & values_to = typeid_cast<ValueColumnType &>(tuple.getColumn(1));

        Data & state = data(place);
        state.sort(interval_scaled, arena);

        for (const Element & element : state.elements)
        {
            timestamps_to.insert(element.timestamp);
            values_to.insert(element.value);
        }

        offsets_to.push_back(offsets_to.back() + state.elements.size());
    }

private:
    const bool array_of_pairs_argument;
    const Int64 interval_scaled;

    static constexpr UInt16 FORMAT_VERSION = 1;
    static constexpr size_t MAX_ELEMENTS_TO_RESERVE = 4096;
};

}
