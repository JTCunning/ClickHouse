#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionQuantile.h>
#include <Core/Field.h>
#include <Common/HashTable/HashMap.h>
#include <Common/NaNUtils.h>

#include <base/sort.h>

#include <cmath>
#include <limits>


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/** Computes quantiles from pre-bucketed data following the exponential bucketing convention
  * of OpenTelemetry exponential histograms / Prometheus native histograms.
  *
  * The `schema` parameter (1..8) defines the bucket grid: at schema `s`, bucket `i` covers
  * `[2^(i/2^s), 2^((i+1)/2^s))`, i.e. `2^s` buckets per doubling of the value. A raw value `v > 0`
  * maps to bucket `floor(log2(v) * 2^s)`. Zero values are counted in the reserved sentinel
  * bucket `-1`.
  *
  * The state is a sparse map from bucket index to count; merging states adds counts per index,
  * which is what makes pre-bucketed rows mergeable across shards and time windows.
  *
  * The quantile is computed by walking buckets in increasing index order, accumulating counts
  * until the target rank `level * total` is crossed, then interpolating geometrically
  * (log-linearly) inside the crossing bucket: `2^((i + fraction) / 2^s)`. Geometric
  * interpolation matches the Prometheus native-histogram convention and is the right choice
  * for log-spaced buckets. Special cases: a target rank landing in the zero bucket returns 0.0;
  * `level = 1` returns the upper bound of the last bucket; an empty state returns NaN.
  */
template <typename Value, typename Count>
struct QuantileExponentialHistogram
{
    /// Bucket indexes are normalized to Int64 regardless of the input column type.
    using BucketIndex = Int64;
    using Hasher = HashCRC32<BucketIndex>;

    /// When creating, the hash table must be small.
    using Map = HashMapWithStackMemory<BucketIndex, Count, Hasher, 4>;
    using Pair = typename Map::value_type;

    /// Bucket index reserved for zero values.
    static constexpr BucketIndex zero_bucket = -1;

    Map map;
    UInt8 schema = 1;

    QuantileExponentialHistogram() = default;
    explicit QuantileExponentialHistogram(size_t schema_) : schema(static_cast<UInt8>(schema_)) { }

    void add(const Value & bucket_index, Count bucket_count)
    {
        map[static_cast<BucketIndex>(bucket_index)] += bucket_count;
    }

    void merge(const QuantileExponentialHistogram & rhs)
    {
        for (const auto & pair : rhs.map)
            map[pair.getKey()] += pair.getMapped();
    }

    void serialize(WriteBuffer & buf) const
    {
        map.write(buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        typename Map::Reader reader(buf);
        while (reader.next())
        {
            const auto & pair = reader.get();
            map[pair.first] = pair.second;
        }
    }

    Float64 getFloat(Float64 level) const
    {
        size_t size = map.size();
        if (0 == size)
            return std::numeric_limits<Float64>::quiet_NaN();

        std::unique_ptr<Pair[]> array_holder(new Pair[size]);
        Pair * array = array_holder.get();

        size_t i = 0;
        for (const auto & pair : map)
        {
            array[i] = pair.getValue();
            ++i;
        }

        ::sort(array, array + size, [](const Pair & a, const Pair & b) { return a.first < b.first; });

        return quantileInterpolated(array, size, level);
    }

    void getManyFloat(const Float64 * levels, const size_t * indices, size_t num_levels, Float64 * result) const
    {
        size_t size = map.size();
        if (0 == size)
        {
            for (size_t i = 0; i < num_levels; ++i)
                result[i] = std::numeric_limits<Float64>::quiet_NaN();
            return;
        }

        std::unique_ptr<Pair[]> array_holder(new Pair[size]);
        Pair * array = array_holder.get();

        size_t i = 0;
        for (const auto & pair : map)
        {
            array[i] = pair.getValue();
            ++i;
        }

        ::sort(array, array + size, [](const Pair & a, const Pair & b) { return a.first < b.first; });

        for (size_t j = 0; j < num_levels; ++j)
            result[indices[j]] = quantileInterpolated(array, size, levels[indices[j]]);
    }

private:
    /// Lower bound of bucket `index`: 2^(index / 2^schema).
    Float64 bucketLowerBound(BucketIndex index) const
    {
        return std::exp2(static_cast<Float64>(index) / static_cast<Float64>(UInt64(1) << schema));
    }

    Float64 bucketUpperBound(BucketIndex index) const
    {
        if (index == zero_bucket)
            return 0.0;
        return bucketLowerBound(index + 1);
    }

    Float64 quantileInterpolated(const Pair * array, size_t size, Float64 level) const
    {
        Float64 total = 0;
        for (size_t i = 0; i < size; ++i)
            total += static_cast<Float64>(array[i].second);

        if (!(total > 0))
            return std::numeric_limits<Float64>::quiet_NaN();

        if (level >= 1.0)
            return bucketUpperBound(array[size - 1].first);

        Float64 target = level * total;
        Float64 cumulative = 0;

        for (size_t i = 0; i < size; ++i)
        {
            Float64 count = static_cast<Float64>(array[i].second);
            if (!(count > 0))
                continue;

            Float64 previous_cumulative = cumulative;
            cumulative += count;

            if (cumulative >= target)
            {
                BucketIndex bucket_index = array[i].first;
                if (bucket_index == zero_bucket)
                    return 0.0;

                Float64 fraction = (target - previous_cumulative) / count;
                return std::exp2((static_cast<Float64>(bucket_index) + fraction) / static_cast<Float64>(UInt64(1) << schema));
            }
        }

        /// Not reachable except through floating-point rounding of `target`.
        return bucketUpperBound(array[size - 1].first);
    }
};

template <typename Value, typename Count>
using FuncQuantileExponentialHistogram = AggregateFunctionQuantile<
    Value,
    QuantileExponentialHistogram<Value, Count>,
    NameQuantileExponentialHistogram,
    Count,
    Float64,
    false,
    true>;
template <typename Value, typename Count>
using FuncQuantilesExponentialHistogram = AggregateFunctionQuantile<
    Value,
    QuantileExponentialHistogram<Value, Count>,
    NameQuantilesExponentialHistogram,
    Count,
    Float64,
    true,
    true>;

template <template <typename, typename> class Function, typename Count>
AggregateFunctionPtr createWithBucketIndexType(const DataTypePtr & bucket_index_type, const DataTypes & argument_types, const Array & params)
{
    WhichDataType which(bucket_index_type);
    switch (which.idx)
    {
        case TypeIndex::Int8: return std::make_shared<Function<Int8, Count>>(argument_types, params);
        case TypeIndex::Int16: return std::make_shared<Function<Int16, Count>>(argument_types, params);
        case TypeIndex::Int32: return std::make_shared<Function<Int32, Count>>(argument_types, params);
        case TypeIndex::Int64: return std::make_shared<Function<Int64, Count>>(argument_types, params);
        case TypeIndex::UInt8: return std::make_shared<Function<UInt8, Count>>(argument_types, params);
        case TypeIndex::UInt16: return std::make_shared<Function<UInt16, Count>>(argument_types, params);
        case TypeIndex::UInt32: return std::make_shared<Function<UInt32, Count>>(argument_types, params);
        case TypeIndex::UInt64: return std::make_shared<Function<UInt64, Count>>(argument_types, params);
        default: return nullptr;
    }
}

template <template <typename, typename> class Function>
AggregateFunctionPtr createAggregateFunctionQuantile(
    const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (argument_types.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires two arguments", name);

    /// The generic constructor of AggregateFunctionQuantile validates that the first parameter
    /// is a positive integer. The exponential bucketing convention additionally caps it at 8
    /// (256 buckets per doubling of the value).
    if (!params.empty() && isInt64OrUInt64FieldType(params[0].getType()))
    {
        Int64 schema = params[0].getType() == Field::Types::Int64 ? params[0].safeGet<Int64>()
                                                                  : static_cast<Int64>(params[0].safeGet<UInt64>());
        if (schema < 1 || schema > 8)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Aggregate function {} requires schema parameter in range [1, 8] but is {}", name, schema);
    }

    const DataTypePtr & bucket_index_type = argument_types[0];
    const DataTypePtr & bucket_count_type = argument_types[1];
    WhichDataType which_bucket_count(bucket_count_type);

    AggregateFunctionPtr res;
    if (isUInt(which_bucket_count.idx))
        res = createWithBucketIndexType<Function, UInt64>(bucket_index_type, argument_types, params);
    else if (isFloat(which_bucket_count.idx))
        res = createWithBucketIndexType<Function, Float64>(bucket_index_type, argument_types, params);
    else
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of second argument for aggregate function {}",
                        bucket_count_type->getName(), name);

    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of first argument for aggregate function {}",
                        bucket_index_type->getName(), name);

    return res;
}

}

void registerAggregateFunctionsQuantileExponentialHistogram(AggregateFunctionFactory & factory);
void registerAggregateFunctionsQuantileExponentialHistogram(AggregateFunctionFactory & factory)
{
    /// For aggregate functions returning array we cannot return NULL on empty set.
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true };

    FunctionDocumentation::Description description_quantileExponentialHistogram = R"(
Computes a [quantile](https://en.wikipedia.org/wiki/Quantile) from pre-bucketed data following the exponential bucketing convention of [OpenTelemetry exponential histograms](https://opentelemetry.io/docs/specs/otel/metrics/data-model/#exponentialhistogram) and [Prometheus native histograms](https://prometheus.io/docs/specs/native_histograms/).

The function never sees raw values. Instead, each input row carries a bucket index and the number of observations in that bucket. The `schema` parameter (an integer from 1 to 8) defines the bucket grid: at schema `s`, bucket `i` covers the value range `[2^(i/2^s), 2^((i+1)/2^s))`, which gives `2^s` buckets per doubling of the value. A raw value `v` greater than zero maps to bucket `floor(log2(v) * 2^s)`. Zero values are counted in the reserved sentinel bucket `-1`.

Because the grid is fixed by `schema` alone, two independent writers bucketing with the same schema produce compatible indexes, and bucket counts merge by plain addition. This makes the pre-bucketed rows mergeable across shards, time windows, and services, for example with a `SummingMergeTree` table keyed by dimensions and bucket index.

The quantile is computed by walking the buckets in increasing index order, accumulating counts until the target rank `level * total_count` is crossed, then interpolating geometrically (log-linearly) inside the crossing bucket: `2^((i + fraction) / 2^s)`. If the target rank lands in the zero bucket, the result is `0`. `level = 1` returns the upper bound of the last bucket. An empty input returns `NaN`.
    )";
    FunctionDocumentation::Syntax syntax_quantileExponentialHistogram = R"(
quantileExponentialHistogram(schema[, level])(bucket_index, bucket_count)
    )";
    FunctionDocumentation::Parameters parameters_quantileExponentialHistogram = {
        {"schema", "Exponential bucketing schema. Constant integer from 1 to 8. Bucket `i` covers `[2^(i/2^s), 2^((i+1)/2^s))`, i.e. `2^schema` buckets per doubling of the value.", {"UInt8"}},
        {"level", "Optional. Level of quantile. Constant floating-point number from 0 to 1. Default value: `0.5`. At `level=0.5` the function calculates the [median](https://en.wikipedia.org/wiki/Median).", {"Float64"}}
    };
    FunctionDocumentation::Arguments arguments_quantileExponentialHistogram = {
        {"bucket_index", "Exponential bucket index computed as `floor(log2(value) * 2^schema)`. The index `-1` is reserved for zero values.", {"Int*", "UInt*"}},
        {"bucket_count", "Number of observations in the bucket.", {"UInt*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_quantileExponentialHistogram = {"Returns the quantile of the specified level, interpolated geometrically inside the bucket in which the quantile rank is found. Returns `NaN` for an empty input.", {"Float64"}};
    FunctionDocumentation::Examples examples_quantileExponentialHistogram = {
    {
        "Usage example",
        R"(
SELECT quantileExponentialHistogram(3, 0.9)(bucket_index, bucket_count)
FROM VALUES('bucket_index Int64, bucket_count UInt64', (-1, 20), (0, 20), (13, 30), (23, 20), (28, 10));
        )",
        R"(
┌─quantileExpon⋯cket_count)─┐
│         6.727171322029716 │
└───────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_quantileExponentialHistogram = {26, 7};
    FunctionDocumentation::Category category_quantileExponentialHistogram = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_quantileExponentialHistogram = {description_quantileExponentialHistogram, syntax_quantileExponentialHistogram, arguments_quantileExponentialHistogram, parameters_quantileExponentialHistogram, returned_value_quantileExponentialHistogram, examples_quantileExponentialHistogram, introduced_in_quantileExponentialHistogram, category_quantileExponentialHistogram};

    factory.registerFunction(NameQuantileExponentialHistogram::name, {createAggregateFunctionQuantile<FuncQuantileExponentialHistogram>, documentation_quantileExponentialHistogram});

    FunctionDocumentation::Description description_quantilesExponentialHistogram = R"(
Computes multiple [quantiles](https://en.wikipedia.org/wiki/Quantile) from pre-bucketed exponential histogram data at different levels simultaneously.

This function is equivalent to [`quantileExponentialHistogram`](/sql-reference/aggregate-functions/reference/quantileExponentialHistogram) but computes multiple quantile levels in a single pass, which is more efficient than calling individual quantile functions.
    )";
    FunctionDocumentation::Syntax syntax_quantilesExponentialHistogram = R"(
quantilesExponentialHistogram(schema, level1[, level2, ...])(bucket_index, bucket_count)
    )";
    FunctionDocumentation::Parameters parameters_quantilesExponentialHistogram = {
        {"schema", "Exponential bucketing schema. Constant integer from 1 to 8. Bucket `i` covers `[2^(i/2^s), 2^((i+1)/2^s))`, i.e. `2^schema` buckets per doubling of the value.", {"UInt8"}},
        {"level", "Levels of quantiles. One or more constant floating-point numbers from 0 to 1.", {"Float64"}}
    };
    FunctionDocumentation::Arguments arguments_quantilesExponentialHistogram = {
        {"bucket_index", "Exponential bucket index computed as `floor(log2(value) * 2^schema)`. The index `-1` is reserved for zero values.", {"Int*", "UInt*"}},
        {"bucket_count", "Number of observations in the bucket.", {"UInt*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_quantilesExponentialHistogram = {"Array of quantiles of the specified levels in the same order as the levels were specified.", {"Array(Float64)"}};
    FunctionDocumentation::Examples examples_quantilesExponentialHistogram = {
    {
        "Usage example",
        R"(
SELECT quantilesExponentialHistogram(3, 0.5, 0.9, 0.99)(bucket_index, bucket_count)
FROM VALUES('bucket_index Int64, bucket_count UInt64', (-1, 20), (0, 20), (13, 30), (23, 20), (28, 10));
        )",
        R"(
┌─quantilesExpo⋯cket_count)─┐
│ [1.834008086409342,6.727171322029716,11.660842789603779] │
└───────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_quantilesExponentialHistogram = {26, 7};
    FunctionDocumentation::Category category_quantilesExponentialHistogram = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_quantilesExponentialHistogram = {description_quantilesExponentialHistogram, syntax_quantilesExponentialHistogram, arguments_quantilesExponentialHistogram, parameters_quantilesExponentialHistogram, returned_value_quantilesExponentialHistogram, examples_quantilesExponentialHistogram, introduced_in_quantilesExponentialHistogram, category_quantilesExponentialHistogram};

    factory.registerFunction(NameQuantilesExponentialHistogram::name, {createAggregateFunctionQuantile<FuncQuantilesExponentialHistogram>, documentation_quantilesExponentialHistogram, properties});
}

}
