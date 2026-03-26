#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/transformEndianness.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsHashing.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>

#include <map>


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
    /// Layout of the 128-bit identifier (same physical order as UInt128 / sipHash128 FixedString):
    /// - High 64 bits ("locality"): first 6 bytes of UTF-8 `metric_name` (big-endian, zero-padded), then
    ///   the first 2 bytes of the canonical sorted label-key string (big-endian, zero-padded).
    /// - Low 64 bits: same chaining rule as sipHash64, but over metric + (tag key, tag value)* in sorted key
    ///   order after merging map entries with promoted `(tag_name, tag_value)` pairs (so promotion does
    ///   not change the id when label names and values are the same).
    ///
    /// Changing this packing is a compatibility break for on-disk `id` values.

    const IColumn * tryUnwrapNullable(const IColumn & column, ColumnPtr & holder)
    {
        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(&column))
        {
            holder = nullable->getNestedColumnPtr();
            return holder.get();
        }
        return &column;
    }

    std::string_view getStringViewAtRow(const IColumn & column, size_t row)
    {
        if (const auto * col_const = checkAndGetColumn<ColumnConst>(&column))
            return col_const->getDataAt(0);

        if (const auto * str = checkAndGetColumn<ColumnString>(&column))
            return str->getDataAt(row);

        if (const auto * fs = checkAndGetColumn<ColumnFixedString>(&column))
            return fs->getDataAt(row);

        throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                        "Argument of function timeSeriesLocalityId must be a String column, got {}",
                        column.getName());
    }

    void appendMapKeyValuesFromRow(
        const IColumn & map_column_input, size_t row, std::map<std::string, std::string> & tag_kv, bool only_insert_if_absent)
    {
        ColumnPtr col = map_column_input.convertToFullColumnIfConst();
        col = col->convertToFullColumnIfLowCardinality();

        const IColumn * base = col.get();
        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(base))
        {
            if (nullable->isNullAt(row))
                return;
            base = &nullable->getNestedColumn();
        }

        const auto * map = checkAndGetColumn<ColumnMap>(base);
        if (!map)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "Last argument of function timeSeriesLocalityId must be a Map column, got {}",
                            map_column_input.getName());

        const auto & nested_col = map->getNestedColumn();
        const auto * array = checkAndGetColumn<ColumnArray>(&nested_col);
        if (!array)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected nested column in Map for function timeSeriesLocalityId");

        const auto * tuple = checkAndGetColumn<ColumnTuple>(&array->getData());
        if (!tuple || tuple->tupleSize() != 2)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected Map nested structure for function timeSeriesLocalityId");

        const IColumn & keys = tuple->getColumn(0);
        const IColumn & vals = tuple->getColumn(1);
        const auto & offsets = array->getOffsets();
        size_t start = row == 0 ? 0 : offsets[row - 1];
        size_t end = offsets[row];
        for (size_t j = start; j < end; ++j)
        {
            auto key_ref = keys.getDataAt(j);
            if (key_ref == TimeSeriesTagNames::MetricName)
                continue;
            auto val_ref = vals.getDataAt(j);
            std::string k(reinterpret_cast<const char *>(key_ref.data()), key_ref.size());
            std::string v(reinterpret_cast<const char *>(val_ref.data()), val_ref.size());
            if (only_insert_if_absent)
                tag_kv.emplace(std::move(k), std::move(v));
            else
                tag_kv.insert_or_assign(std::move(k), std::move(v));
        }
    }

    void fillMergedTags(const ColumnsWithTypeAndName & arguments, size_t row, std::map<std::string, std::string> & tag_kv)
    {
        tag_kv.clear();
        if (arguments.size() == 2)
        {
            appendMapKeyValuesFromRow(*arguments[1].column, row, tag_kv, /*only_insert_if_absent=*/false);
            return;
        }

        /// Remaining labels from the map first; promoted columns then override (canonical values for keys
        /// that appear in both should match; if not, promoted wins like DEFAULT id expressions do).
        appendMapKeyValuesFromRow(*arguments.back().column, row, tag_kv, /*only_insert_if_absent=*/true);
        for (size_t i = 1; i + 2 < arguments.size(); i += 2)
        {
            std::string_view name = getStringViewAtRow(*arguments[i].column->convertToFullColumnIfConst(), row);
            if (name.empty() || name == TimeSeriesTagNames::MetricName)
                continue;
            std::string_view val = getStringViewAtRow(*arguments[i + 1].column->convertToFullColumnIfConst(), row);
            tag_kv.insert_or_assign(std::string(name), std::string(val));
        }
    }

    std::string makeCanonicalKeyString(const std::map<std::string, std::string> & tag_kv)
    {
        std::string canonical_keys;
        canonical_keys.reserve(128);
        for (const auto & [k, _] : tag_kv)
        {
            if (!canonical_keys.empty())
                canonical_keys.push_back('\x01');
            canonical_keys.append(k);
        }
        return canonical_keys;
    }

    UInt64 canonicalSipHash64MergedTags(std::string_view metric_utf8, const std::map<std::string, std::string> & tag_kv)
    {
        UInt64 h = 0;
        bool first = true;
        auto feed = [&](std::string_view s)
        {
            UInt64 piece = SipHash64Impl::apply(s.data(), s.size());
            if (first)
            {
                h = piece;
                first = false;
            }
            else
                h = SipHash64Impl::combineHashes(h, piece);
        };

        feed(metric_utf8);
        for (const auto & [k, v] : tag_kv)
        {
            feed(k);
            feed(v);
        }
        return h;
    }

    void checkStringLikeType(const DataTypePtr & type, size_t arg_index)
    {
        auto nested = removeNullable(removeLowCardinality(type));
        WhichDataType which(*nested);
        if (which.isString() || which.isFixedString() || which.isNothing())
            return;
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Argument #{} of function timeSeriesLocalityId must be String-like, got {}",
                        arg_index,
                        nested->getName());
    }

    void checkMapType(const DataTypePtr & type, size_t arg_index)
    {
        auto nested = removeNullable(removeLowCardinality(type));
        if (typeid_cast<const DataTypeMap *>(nested.get()))
            return;
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Argument #{} of function timeSeriesLocalityId must be Map, got {}",
                        arg_index,
                        nested->getName());
    }

    UInt64 makeLocalityHighBits(std::string_view metric_utf8, const std::string & canonical_keys)
    {
        UInt64 hi = 0;
        for (size_t b = 0; b < 6; ++b)
        {
            uint8_t byte = b < metric_utf8.size() ? static_cast<uint8_t>(metric_utf8[b]) : 0;
            hi = (hi << 8) | byte;
        }
        hi <<= 16;
        uint16_t keys16 = 0;
        if (!canonical_keys.empty())
            keys16 = static_cast<uint16_t>(
                (static_cast<uint16_t>(static_cast<uint8_t>(canonical_keys[0])) << 8)
                | static_cast<uint16_t>(canonical_keys.size() > 1 ? static_cast<uint8_t>(canonical_keys[1]) : 0));
        hi |= keys16;
        return hi;
    }
}


/// timeSeriesLocalityId(metric_name[, tag_name_literal, tag_value_column]..., tags_or_all_tags_map)
/// The tag name literals must be constant String columns (as produced by TimeSeries DEFAULT generation).
class FunctionTimeSeriesLocalityId final : public IFunction
{
public:
    static constexpr auto name = "timeSeriesLocalityId";

    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<FunctionTimeSeriesLocalityId>(); }

public:
    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isDeterministic() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} requires at least two arguments",
                            name);

        if (arguments.size() == 2)
        {
            checkStringLikeType(arguments[0], 1);
            checkMapType(arguments[1], 2);
            return std::make_shared<DataTypeFixedString>(16);
        }

        if ((arguments.size() - 2) % 2 != 0)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} expects (metric_name, [tag_name, tag_value]..., tags_map); "
                            "the number of arguments between metric_name and the map must be even",
                            name);

        checkStringLikeType(arguments[0], 1);
        for (size_t i = 1; i + 2 < arguments.size(); i += 2)
        {
            checkStringLikeType(arguments[i], i + 1);
            checkStringLikeType(arguments[i + 1], i + 2);
        }
        checkMapType(arguments.back(), arguments.size());
        return std::make_shared<DataTypeFixedString>(16);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_to = ColumnFixedString::create(sizeof(UInt128));
        auto & chars = col_to->getChars();
        chars.resize(input_rows_count * sizeof(UInt128));

        std::map<std::string, std::string> tag_kv;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            ColumnPtr mholder;
            const auto * metric_col = tryUnwrapNullable(*arguments[0].column->convertToFullColumnIfConst(), mholder);
            std::string_view metric_view = getStringViewAtRow(*metric_col, row);

            fillMergedTags(arguments, row, tag_kv);
            std::string canonical_keys = makeCanonicalKeyString(tag_kv);

            UInt64 locality_hi = makeLocalityHighBits(metric_view, canonical_keys);
            UInt64 hash_lo = canonicalSipHash64MergedTags(metric_view, tag_kv);
            UInt128 combined = (UInt128{locality_hi} << 64) | UInt128{hash_lo};

            if constexpr (std::endian::native == std::endian::big)
                transformEndianness<std::endian::little>(combined);

            memcpy(&chars[row * sizeof(UInt128)], &combined, sizeof(UInt128));
        }

        return col_to;
    }
};


REGISTER_FUNCTION(TimeSeriesLocalityId)
{
    FunctionDocumentation::Description description = R"(
Builds a 128-bit series id for `MergeTree` ordering: high 64 bits depend on `metric_name` and the sorted set of
label *names* (locality); low 64 bits use the same `sipHash64` chaining as the multi-argument function, but over
`metric_name` plus `(tag_name, tag_value)` pairs merged from `tags_map` and promoted columns in **sorted tag-name
order** (so the id does not change when a label moves between the map and a promoted column).
Compared with `sipHash128`, series that share the same metric and tag-key shape get closer ids under `ORDER BY (id, timestamp)`.

Replacing the default `id` with this function is breaking versus `sipHash128` / `reinterpretAsUUID` until data is reloaded.
`UInt64` ids are too narrow for this split layout and still default to `sipHash64` alone.
    )";

    /*
    Logical tags: start from all `(key,value)` pairs in the trailing map except `__name__` (insert_or_assign).
    Then apply each promoted `(tag_name_literal, tag_value_column)` with insert_or_assign so column values win if a
    key exists both in the map and as a promoted column.

    locality_hi: `makeLocalityHighBits(metric, canonical_keys)` where `canonical_keys` joins sorted tag **names**
    with `\x01`. The low 16 bits of locality_hi are the first two bytes of that string — adding or renaming a tag
    so that the alphabetically smallest name changes will change those bits.

    low_64: `sipHash64` chaining in key order — `metric` UTF-8 bytes, then each tag name and tag value String as if
    passed as successive `sipHash64` arguments (see SipHash64Impl::combineHashes).

    Layout (one row):

                    ├─ metric UTF-8  ──►  first 6 bytes (zero-padded)  ─────────────────────────────────┐
                    └─ sorted tag names (merged map + promoted)  ──►  first 2 bytes of canonical_keys --│
                                                                                                        ▼
                   ┌────────────────────────── 64 bits (big-endian packing in hi) ────────────────────────┐
       UInt128 id: │ 6× metric bytes  │ first 2 bytes of sorted-name join (`\x01` between names)          │
                   └───────────────────────────────────────────────────┬──────────────────────────────────┘
                                                                       │
                Stored as FixedString(16) / UInt128 little-endian:  [ locality_hi (64) | low_64 ]

    Invariant (map-only vs promoted, same labels): `timeSeriesLocalityId('m', map('job','x','k','v'))` must equal
    `timeSeriesLocalityId('m','job','x',map('k','v'))` — covered by stateless tests.

    Example (three rows; low_64 is canonical merged `sipHash64`, not raw SQL arguments):

      row aa:  timeSeriesLocalityId('cpu', 'env', 'e1', 'instance', 'aa', map())
      row bb:  timeSeriesLocalityId('cpu', 'env', 'e1', 'instance', 'bb', map('alphabetic_sort_breaker', 'true'))
      row cc:  timeSeriesLocalityId('cpu', 'env', 'e2', 'instance', 'cc', map('z_alphabetic_sort_maintainer', 'true'))

      aa has only promoted `env` / `instance`. bb adds map key `alphabetic_sort_breaker` (lexicographically before
      `env`, so it becomes the first segment of the canonical name string). cc adds `z_alphabetic_sort_maintainer`
      (after `env` and `instance`) and different promoted values. Sorted names: aa → `env`,`instance`; bb →
      `alphabetic_sort_breaker`,`env`,`instance`; cc → `env`,`instance`,`z_alphabetic_sort_maintainer`.

      locality_hi takes the first two UTF-8 bytes of the `\x01`-joined sorted names. aa and cc still start with
      `env…` → `locality_hi = 0x637075000000656e`. bb starts with `alphabetic_sort_breaker…` → first two bytes are
      `al` → `locality_hi = 0x637075000000616c` (smaller high half than aa/cc).

      As read by `reinterpretAsUInt128(...)` on little-endian hosts (`hex(...)` may use upper-case digits), **ascending**
      full `UInt128` (same order as `ORDER BY id`):

            row bb — locality_hi 0x637075000000616c  low_64 0x3f5e8b3a9628f394  full 0x637075000000616c3f5e8b3a9628f394
            row cc — locality_hi 0x637075000000656e  low_64 0x15a0d1438ca01bac  full 0x637075000000656e15a0d1438ca01bac
            row aa — locality_hi 0x637075000000656e  low_64 0x5ec965d7e8cf409f   full 0x637075000000656e5ec965d7e8cf409f
    */
    FunctionDocumentation::Syntax syntax = "timeSeriesLocalityId(metric_name[, tag_name, tag_value, ..., tags_map])";
    FunctionDocumentation::Arguments arguments_syntax = {
        {"metric_name", "Prometheus metric name (`__name__`).", {"String"}},
        {"tag_name", "Label name for a promoted tag column (constant).", {"String"}},
        {"tag_value", "Label value from the corresponding column.", {"String"}},
        {"tags_map", "`tags` or `all_tags` map column.", {"Map"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `FixedString(16)` (same representation as `sipHash128`).", {"FixedString(16)"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 3};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments_syntax, {}, returned_value, {}, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesLocalityId>(documentation);
}

}
