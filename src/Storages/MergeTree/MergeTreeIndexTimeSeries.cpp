#include <Storages/MergeTree/MergeTreeIndexTimeSeries.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/OptimizedRegularExpression.h>
#include <Common/PODArray.h>
#include <Common/ProfileEvents.h>
#include <Common/RegexpUtils.h>
#include <Common/SipHash.h>
#include <Common/Stopwatch.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeIndicesSerialization.h>

#include <roaring/roaring.hh>

#include <algorithm>
#include <numeric>


namespace ProfileEvents
{
    extern const Event TimeSeriesIndexLookupMicroseconds;
    extern const Event TimeSeriesIndexLookupParts;
    extern const Event TimeSeriesIndexLookupSeries;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CORRUPTED_DATA;
    extern const int FILE_DOESNT_EXIST;
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_MANY_ROWS;
}

namespace
{

using SubstreamType = MergeTreeIndexSubstream::Type;

constexpr UInt64 FORMAT_VERSION = 1;

/// The number of values in a block of a dictionary. A value is found by its number without a search.
constexpr size_t DICTIONARY_BLOCK_SIZE = 128;

/// The number of rows in a block of series.
constexpr size_t SERIES_BLOCK_SIZE = 1024;

/// A posting list with this many rows or fewer is kept in the dictionary next to its value.
constexpr size_t MAX_INLINE_POSTING_SIZE = 16;

/// A position inside a compressed file is taken only at these distances, so a reader decompresses little to reach it.
constexpr size_t DICTIONARY_COMPRESS_BLOCK_SIZE = 16 * 1024;
constexpr size_t SERIES_COMPRESS_BLOCK_SIZE = 64 * 1024;

/// The index is built for the whole part, as the text index is.
constexpr size_t MIN_GRANULARITY = 100000000;

const MergeTreeIndexSubstreams & getTimeSeriesIndexSubstreams()
{
    /// In the order of `MergeTreeIndexSubstream::Type`: the writer matches its streams with this list by position.
    static const MergeTreeIndexSubstreams substreams =
    {
        {SubstreamType::Regular, "", ".idx"},
        {SubstreamType::TimeSeriesIndexDictionary, ".dct", ".idx"},
        {SubstreamType::TimeSeriesIndexPostings, ".pst", ".idx"},
        {SubstreamType::TimeSeriesIndexSeries, ".srs", ".idx"},
    };
    return substreams;
}

/// The identifiers are kept as their binary representation, and a row is found in a block of series by its position.
bool hasBinaryRepresentationOfFixedSize(const DataTypePtr & type)
{
    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
        return std::ranges::all_of(tuple_type->getElements(), hasBinaryRepresentationOfFixedSize);
    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        return hasBinaryRepresentationOfFixedSize(low_cardinality_type->getDictionaryType());
    return type->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion();
}

bool isMapOfStrings(const DataTypePtr & type)
{
    const auto * map_type = typeid_cast<const DataTypeMap *>(type.get());
    return map_type && isString(removeLowCardinality(map_type->getKeyType())) && isString(removeLowCardinality(map_type->getValueType()));
}

void writeMark(const MarkInCompressedFile & mark, WriteBuffer & out)
{
    writeVarUInt(mark.offset_in_compressed_file, out);
    writeVarUInt(mark.offset_in_decompressed_block, out);
}

MarkInCompressedFile readMark(ReadBuffer & in)
{
    MarkInCompressedFile mark{};
    readVarUInt(mark.offset_in_compressed_file, in);
    readVarUInt(mark.offset_in_decompressed_block, in);
    return mark;
}

MarkInCompressedFile startCompressedBlockIfNeeded(MergeTreeIndexWriterStream & stream, size_t compress_block_size)
{
    if (stream.compressed_hashing.offset() >= compress_block_size)
        stream.compressed_hashing.next();
    return stream.getCurrentMark();
}


/// The rows of a part collected for the index.
struct TimeSeriesIndexBuildState
{
    using StringMap = HashMapWithSavedHash<std::string_view, UInt32>;

    struct Label
    {
        String name;
        StringMap symbol_by_value;
        /// The values in the order of their first occurrence. The position of a value is its symbol during the build.
        PODArray<std::string_view> values;
        UInt64 num_rows = 0;
    };

    explicit TimeSeriesIndexBuildState(size_t id_width_)
        : id_width(id_width_)
    {
    }

    UInt32 getOrAddLabel(std::string_view name)
    {
        StringMap::LookupResult it = nullptr;
        bool inserted = false;
        label_by_name.emplace(ArenaKeyHolder{name, arena}, it, inserted);
        if (inserted)
        {
            it->getMapped() = static_cast<UInt32>(labels.size());
            labels.push_back(std::make_unique<Label>());
            labels.back()->name = String{name};
        }
        return it->getMapped();
    }

    UInt32 getOrAddSymbol(Label & label, std::string_view value)
    {
        StringMap::LookupResult it = nullptr;
        bool inserted = false;
        label.symbol_by_value.emplace(ArenaKeyHolder{value, arena}, it, inserted);
        if (inserted)
        {
            it->getMapped() = static_cast<UInt32>(label.values.size());
            label.values.push_back(it->getKey());
        }
        return it->getMapped();
    }

    UInt32 getOrAddLabelSet(const UInt32 * label_numbers, size_t count)
    {
        std::string_view key{reinterpret_cast<const char *>(label_numbers), count * sizeof(UInt32)};
        StringMap::LookupResult it = nullptr;
        bool inserted = false;
        label_set_by_key.emplace(ArenaKeyHolder{key, arena}, it, inserted);
        if (inserted)
        {
            it->getMapped() = static_cast<UInt32>(label_sets.size());
            label_sets.emplace_back(label_numbers, label_numbers + count);
        }
        return it->getMapped();
    }

    /// The identifiers of all rows one after another, `id_width` bytes each.
    size_t id_width;
    PODArray<char> id_bytes;
    Arena arena;

    VectorWithMemoryTracking<std::unique_ptr<Label>> labels;
    StringMap label_by_name;

    /// A label set is the sorted list of the labels of a row, as their numbers during the build.
    VectorWithMemoryTracking<VectorWithMemoryTracking<UInt32>> label_sets;
    StringMap label_set_by_key;

    PODArray<UInt32> row_label_set;
    /// The symbols of the values of all rows one after another, in the order of the label set of each row.
    PODArray<UInt32> row_symbols;
    UInt64 num_rows = 0;
};


void writeTimeSeriesIndex(const TimeSeriesIndexBuildState & state, MergeTreeIndexOutputStreams & streams)
{
    auto & header_stream = *streams.at(SubstreamType::Regular);
    auto & dictionary_stream = *streams.at(SubstreamType::TimeSeriesIndexDictionary);
    auto & postings_stream = *streams.at(SubstreamType::TimeSeriesIndexPostings);
    auto & series_stream = *streams.at(SubstreamType::TimeSeriesIndexSeries);

    const size_t num_rows = state.num_rows;
    const size_t num_labels = state.labels.size();
    const size_t num_label_sets = state.label_sets.size();

    if (num_rows > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::TOO_MANY_ROWS, "An index of type `time_series` cannot be built for a part with {} rows", num_rows);

    /// The labels are numbered in the order of their names.
    VectorWithMemoryTracking<UInt32> label_order(num_labels);
    std::iota(label_order.begin(), label_order.end(), 0);
    std::sort(label_order.begin(), label_order.end(), [&](UInt32 left, UInt32 right) { return state.labels[left]->name < state.labels[right]->name; });

    VectorWithMemoryTracking<UInt32> label_number(num_labels);
    for (size_t i = 0; i != num_labels; ++i)
        label_number[label_order[i]] = static_cast<UInt32>(i);

    struct LabelInfo
    {
        UInt64 num_blocks = 0;
        bool in_all_rows = false;
        UInt64 presence_offset = 0;
        UInt64 presence_size = 0;
        MarkInCompressedFile sparse_index_mark{};
    };
    VectorWithMemoryTracking<LabelInfo> label_infos(num_labels);

    /// The number of a value in the sorted dictionary of its label, by the symbol of the value during the build.
    VectorWithMemoryTracking<PODArray<UInt32>> value_numbers(num_labels);

    PODArray<char> posting_buffer;
    auto write_posting = [&](const UInt32 * rows, size_t count) -> std::pair<UInt64, UInt64>
    {
        roaring::Roaring bitmap(count, rows);
        bitmap.runOptimize();
        size_t size = bitmap.getSizeInBytes();
        posting_buffer.resize(size);
        bitmap.write(posting_buffer.data());
        UInt64 offset = postings_stream.plain_hashing.count();
        postings_stream.plain_hashing.write(posting_buffer.data(), size);
        return {offset, size};
    };

    VectorWithMemoryTracking<Int32> position_in_label_set(num_label_sets);
    auto & dictionary_out = dictionary_stream.compressed_hashing;

    for (size_t number = 0; number != num_labels; ++number)
    {
        const UInt32 build_number = label_order[number];
        const auto & label = *state.labels[build_number];
        auto & info = label_infos[number];
        const size_t num_values = label.values.size();

        PODArray<UInt32> order(num_values);
        std::iota(order.begin(), order.end(), 0);
        std::sort(order.begin(), order.end(), [&](UInt32 left, UInt32 right) { return label.values[left] < label.values[right]; });

        auto & numbers = value_numbers[build_number];
        numbers.resize(num_values);
        for (size_t i = 0; i != num_values; ++i)
            numbers[order[i]] = static_cast<UInt32>(i);

        for (size_t set = 0; set != num_label_sets; ++set)
        {
            const auto & label_set = state.label_sets[set];
            auto it = std::lower_bound(label_set.begin(), label_set.end(), build_number);
            position_in_label_set[set] = (it != label_set.end() && *it == build_number) ? static_cast<Int32>(it - label_set.begin()) : -1;
        }

        /// A counting sort of the rows with the label by the number of the value: the posting lists one after another.
        PODArray<UInt32> posting_starts(num_values + 1, 0);
        PODArray<UInt32> rows_with_label;
        rows_with_label.reserve(label.num_rows);
        PODArray<UInt32> value_of_row;
        value_of_row.reserve(label.num_rows);
        {
            size_t offset = 0;
            for (size_t row = 0; row != num_rows; ++row)
            {
                const UInt32 set = state.row_label_set[row];
                const Int32 position = position_in_label_set[set];
                if (position >= 0)
                {
                    const UInt32 value_number = numbers[state.row_symbols[offset + position]];
                    ++posting_starts[value_number + 1];
                    rows_with_label.push_back(static_cast<UInt32>(row));
                    value_of_row.push_back(value_number);
                }
                offset += state.label_sets[set].size();
            }
        }
        for (size_t i = 0; i != num_values; ++i)
            posting_starts[i + 1] += posting_starts[i];

        PODArray<UInt32> posting_rows(rows_with_label.size());
        {
            PODArray<UInt32> cursors(posting_starts.begin(), posting_starts.begin() + num_values);
            for (size_t i = 0; i != rows_with_label.size(); ++i)
                posting_rows[cursors[value_of_row[i]]++] = rows_with_label[i];
        }

        info.in_all_rows = (rows_with_label.size() == num_rows);
        if (!info.in_all_rows)
            std::tie(info.presence_offset, info.presence_size) = write_posting(rows_with_label.data(), rows_with_label.size());

        VectorWithMemoryTracking<std::pair<std::string_view, MarkInCompressedFile>> sparse_index;
        for (size_t i = 0; i != num_values; ++i)
        {
            const std::string_view value = label.values[order[i]];
            if (i % DICTIONARY_BLOCK_SIZE == 0)
                sparse_index.emplace_back(value, startCompressedBlockIfNeeded(dictionary_stream, DICTIONARY_COMPRESS_BLOCK_SIZE));

            writeStringBinary(value, dictionary_out);

            const UInt32 * rows = posting_rows.data() + posting_starts[i];
            const size_t cardinality = posting_starts[i + 1] - posting_starts[i];
            writeVarUInt(cardinality, dictionary_out);
            if (cardinality <= MAX_INLINE_POSTING_SIZE)
            {
                UInt32 previous = 0;
                for (size_t j = 0; j != cardinality; ++j)
                {
                    writeVarUInt(rows[j] - previous, dictionary_out);
                    previous = rows[j];
                }
            }
            else
            {
                auto [offset, size] = write_posting(rows, cardinality);
                writeVarUInt(offset, dictionary_out);
                writeVarUInt(size, dictionary_out);
            }
        }

        info.num_blocks = sparse_index.size();
        info.sparse_index_mark = startCompressedBlockIfNeeded(dictionary_stream, DICTIONARY_COMPRESS_BLOCK_SIZE);
        for (const auto & [first_value, mark] : sparse_index)
        {
            writeStringBinary(first_value, dictionary_out);
            writeMark(mark, dictionary_out);
        }
    }

    /// The labels of each label set in the order of their names.
    VectorWithMemoryTracking<VectorWithMemoryTracking<UInt32>> positions_by_name(num_label_sets);
    for (size_t set = 0; set != num_label_sets; ++set)
    {
        const auto & label_set = state.label_sets[set];
        auto & positions = positions_by_name[set];
        positions.resize(label_set.size());
        std::iota(positions.begin(), positions.end(), 0);
        std::sort(positions.begin(), positions.end(), [&](UInt32 left, UInt32 right) { return label_number[label_set[left]] < label_number[label_set[right]]; });
    }

    /// A block of series: the identifiers of its rows, the end of the record of each row, and the records.
    /// A record is the number of the label set and the numbers of the values of its labels.
    VectorWithMemoryTracking<MarkInCompressedFile> series_block_marks;
    auto & series_out = series_stream.compressed_hashing;
    {
        size_t offset = 0;
        PODArray<char> records;
        PODArray<UInt32> record_ends;
        for (size_t block_start = 0; block_start < num_rows; block_start += SERIES_BLOCK_SIZE)
        {
            const size_t block_end = std::min(num_rows, block_start + SERIES_BLOCK_SIZE);
            series_block_marks.push_back(startCompressedBlockIfNeeded(series_stream, SERIES_COMPRESS_BLOCK_SIZE));

            series_out.write(state.id_bytes.data() + block_start * state.id_width, (block_end - block_start) * state.id_width);

            records.clear();
            record_ends.clear();
            {
                WriteBufferFromVector<PODArray<char>> records_out(records);
                for (size_t row = block_start; row != block_end; ++row)
                {
                    const UInt32 set = state.row_label_set[row];
                    const auto & label_set = state.label_sets[set];
                    writeVarUInt(set, records_out);
                    for (UInt32 position : positions_by_name[set])
                        writeVarUInt(value_numbers[label_set[position]][state.row_symbols[offset + position]], records_out);
                    offset += label_set.size();
                    record_ends.push_back(static_cast<UInt32>(records_out.count()));
                }
                records_out.finalize();
            }

            for (UInt32 record_end : record_ends)
                writeBinaryLittleEndian(record_end, series_out);
            series_out.write(records.data(), records.size());
        }
    }

    auto & header_out = header_stream.compressed_hashing;
    writeVarUInt(FORMAT_VERSION, header_out);
    writeVarUInt(num_rows, header_out);
    writeVarUInt(state.id_width, header_out);

    writeVarUInt(num_labels, header_out);
    for (size_t number = 0; number != num_labels; ++number)
    {
        const auto & label = *state.labels[label_order[number]];
        const auto & info = label_infos[number];
        writeStringBinary(label.name, header_out);
        writeVarUInt(label.values.size(), header_out);
        writeVarUInt(label.num_rows, header_out);
        writeVarUInt(info.in_all_rows ? 1 : 0, header_out);
        writeVarUInt(info.presence_offset, header_out);
        writeVarUInt(info.presence_size, header_out);
        writeVarUInt(info.num_blocks, header_out);
        writeMark(info.sparse_index_mark, header_out);
    }

    writeVarUInt(num_label_sets, header_out);
    for (size_t set = 0; set != num_label_sets; ++set)
    {
        const auto & label_set = state.label_sets[set];
        writeVarUInt(label_set.size(), header_out);
        for (UInt32 position : positions_by_name[set])
            writeVarUInt(label_number[label_set[position]], header_out);
    }

    writeVarUInt(series_block_marks.size(), header_out);
    for (const auto & mark : series_block_marks)
        writeMark(mark, header_out);
}


struct MergeTreeIndexGranuleTimeSeries final : public IMergeTreeIndexGranule
{
    explicit MergeTreeIndexGranuleTimeSeries(std::shared_ptr<const TimeSeriesIndexBuildState> state_ = nullptr)
        : state(std::move(state_))
    {
    }

    void serializeBinary(WriteBuffer &) const override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "An index of type `time_series` is written to several streams");
    }

    void serializeBinaryWithMultipleStreams(MergeTreeIndexOutputStreams & streams) const override
    {
        if (state && state->num_rows)
            writeTimeSeriesIndex(*state, streams);
    }

    /// The index is not used to filter granules, `MergeTreeIndexTimeSeries::lookup` reads its files.
    void deserializeBinary(ReadBuffer &, MergeTreeIndexVersion) override {}
    void deserializeBinaryWithMultipleStreams(MergeTreeIndexInputStreams &, MergeTreeIndexDeserializationState &) override {}

    [[nodiscard]] bool empty() const override { return !state || !state->num_rows; }
    [[nodiscard]] size_t memoryUsageBytes() const override { return 0; }

    std::shared_ptr<const TimeSeriesIndexBuildState> state;
};


struct MergeTreeIndexAggregatorTimeSeries final : public IMergeTreeIndexAggregator
{
    explicit MergeTreeIndexAggregatorTimeSeries(const MergeTreeIndexTimeSeries & index_)
        : index(index_)
        , state(std::make_shared<TimeSeriesIndexBuildState>(index.id_width))
    {
    }

    [[nodiscard]] bool empty() const override { return !state->num_rows; }

    MergeTreeIndexGranulePtr getGranuleAndReset() override
    {
        auto granule = std::make_shared<MergeTreeIndexGranuleTimeSeries>(std::move(state));
        state = std::make_shared<TimeSeriesIndexBuildState>(index.id_width);
        return granule;
    }

    void update(const Block & block, size_t * pos, size_t limit) override
    {
        if (*pos >= block.rows())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "The provided position is not less than the number of block rows. Position: {}, Block rows: {}.", *pos, block.rows());

        const size_t rows_read = std::min(limit, block.rows() - *pos);
        const auto & column_names = index.index.column_names;

        {
            auto id_column = recursiveRemoveSparse(block.getByName(column_names[0]).column->convertToFullColumnIfConst());
            auto id_serialization = index.id_type->getDefaultSerialization();
            FormatSettings format_settings;
            WriteBufferFromVector<PODArray<char>> ids_out(state->id_bytes, AppendModeTag{});
            for (size_t row = *pos; row != *pos + rows_read; ++row)
                id_serialization->serializeBinary(*id_column, row, ids_out, format_settings);
            ids_out.finalize();

            if (state->id_bytes.size() != (state->num_rows + rows_read) * state->id_width)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Index {} of type `time_series` requires identifiers with a binary representation of a fixed size, type {} does not have it",
                    index.index.name, index.id_type->getName());
        }

        struct Source
        {
            ColumnPtr holder;
            const ColumnString * keys = nullptr;
            const ColumnString * values = nullptr;
            const IColumn::Offsets * offsets = nullptr;
            UInt32 label = 0;
        };
        VectorWithMemoryTracking<Source> sources;
        for (const auto & label_source : index.label_sources)
        {
            auto & source = sources.emplace_back();
            source.holder = recursiveRemoveLowCardinality(
                recursiveRemoveSparse(block.getByName(column_names[label_source.column_position]).column->convertToFullColumnIfConst()));
            if (label_source.is_map)
            {
                const auto & nested = assert_cast<const ColumnMap &>(*source.holder).getNestedColumn();
                const auto & tuple = assert_cast<const ColumnTuple &>(nested.getData());
                source.keys = &assert_cast<const ColumnString &>(tuple.getColumn(0));
                source.values = &assert_cast<const ColumnString &>(tuple.getColumn(1));
                source.offsets = &nested.getOffsets();
            }
            else
            {
                source.values = &assert_cast<const ColumnString &>(*source.holder);
                source.label = state->getOrAddLabel(label_source.label_name);
            }
        }

        VectorWithMemoryTracking<std::pair<UInt32, UInt32>> labels_of_row;
        VectorWithMemoryTracking<UInt32> label_numbers;
        VectorWithMemoryTracking<UInt32> previous_label_numbers;
        UInt32 previous_label_set = 0;
        bool has_previous_label_set = false;

        for (size_t row = *pos; row != *pos + rows_read; ++row)
        {
            labels_of_row.clear();
            for (const auto & source : sources)
            {
                if (source.offsets)
                {
                    const size_t begin = row ? (*source.offsets)[row - 1] : 0;
                    const size_t end = (*source.offsets)[row];
                    for (size_t i = begin; i != end; ++i)
                    {
                        const std::string_view value = source.values->getDataAt(i);
                        if (value.empty())
                            continue;
                        const UInt32 label = state->getOrAddLabel(source.keys->getDataAt(i));
                        labels_of_row.emplace_back(label, state->getOrAddSymbol(*state->labels[label], value));
                    }
                }
                else
                {
                    const std::string_view value = source.values->getDataAt(row);
                    if (!value.empty())
                        labels_of_row.emplace_back(source.label, state->getOrAddSymbol(*state->labels[source.label], value));
                }
            }

            std::stable_sort(labels_of_row.begin(), labels_of_row.end(), [](const auto & left, const auto & right) { return left.first < right.first; });
            labels_of_row.erase(
                std::unique(labels_of_row.begin(), labels_of_row.end(), [](const auto & left, const auto & right) { return left.first == right.first; }),
                labels_of_row.end());

            label_numbers.clear();
            for (const auto & [label, symbol] : labels_of_row)
            {
                label_numbers.push_back(label);
                state->row_symbols.push_back(symbol);
                ++state->labels[label]->num_rows;
            }

            if (!has_previous_label_set || label_numbers != previous_label_numbers)
            {
                previous_label_set = state->getOrAddLabelSet(label_numbers.data(), label_numbers.size());
                previous_label_numbers = label_numbers;
                has_previous_label_set = true;
            }
            state->row_label_set.push_back(previous_label_set);
        }

        state->num_rows += rows_read;
        *pos += rows_read;
    }

    const MergeTreeIndexTimeSeries & index;
    std::shared_ptr<TimeSeriesIndexBuildState> state;
};


class MergeTreeIndexConditionTimeSeries final : public IMergeTreeIndexCondition
{
public:
    [[nodiscard]] bool alwaysUnknownOrTrue() const override { return true; }
    [[nodiscard]] bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr, const UpdatePartialDisjunctionResultFn &) const override { return true; }
    [[nodiscard]] std::string getDescription() const override { return {}; }
};


/// A matcher prepared for a lookup. `base` below is the matcher without its negation: `label = value` or `label =~ regex`.
struct PreparedMatcher
{
    explicit PreparedMatcher(const TimeSeriesIndexMatcher & matcher)
        : label(matcher.label)
    {
        using Kind = TimeSeriesIndexMatcher::Kind;
        negated = (matcher.kind == Kind::NotEqual) || (matcher.kind == Kind::NotRegex);

        if ((matcher.kind == Kind::Equal) || (matcher.kind == Kind::NotEqual))
        {
            values.emplace();
            if (matcher.value.empty())
                base_matches_empty = true;
            else
                values->push_back(matcher.value);
        }
        else if (matcher.alternatives)
        {
            values.emplace();
            for (const auto & term : *matcher.alternatives)
            {
                if (term.empty())
                    base_matches_empty = true;
                else
                    values->push_back(term);
            }
        }
        else if (matcher.value == "^.+$")
        {
            /// Every non-empty value: `base` is the comparison with the empty value, negated.
            values.emplace();
            base_matches_empty = true;
            negated = !negated;
        }
        else if (matcher.value == "^.*$")
        {
            base_matches_all = true;
        }
        else
        {
            regex = std::make_unique<OptimizedRegularExpression>(
                matcher.value, OptimizedRegularExpression::RE_NO_CAPTURE | OptimizedRegularExpression::RE_DOT_NL);
            base_matches_empty = regex->match("", 0);
            prefix = extractFixedPrefixFromRegularExpression(matcher.value, /* requires_perfect_prefix = */ false).prefix;
        }
    }

    [[nodiscard]] bool baseMatches(std::string_view value) const
    {
        if (base_matches_all)
            return true;
        if (value.empty())
            return base_matches_empty;
        if (values)
            return std::find(values->begin(), values->end(), value) != values->end();
        return regex->match(value.data(), value.size());
    }

    /// The matcher selects a subset of the rows having one of a few values, so it gives a small set of rows to start with.
    [[nodiscard]] bool isSelective() const { return values && !negated && !base_matches_empty; }

    String label;
    bool negated = false;
    bool base_matches_empty = false;
    /// `base` is true for every value, including the empty one.
    bool base_matches_all = false;
    /// The non-empty values matched by `base` if it is a list of them.
    std::optional<Strings> values;
    /// Otherwise the regular expression of `base` and the prefix all the values matching it have.
    std::unique_ptr<OptimizedRegularExpression> regex;
    String prefix;
};

using PreparedMatchers = VectorWithMemoryTracking<PreparedMatcher>;


/// Reads the index files of one part.
class TimeSeriesIndexPartReader
{
public:
    static constexpr size_t npos = static_cast<size_t>(-1);

    struct Label
    {
        String name;
        UInt64 num_values = 0;
        UInt64 num_rows = 0;
        bool in_all_rows = false;
        UInt64 presence_offset = 0;
        UInt64 presence_size = 0;
        UInt64 num_blocks = 0;
        MarkInCompressedFile sparse_index_mark{};

        bool sparse_index_loaded = false;
        VectorWithMemoryTracking<String> first_values;
        VectorWithMemoryTracking<MarkInCompressedFile> block_marks;
    };

    struct DictionaryBlock
    {
        struct Posting
        {
            UInt64 cardinality = 0;
            /// For a posting list kept in the dictionary: the position of its first row in `inline_rows`.
            /// Otherwise the position of the posting list in the file with postings.
            UInt64 offset = 0;
            UInt64 size = 0;
        };

        VectorWithMemoryTracking<String> values;
        VectorWithMemoryTracking<Posting> postings;
        VectorWithMemoryTracking<UInt32> inline_rows;
    };

    TimeSeriesIndexPartReader(
        const MergeTreeIndexTimeSeries & index_, const MergeTreeData::DataPartPtr & part_, const MergeTreeReaderSettings & reader_settings_)
        : index(index_)
        , part(part_)
        , part_info(std::make_shared<LoadedMergeTreeDataPartInfoForReader>(part, std::make_shared<AlterConversions>()))
        , reader_settings(reader_settings_)
    {
        readHeader();
    }

    [[nodiscard]] UInt64 getNumRows() const { return num_rows; }
    [[nodiscard]] const VectorWithMemoryTracking<Label> & getLabels() const { return labels; }
    [[nodiscard]] const VectorWithMemoryTracking<VectorWithMemoryTracking<UInt32>> & getLabelSets() const { return label_sets; }

    [[nodiscard]] size_t findLabel(std::string_view name) const
    {
        auto it = std::lower_bound(labels.begin(), labels.end(), name, [](const Label & label, std::string_view name_) { return label.name < name_; });
        return (it != labels.end() && it->name == name) ? static_cast<size_t>(it - labels.begin()) : npos;
    }

    [[nodiscard]] roaring::Roaring getAllRows() const
    {
        roaring::Roaring rows;
        rows.addRange(0, num_rows);
        return rows;
    }

    roaring::Roaring getRowsWithLabel(size_t label_number)
    {
        const auto & label = labels[label_number];
        if (label.in_all_rows)
            return getAllRows();
        return readPosting(label.presence_offset, label.presence_size);
    }

    /// Adds the rows having one of `values` of a label.
    void addRowsWithValues(size_t label_number, const Strings & values, roaring::Roaring & rows)
    {
        auto & label = labels[label_number];
        loadSparseIndex(label);
        VectorWithMemoryTracking<UInt32> inline_rows;
        for (const auto & value : values)
        {
            auto it = std::upper_bound(label.first_values.begin(), label.first_values.end(), value);
            if (it == label.first_values.begin())
                continue;
            const auto & block = getDictionaryBlock(label_number, it - label.first_values.begin() - 1);
            auto value_it = std::lower_bound(block.values.begin(), block.values.end(), value);
            if (value_it != block.values.end() && *value_it == value)
                addPosting(block, value_it - block.values.begin(), rows, inline_rows);
        }
        rows.addMany(inline_rows.size(), inline_rows.data());
    }

    /// Adds the rows having a value of a label for which `predicate` is true. All such values start with `prefix`.
    template <typename Predicate>
    void addRowsWithMatchingValues(size_t label_number, std::string_view prefix, Predicate && predicate, roaring::Roaring & rows)
    {
        auto & label = labels[label_number];
        loadSparseIndex(label);

        size_t block_number = 0;
        if (!prefix.empty())
        {
            auto it = std::upper_bound(label.first_values.begin(), label.first_values.end(), prefix);
            if (it != label.first_values.begin())
                block_number = it - label.first_values.begin() - 1;
        }

        VectorWithMemoryTracking<UInt32> inline_rows;
        DictionaryBlock block;
        for (; block_number != label.num_blocks; ++block_number)
        {
            const std::string_view first_value = label.first_values[block_number];
            if (!prefix.empty() && first_value > prefix && !first_value.starts_with(prefix))
                break;

            readDictionaryBlock(label, block_number, block);
            for (size_t i = 0; i != block.values.size(); ++i)
            {
                const std::string_view value = block.values[i];
                if (!prefix.empty() && !value.starts_with(prefix))
                    continue;
                if (predicate(value))
                    addPosting(block, i, rows, inline_rows);
            }
        }
        rows.addMany(inline_rows.size(), inline_rows.data());
    }

    const String & getValue(size_t label_number, UInt64 value_number)
    {
        const size_t block_number = value_number / DICTIONARY_BLOCK_SIZE;
        const auto & block = getDictionaryBlock(label_number, block_number);
        const size_t position = value_number % DICTIONARY_BLOCK_SIZE;
        if (position >= block.values.size())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Index {} of part {} has no value {} of label {}",
                index.index.name, part->name, value_number, labels[label_number].name);
        return block.values[position];
    }

    /// Reads the rows of `rows` in their order and calls `callback(id_data, label_set, value_numbers)`.
    template <typename Callback>
    void readSeries(const roaring::Roaring & rows, Callback && callback)
    {
        auto & stream = getStream(SubstreamType::TimeSeriesIndexSeries);
        const size_t id_width = index.id_width;
        VectorWithMemoryTracking<UInt64> value_numbers;
        PODArray<char> ids;
        PODArray<UInt32> record_ends;
        PODArray<char> records;
        VectorWithMemoryTracking<UInt32> rows_in_block_to_read;

        auto it = rows.begin();
        const auto end = rows.end();
        while (it != end)
        {
            const size_t block_number = *it / SERIES_BLOCK_SIZE;
            if (block_number >= series_block_marks.size())
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Index {} of part {} has no row {}", index.index.name, part->name, *it);

            const size_t block_start = block_number * SERIES_BLOCK_SIZE;
            const size_t rows_in_block = std::min<size_t>(SERIES_BLOCK_SIZE, num_rows - block_start);

            rows_in_block_to_read.clear();
            for (; it != end && *it < block_start + rows_in_block; ++it)
                rows_in_block_to_read.push_back(static_cast<UInt32>(*it - block_start));

            stream.seekToMark(series_block_marks[block_number]);
            auto & in = *stream.getDataBuffer();

            ids.resize(rows_in_block * id_width);
            in.readStrict(ids.data(), ids.size());

            record_ends.resize(rows_in_block);
            in.readStrict(reinterpret_cast<char *>(record_ends.data()), rows_in_block * sizeof(UInt32));

            const size_t records_size = record_ends[rows_in_block_to_read.back()];
            records.resize(records_size);
            in.readStrict(records.data(), records_size);

            for (UInt32 row : rows_in_block_to_read)
            {
                const size_t record_begin = row ? record_ends[row - 1] : 0;
                const size_t record_end = record_ends[row];
                if (record_begin > record_end || record_end > records_size)
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "Index {} of part {} has a broken record of row {}",
                        index.index.name, part->name, block_start + row);

                ReadBufferFromMemory record_in(records.data() + record_begin, record_end - record_begin);
                UInt64 label_set = 0;
                readVarUInt(label_set, record_in);
                if (label_set >= label_sets.size())
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "Index {} of part {} has no label set {}", index.index.name, part->name, label_set);

                value_numbers.resize(label_sets[label_set].size());
                for (auto & value_number : value_numbers)
                    readVarUInt(value_number, record_in);

                callback(ids.data() + row * id_width, label_set, value_numbers);
            }
        }
    }

private:
    void readHeader()
    {
        auto & stream = getStream(SubstreamType::Regular);
        stream.seekToStart();
        auto & in = *stream.getDataBuffer();

        UInt64 version = 0;
        readVarUInt(version, in);
        if (version != FORMAT_VERSION)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Index {} of part {} has unknown format version {}", index.index.name, part->name, version);

        readVarUInt(num_rows, in);

        UInt64 stored_id_width = 0;
        readVarUInt(stored_id_width, in);
        if (stored_id_width != index.id_width)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Index {} of part {} has identifiers of {} bytes, expected {} bytes",
                index.index.name, part->name, stored_id_width, index.id_width);

        UInt64 num_labels = 0;
        readVarUInt(num_labels, in);
        labels.resize(num_labels);
        for (auto & label : labels)
        {
            readStringBinary(label.name, in);
            readVarUInt(label.num_values, in);
            readVarUInt(label.num_rows, in);
            UInt64 in_all_rows = 0;
            readVarUInt(in_all_rows, in);
            label.in_all_rows = (in_all_rows != 0);
            readVarUInt(label.presence_offset, in);
            readVarUInt(label.presence_size, in);
            readVarUInt(label.num_blocks, in);
            label.sparse_index_mark = readMark(in);
        }

        UInt64 num_label_sets = 0;
        readVarUInt(num_label_sets, in);
        label_sets.resize(num_label_sets);
        for (auto & label_set : label_sets)
        {
            UInt64 size = 0;
            readVarUInt(size, in);
            label_set.resize(size);
            for (auto & label_number : label_set)
            {
                UInt64 number = 0;
                readVarUInt(number, in);
                if (number >= num_labels)
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "Index {} of part {} has no label {}", index.index.name, part->name, number);
                label_number = static_cast<UInt32>(number);
            }
        }

        UInt64 num_series_blocks = 0;
        readVarUInt(num_series_blocks, in);
        series_block_marks.resize(num_series_blocks);
        for (auto & mark : series_block_marks)
            mark = readMark(in);
    }

    MergeTreeIndexReaderStream & getStream(SubstreamType type)
    {
        auto it = streams.find(type);
        if (it != streams.end())
            return *it->second;

        const auto & substreams = getTimeSeriesIndexSubstreams();
        const auto & substream = *std::ranges::find_if(substreams, [type](const auto & candidate) { return candidate.type == type; });
        const String stream_name = index.getFileName() + substream.suffix;

        auto data_part_storage = part_info->getDataPartStorage();
        auto actual_stream_name = IMergeTreeDataPart::getStreamNameOrHash(stream_name, substream.extension, part_info->getChecksums());
        if (!actual_stream_name)
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Part {} has no file {} of index {}",
                part->name, stream_name + substream.extension, index.index.name);

        auto settings = reader_settings;
        settings.is_compressed = MergeTreeIndexSubstream::isCompressed(type);
        if (type != SubstreamType::Regular)
        {
            /// The files are read at a few positions, so a large buffer reads much more than the lookup uses.
            constexpr size_t max_buffer_size = 128 * 1024;
            auto & read_settings = settings.read_settings;
            read_settings.local_fs_settings.buffer_size = std::min(read_settings.local_fs_settings.buffer_size, max_buffer_size);
            read_settings.remote_fs_settings.buffer_size = std::min(read_settings.remote_fs_settings.buffer_size, max_buffer_size);
        }

        static constexpr size_t marks_count = 1;
        auto stream = std::make_unique<MergeTreeReaderStreamSingleColumnWholePart>(
            data_part_storage,
            *actual_stream_name,
            substream.extension,
            marks_count,
            MarkRanges{{0, marks_count}},
            settings,
            /* uncompressed_cache = */ nullptr,
            part_info->getFileSizeOrZero(*actual_stream_name + substream.extension),
            /* marks_loader = */ nullptr,
            ReadBufferFromFileBase::ProfileCallback{},
            CLOCK_MONOTONIC_COARSE);

        return *streams.emplace(type, std::move(stream)).first->second;
    }

    void loadSparseIndex(Label & label)
    {
        if (label.sparse_index_loaded)
            return;

        auto & stream = getStream(SubstreamType::TimeSeriesIndexDictionary);
        stream.seekToMark(label.sparse_index_mark);
        auto & in = *stream.getDataBuffer();

        label.first_values.resize(label.num_blocks);
        label.block_marks.resize(label.num_blocks);
        for (size_t i = 0; i != label.num_blocks; ++i)
        {
            readStringBinary(label.first_values[i], in);
            label.block_marks[i] = readMark(in);
        }
        label.sparse_index_loaded = true;
    }

    void readDictionaryBlock(Label & label, size_t block_number, DictionaryBlock & block)
    {
        loadSparseIndex(label);
        if (block_number >= label.num_blocks)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Index {} of part {} has no block {} of label {}",
                index.index.name, part->name, block_number, label.name);

        auto & stream = getStream(SubstreamType::TimeSeriesIndexDictionary);
        stream.seekToMark(label.block_marks[block_number]);
        auto & in = *stream.getDataBuffer();

        const size_t num_values = std::min<size_t>(DICTIONARY_BLOCK_SIZE, label.num_values - block_number * DICTIONARY_BLOCK_SIZE);
        block.values.resize(num_values);
        block.postings.resize(num_values);
        block.inline_rows.clear();
        for (size_t i = 0; i != num_values; ++i)
        {
            readStringBinary(block.values[i], in);
            auto & posting = block.postings[i];
            readVarUInt(posting.cardinality, in);
            if (posting.cardinality <= MAX_INLINE_POSTING_SIZE)
            {
                posting.offset = block.inline_rows.size();
                posting.size = 0;
                UInt32 previous = 0;
                for (size_t j = 0; j != posting.cardinality; ++j)
                {
                    UInt64 delta = 0;
                    readVarUInt(delta, in);
                    previous += static_cast<UInt32>(delta);
                    block.inline_rows.push_back(previous);
                }
            }
            else
            {
                readVarUInt(posting.offset, in);
                readVarUInt(posting.size, in);
            }
        }
    }

    const DictionaryBlock & getDictionaryBlock(size_t label_number, size_t block_number)
    {
        if (last_blocks.empty())
            last_blocks.resize(labels.size(), {npos, nullptr});

        auto & last_block = last_blocks[label_number];
        if (last_block.first == block_number)
            return *last_block.second;

        const UInt64 key = (static_cast<UInt64>(label_number) << 32) | block_number;
        auto it = dictionary_blocks.find(key);
        if (it == dictionary_blocks.end())
        {
            auto block = std::make_unique<DictionaryBlock>();
            readDictionaryBlock(labels[label_number], block_number, *block);
            it = dictionary_blocks.emplace(key, std::move(block)).first;
        }

        last_block = {block_number, it->second.get()};
        return *it->second;
    }

    void addPosting(const DictionaryBlock & block, size_t position, roaring::Roaring & rows, VectorWithMemoryTracking<UInt32> & inline_rows)
    {
        const auto & posting = block.postings[position];
        if (posting.cardinality <= MAX_INLINE_POSTING_SIZE)
        {
            const auto * begin = block.inline_rows.data() + posting.offset;
            inline_rows.insert(inline_rows.end(), begin, begin + posting.cardinality);
        }
        else
        {
            rows |= readPosting(posting.offset, posting.size);
        }
    }

    roaring::Roaring readPosting(UInt64 offset, UInt64 size)
    {
        auto & stream = getStream(SubstreamType::TimeSeriesIndexPostings);
        stream.seekToMark(MarkInCompressedFile{offset, 0});
        posting_buffer.resize(size);
        stream.getDataBuffer()->readStrict(posting_buffer.data(), size);
        return roaring::Roaring::readSafe(posting_buffer.data(), size);
    }

    const MergeTreeIndexTimeSeries & index;
    MergeTreeData::DataPartPtr part;
    MergeTreeDataPartInfoForReaderPtr part_info;
    MergeTreeReaderSettings reader_settings;

    UInt64 num_rows = 0;
    VectorWithMemoryTracking<Label> labels;
    VectorWithMemoryTracking<VectorWithMemoryTracking<UInt32>> label_sets;
    VectorWithMemoryTracking<MarkInCompressedFile> series_block_marks;

    std::map<SubstreamType, std::unique_ptr<MergeTreeIndexReaderStream>> streams; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::unordered_map<UInt64, std::unique_ptr<DictionaryBlock>> dictionary_blocks; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    VectorWithMemoryTracking<std::pair<size_t, const DictionaryBlock *>> last_blocks;
    PODArray<char> posting_buffer;
};


/// Finds the rows of a part matching all the matchers. Returns the matchers left to check on the labels of each row.
roaring::Roaring findRows(
    TimeSeriesIndexPartReader & reader, const PreparedMatchers & matchers, VectorWithMemoryTracking<const PreparedMatcher *> & matchers_to_check)
{
    static constexpr size_t npos = TimeSeriesIndexPartReader::npos;
    const auto & labels = reader.getLabels();

    std::optional<roaring::Roaring> rows;

    /// Applies a matcher, given the rows having a non-empty value matched by its `base`.
    auto apply = [&](const PreparedMatcher & matcher, size_t label_number, roaring::Roaring && rows_with_values)
    {
        if (!rows)
            rows = reader.getAllRows();

        if (!matcher.base_matches_empty)
        {
            if (matcher.negated)
                *rows -= rows_with_values;
            else
                *rows &= rows_with_values;
            return;
        }

        roaring::Roaring rows_with_label;
        if (label_number != npos)
            rows_with_label = reader.getRowsWithLabel(label_number);

        if (matcher.negated)
        {
            *rows &= rows_with_label;
            *rows -= rows_with_values;
        }
        else
        {
            *rows = (*rows & rows_with_values) | (*rows - rows_with_label);
        }
    };

    auto get_rows_with_values = [&](const PreparedMatcher & matcher, size_t label_number)
    {
        roaring::Roaring rows_with_values;
        if (label_number != npos)
            reader.addRowsWithValues(label_number, *matcher.values, rows_with_values);
        return rows_with_values;
    };

    /// The matchers selecting a few values first, starting with the smallest set of rows.
    {
        VectorWithMemoryTracking<roaring::Roaring> selected;
        for (const auto & matcher : matchers)
        {
            if (matcher.isSelective())
            {
                selected.push_back(get_rows_with_values(matcher, reader.findLabel(matcher.label)));
                if (selected.back().isEmpty())
                    return {};
            }
        }

        std::sort(selected.begin(), selected.end(), [](const auto & left, const auto & right) { return left.cardinality() < right.cardinality(); });
        for (auto & selected_rows : selected)
        {
            if (!rows)
                rows = std::move(selected_rows);
            else
                *rows &= selected_rows;
            if (rows->isEmpty())
                return {};
        }
    }

    for (const auto & matcher : matchers)
    {
        if (matcher.values && !matcher.isSelective())
        {
            size_t label_number = reader.findLabel(matcher.label);
            apply(matcher, label_number, get_rows_with_values(matcher, label_number));
            if (rows->isEmpty())
                return {};
        }
    }

    for (const auto & matcher : matchers)
    {
        if (matcher.values)
            continue;

        if (matcher.base_matches_all)
        {
            if (matcher.negated)
                return {};
            continue;
        }

        size_t label_number = reader.findLabel(matcher.label);
        if (label_number == npos)
        {
            apply(matcher, label_number, {});
        }
        else if (rows && rows->cardinality() * 8 < labels[label_number].num_values)
        {
            /// Fewer rows than values: it is cheaper to check the values of these rows than all the values of the label.
            matchers_to_check.push_back(&matcher);
        }
        else
        {
            roaring::Roaring rows_with_values;
            reader.addRowsWithMatchingValues(
                label_number, matcher.prefix, [&](std::string_view value) { return matcher.regex->match(value.data(), value.size()); }, rows_with_values);
            apply(matcher, label_number, std::move(rows_with_values));
        }

        if (rows && rows->isEmpty())
            return {};
    }

    if (!rows)
        rows = reader.getAllRows();
    return std::move(*rows);
}


void lookupInPart(
    const MergeTreeIndexTimeSeries & index,
    const MergeTreeData::DataPartPtr & part,
    const PreparedMatchers & matchers,
    const MergeTreeReaderSettings & reader_settings,
    HashSet<UInt128, UInt128TrivialHash> * seen_ids,
    TimeSeriesIndexLookupResult & result)
{
    static constexpr size_t npos = TimeSeriesIndexPartReader::npos;

    TimeSeriesIndexPartReader reader(index, part, reader_settings);
    if (reader.getNumRows() != part->rows_count)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Index {} of part {} has {} rows, and the part has {} rows",
            index.index.name, part->name, reader.getNumRows(), part->rows_count);

    VectorWithMemoryTracking<const PreparedMatcher *> matchers_to_check;
    auto rows = findRows(reader, matchers, matchers_to_check);
    if (rows.isEmpty())
        return;

    const auto & labels = reader.getLabels();
    const auto & label_sets = reader.getLabelSets();

    struct MatcherToCheck
    {
        const PreparedMatcher * matcher = nullptr;
        size_t label_number = npos;
        /// The position of the label in each label set, or -1.
        VectorWithMemoryTracking<Int32> positions;
        std::unordered_map<UInt64, bool> result_by_value; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    };
    VectorWithMemoryTracking<MatcherToCheck> checks;
    for (const auto * matcher : matchers_to_check)
    {
        auto & check = checks.emplace_back();
        check.matcher = matcher;
        check.label_number = reader.findLabel(matcher->label);
        check.positions.reserve(label_sets.size());
        for (const auto & label_set : label_sets)
        {
            auto it = std::lower_bound(label_set.begin(), label_set.end(), check.label_number);
            check.positions.push_back((it != label_set.end() && *it == check.label_number) ? static_cast<Int32>(it - label_set.begin()) : -1);
        }
    }

    const size_t id_width = index.id_width;
    auto id_serialization = index.id_type->getDefaultSerialization();
    FormatSettings format_settings;

    reader.readSeries(rows, [&](const char * id_data, UInt64 label_set_number, const VectorWithMemoryTracking<UInt64> & value_numbers)
    {
        for (auto & check : checks)
        {
            const Int32 position_in_set = check.positions[label_set_number];
            bool base_matches = false;
            if (position_in_set < 0)
            {
                base_matches = check.matcher->base_matches_empty;
            }
            else
            {
                const UInt64 value_number = value_numbers[position_in_set];
                auto [it, inserted] = check.result_by_value.try_emplace(value_number, false);
                if (inserted)
                    it->second = check.matcher->baseMatches(reader.getValue(check.label_number, value_number));
                base_matches = it->second;
            }
            if (base_matches == check.matcher->negated)
                return;
        }

        if (seen_ids)
        {
            HashSet<UInt128, UInt128TrivialHash>::LookupResult it = nullptr;
            bool inserted = false;
            seen_ids->emplace(sipHash128(id_data, id_width), it, inserted);
            if (!inserted)
                return;
        }

        const auto & label_set = label_sets[label_set_number];
        auto labels_of_series = std::make_shared<TimeSeriesIndexLookupResult::Labels>();
        labels_of_series->reserve(label_set.size());
        for (size_t i = 0; i != label_set.size(); ++i)
            labels_of_series->emplace_back(labels[label_set[i]].name, reader.getValue(label_set[i], value_numbers[i]));

        ReadBufferFromMemory id_in(id_data, id_width);
        id_serialization->deserializeBinary(*result.ids, id_in, format_settings);
        result.labels.push_back(std::move(labels_of_series));
    });
}

}


MergeTreeIndexTimeSeries::MergeTreeIndexTimeSeries(StorageMetadataPtr metadata_snapshot_, const IndexDescription & index_)
    : IMergeTreeIndex(std::move(metadata_snapshot_), index_)
    , id_type(index_.data_types.at(0))
    , label_sources(parseLabelSources(index_))
{
    auto default_id = id_type->createColumn();
    default_id->insertDefault();
    WriteBufferFromOwnString out;
    id_type->getDefaultSerialization()->serializeBinary(*default_id, 0, out, FormatSettings{});
    id_width = out.count();
}

MergeTreeIndexTimeSeries::LabelSources MergeTreeIndexTimeSeries::parseLabelSources(const IndexDescription & description)
{
    std::unordered_map<String, String> label_name_by_column; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    if (description.arguments)
    {
        for (const auto & argument : description.arguments->children)
        {
            const auto * function = argument->as<ASTFunction>();
            const auto * column = (function && function->name == "equals" && function->arguments->children.size() == 2)
                ? function->arguments->children[0]->as<ASTIdentifier>() : nullptr;
            const auto * label_name = column ? function->arguments->children[1]->as<ASTLiteral>() : nullptr;
            if (!label_name || label_name->value.getType() != Field::Types::String || label_name->value.safeGet<String>().empty())
                throw Exception(ErrorCodes::INCORRECT_QUERY,
                    "An argument of index {} of type `time_series` must be `column = 'label name'`, got {}",
                    description.name, argument->formatForErrorMessage());
            if (!label_name_by_column.emplace(column->name(), label_name->value.safeGet<String>()).second)
                throw Exception(ErrorCodes::INCORRECT_QUERY, "Index {} of type `time_series` names column {} twice", description.name, column->name());
        }
    }

    if (description.column_names.size() < 2)
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Index {} of type `time_series` must have a column with the identifiers of time series and columns with their labels", description.name);

    if (!hasBinaryRepresentationOfFixedSize(description.data_types[0]))
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Column {} of index {} of type `time_series` must have a type with a binary representation of a fixed size, got {}",
            description.column_names[0], description.name, description.data_types[0]->getName());

    LabelSources sources;
    for (size_t i = 1; i != description.column_names.size(); ++i)
    {
        const auto & column_name = description.column_names[i];
        const auto type = removeLowCardinality(description.data_types[i]);
        auto & source = sources.emplace_back();
        source.column_position = i;

        if (typeid_cast<const DataTypeMap *>(type.get()))
        {
            if (!isMapOfStrings(type))
                throw Exception(ErrorCodes::INCORRECT_QUERY,
                    "Column {} of index {} of type `time_series` must have type `Map(String, String)`, got {}",
                    column_name, description.name, description.data_types[i]->getName());
            if (label_name_by_column.contains(column_name))
                throw Exception(ErrorCodes::INCORRECT_QUERY,
                    "Index {} of type `time_series` cannot name a label for column {} of type `Map`", description.name, column_name);
            source.is_map = true;
        }
        else if (isString(type))
        {
            auto it = label_name_by_column.find(column_name);
            source.label_name = (it != label_name_by_column.end()) ? it->second : column_name;
            if (it != label_name_by_column.end())
                label_name_by_column.erase(it);
        }
        else
        {
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                "Column {} of index {} of type `time_series` must have type `String` or `Map(String, String)`, got {}",
                column_name, description.name, description.data_types[i]->getName());
        }
    }

    if (!label_name_by_column.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Index {} of type `time_series` names a label for column {} which is not a column of the index",
            description.name, label_name_by_column.begin()->first);

    return sources;
}

bool MergeTreeIndexTimeSeries::supportsColumnTypes(const DataTypePtr & id_type, const DataTypes & label_types)
{
    return hasBinaryRepresentationOfFixedSize(id_type)
        && std::ranges::all_of(label_types, [](const DataTypePtr & type) { return isMapOfStrings(type) || isString(removeLowCardinality(type)); });
}

MergeTreeIndexSubstreams MergeTreeIndexTimeSeries::getSubstreams() const
{
    return getTimeSeriesIndexSubstreams();
}

MergeTreeIndexFormat MergeTreeIndexTimeSeries::getPhysicalFormat(
    const MergeTreeDataPartChecksums & checksums, const IDataPartStorage & storage, const std::string & relative_path_prefix) const
{
    if (!indexFileExistsInChecksums(checksums, relative_path_prefix, ".idx", &storage))
        return {0, {}};
    return {1, getTimeSeriesIndexSubstreams()};
}

MergeTreeIndexGranulePtr MergeTreeIndexTimeSeries::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleTimeSeries>();
}

MergeTreeIndexAggregatorPtr MergeTreeIndexTimeSeries::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorTimeSeries>(*this);
}

MergeTreeIndexConditionPtr MergeTreeIndexTimeSeries::createIndexCondition(const ActionsDAG::Node *, ContextPtr) const
{
    return std::make_shared<MergeTreeIndexConditionTimeSeries>();
}

TimeSeriesIndexLookupResult MergeTreeIndexTimeSeries::lookup(
    const MergeTreeData::DataPartsVector & parts, const TimeSeriesIndexMatchers & matchers, const ContextPtr & context) const
{
    Stopwatch watch;

    TimeSeriesIndexLookupResult result;
    result.ids = id_type->createColumn();

    PreparedMatchers prepared_matchers;
    prepared_matchers.reserve(matchers.size());
    for (const auto & matcher : matchers)
        prepared_matchers.emplace_back(matcher);

    const auto reader_settings = MergeTreeReaderSettings::createFromContext(context);

    size_t num_parts = 0;
    for (const auto & part : parts)
        num_parts += (part->rows_count != 0);

    /// The same time series can be in several parts until they are merged.
    HashSet<UInt128, UInt128TrivialHash> seen_ids;

    for (const auto & part : parts)
    {
        if (part->rows_count)
            lookupInPart(*this, part, prepared_matchers, reader_settings, (num_parts > 1) ? &seen_ids : nullptr, result);
    }

    ProfileEvents::increment(ProfileEvents::TimeSeriesIndexLookupParts, num_parts);
    ProfileEvents::increment(ProfileEvents::TimeSeriesIndexLookupSeries, result.labels.size());
    ProfileEvents::increment(ProfileEvents::TimeSeriesIndexLookupMicroseconds, watch.elapsedMicroseconds());
    return result;
}


MergeTreeIndexPtr timeSeriesIndexCreator(StorageMetadataPtr metadata_snapshot, const IndexDescription & index, const MergeTreeSettings &)
{
    return std::make_shared<MergeTreeIndexTimeSeries>(std::move(metadata_snapshot), index);
}

void timeSeriesIndexValidator(const IndexDescription & index, bool /* attach */, const MergeTreeSettings &)
{
    MergeTreeIndexTimeSeries::parseLabelSources(index);

    if (index.granularity < MIN_GRANULARITY)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Index {} of type `time_series` is built for a whole part, its GRANULARITY must be at least {}", index.name, MIN_GRANULARITY);
}

}
