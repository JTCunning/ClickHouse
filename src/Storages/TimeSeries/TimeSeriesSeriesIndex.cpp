#include <Storages/TimeSeries/TimeSeriesSeriesIndex.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Arena.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/HashTable/HashMap.h>
#include <Common/MemoryTrackerSwitcher.h>
#include <Common/OptimizedRegularExpression.h>
#include <Common/PODArray.h>
#include <Common/RadixSort.h>
#include <Common/ProfileEvents.h>
#include <Common/SharedMutex.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/setThreadName.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Core/DecimalFunctions.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <Storages/TimeSeries/splitTimeSeriesType.h>
#include <Storages/TimeSeries/tryGetLiteralAlternatives.h>

#include <base/scope_guard.h>
#include <city.h>
#include <roaring/roaring.hh>

#include <chrono>
#include <ctime>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>


namespace ProfileEvents
{
    extern const Event TimeSeriesSeriesIndexAppliedParts;
    extern const Event TimeSeriesSeriesIndexAppliedRows;
    extern const Event TimeSeriesSeriesIndexApplyMicroseconds;
    extern const Event TimeSeriesSeriesIndexRebuilds;
}

namespace CurrentMetrics
{
    extern const Metric TimeSeriesSeriesIndexBytes;
    extern const Metric TimeSeriesSeriesIndexSeries;
    extern const Metric TimeSeriesSeriesIndexThreads;
    extern const Metric TimeSeriesSeriesIndexThreadsActive;
    extern const Metric TimeSeriesSeriesIndexThreadsScheduled;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsUInt64 recent_samples_ttl_seconds;
    extern const TimeSeriesSettingsMap tags_to_columns;
}

namespace
{

using Ordinal = UInt32;
constexpr Ordinal NO_ORDINAL = std::numeric_limits<Ordinal>::max();
constexpr UInt32 NO_NAME = std::numeric_limits<UInt32>::max();
constexpr UInt64 NO_LABELS = std::numeric_limits<UInt64>::max();

constexpr Int64 NO_MIN_TIME = std::numeric_limits<Int64>::max();
constexpr Int64 NO_MAX_TIME = std::numeric_limits<Int64>::min();

/// The time series has a time range inside the horizon, it has no tags yet, and a row with its tags could have been skipped.
constexpr UInt8 FLAG_UNRESOLVED = 1;

/// The background task looks for new data parts with this period,
/// and with the shorter one right after it has applied some: more of them are probably there already.
constexpr size_t POLL_INTERVAL_MS = 500;
constexpr size_t POLL_INTERVAL_AFTER_APPLY_MS = 20;
/// A query applies new data parts itself if they have no more rows than this in total, otherwise it reads the tables.
constexpr size_t MAX_ROWS_TO_APPLY_IN_QUERY = 1'000'000;
/// A query waits this long for the background task which applies data parts at the moment.
constexpr auto MAX_WAIT_IN_QUERY = std::chrono::milliseconds(50);
/// The number of lookups a query makes: it applies new data parts between them.
constexpr size_t MAX_LOOKUPS_IN_QUERY = 2;
/// After a query has waited for the index in vain, the next queries don't wait for the index and don't apply
/// data parts for this time: they use the index only if it has applied everything.
constexpr Int64 TIME_WITHOUT_WAIT_AFTER_MISS_MS = 1000;
/// A matcher with a regular expression selects the time series from the lists of the values it accepts. To unite many lists
/// is slow, so with fewer candidates per list than this the candidates are checked instead.
constexpr size_t MAX_CANDIDATES_PER_LIST_TO_CHECK = 128;
/// The background task reads data parts with this number of threads. A query reads them with one thread.
constexpr size_t NUM_THREADS_TO_READ = 8;
/// The tags of the time series found by a lookup are made by a few threads if there are many time series.
/// The lookups of one table share this number of threads.
constexpr size_t MAX_THREADS_TO_MAKE_TAGS = 16;
constexpr size_t MAX_THREADS_TO_MAKE_TAGS_IN_LOOKUP = 8;
constexpr size_t MIN_SERIES_PER_THREAD_TO_MAKE_TAGS = 4096;
/// One query to a target table reads no more rows than this, if it reads more than one data part.
constexpr size_t MAX_ROWS_IN_BATCH = 20'000'000;
/// The index is rebuilt if a time series stays without tags this long while they could have been skipped.
constexpr auto MAX_UNRESOLVED_TIME = std::chrono::seconds(60);
/// The minimum time between two rebuilds caused by a change of the tables.
constexpr auto MIN_TIME_BETWEEN_REBUILDS = std::chrono::seconds(10);
/// The index is built again this long after it reached its size limit.
constexpr auto TIME_TO_RETRY_AFTER_SIZE_LIMIT = std::chrono::seconds(3600);
/// The index frees its memory if no query used it this long.
constexpr auto MAX_IDLE_TIME = std::chrono::seconds(3600);
/// The size of the lists of time series is calculated by a pass over all of them, which is slow. Between two passes
/// the size is estimated: a time series added to a list takes about this number of bytes.
constexpr size_t ESTIMATED_BYTES_PER_POSTING_ADD = 4;
/// A list of time series with more than one of them is a bitmap, which keeps its data in several allocated pieces.
constexpr size_t ESTIMATED_OVERHEAD_OF_BITMAP = 96;
/// The size of the lists of time series is calculated again after this time, or earlier if the index has grown by 1/8.
constexpr auto POSTINGS_BYTES_CALCULATION_INTERVAL = std::chrono::seconds(600);


/// The identifiers are kept as their native binary representation. The supported types are the ones with a typed map
/// in `ContextTimeSeriesTagsCollector`: a fixed-size value of 8 or 16 bytes, or a tuple of UInt64 and such a value.
struct IDLayout
{
    size_t first_size = 0;
    size_t second_size = 0;
    [[nodiscard]] size_t size() const { return first_size + second_size; }
};

size_t getIDComponentSize(const IDataType & type)
{
    WhichDataType which{type};
    if (which.isUInt64())
        return 8;
    if (which.isUInt128() || which.isUUID())
        return 16;
    if (const auto * fixed_string = typeid_cast<const DataTypeFixedString *>(&type); fixed_string && (fixed_string->getN() == 16))
        return 16;
    return 0;
}

std::optional<IDLayout> tryGetIDLayout(const DataTypePtr & id_type)
{
    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(id_type.get()))
    {
        const auto & elements = tuple_type->getElements();
        if ((elements.size() != 2) || !WhichDataType{*elements[0]}.isUInt64())
            return {};
        size_t second_size = getIDComponentSize(*elements[1]);
        if (!second_size)
            return {};
        return IDLayout{.first_size = 8, .second_size = second_size};
    }
    size_t size = getIDComponentSize(*id_type);
    if (!size)
        return {};
    return IDLayout{.first_size = 0, .second_size = size};
}

/// Reads identifiers from a full column of the identifier type.
class IDReader
{
public:
    IDReader(const IColumn & column, const IDLayout & layout_) : layout(layout_)
    {
        if (layout.first_size)
        {
            const auto & tuple = assert_cast<const ColumnTuple &>(column);
            first = tuple.getColumn(0).getRawData().data();
            second = tuple.getColumn(1).getRawData().data();
        }
        else
        {
            second = column.getRawData().data();
        }
    }

    void get(size_t row, char * dest) const
    {
        if (layout.first_size)
            memcpy(dest, first + row * layout.first_size, layout.first_size);
        memcpy(dest + layout.first_size, second + row * layout.second_size, layout.second_size);
    }

private:
    const IDLayout layout;
    const char * first = nullptr;
    const char * second = nullptr;
};

constexpr size_t MAX_ID_SIZE = 24;


/// An open-addressing hash table from an identifier to the ordinal of its time series.
/// A cell has 8 bytes: 32 bits of the hash and the ordinal plus one. The identifiers themselves are stored by the caller.
class OrdinalTable
{
public:
    OrdinalTable() { cells.resize_fill(1 << 10); }

    void prefetch(UInt64 hash) const { __builtin_prefetch(&cells[hash & (cells.size() - 1)]); }

    template <typename Equal>
    Ordinal find(UInt64 hash, Equal && equal) const
    {
        const size_t mask = cells.size() - 1;
        const UInt64 tag = hash >> 32;
        for (size_t pos = hash & mask;; pos = (pos + 1) & mask)
        {
            const UInt64 cell = cells[pos];
            if (!cell)
                return NO_ORDINAL;
            if ((cell >> 32) == tag)
            {
                const Ordinal ordinal = static_cast<Ordinal>(cell) - 1;
                if (equal(ordinal))
                    return ordinal;
            }
        }
    }

    /// Returns the first ordinal which `find` would check, to prefetch its data.
    [[nodiscard]] Ordinal findCandidate(UInt64 hash) const
    {
        const size_t mask = cells.size() - 1;
        const UInt64 tag = hash >> 32;
        for (size_t pos = hash & mask;; pos = (pos + 1) & mask)
        {
            const UInt64 cell = cells[pos];
            if (!cell)
                return NO_ORDINAL;
            if ((cell >> 32) == tag)
                return static_cast<Ordinal>(cell) - 1;
        }
    }

    template <typename GetHash>
    void insert(UInt64 hash, Ordinal ordinal, GetHash && get_hash)
    {
        if ((count + 1) * 4 > cells.size() * 3)
        {
            PODArray<UInt64> old_cells;
            old_cells.swap(cells);
            cells.resize_fill(old_cells.size() * 2);
            for (UInt64 cell : old_cells)
            {
                if (cell)
                {
                    const Ordinal old_ordinal = static_cast<Ordinal>(cell) - 1;
                    place(get_hash(old_ordinal), old_ordinal);
                }
            }
        }
        place(hash, ordinal);
        ++count;
    }

    [[nodiscard]] size_t allocatedBytes() const { return cells.allocated_bytes(); }

private:
    void place(UInt64 hash, Ordinal ordinal)
    {
        const size_t mask = cells.size() - 1;
        size_t pos = hash & mask;
        while (cells[pos])
            pos = (pos + 1) & mask;
        cells[pos] = ((hash >> 32) << 32) | (static_cast<UInt64>(ordinal) + 1);
    }

    PODArray<UInt64> cells;
    size_t count = 0;
};


/// The list of the time series with one pair of a tag name and a tag value.
struct Posting
{
    Ordinal single = NO_ORDINAL;
    std::unique_ptr<roaring::Roaring> bitmap;

    /// Adds sorted ordinals.
    void addMany(size_t count, const Ordinal * ordinals)
    {
        if (!count)
            return;
        if (!bitmap)
        {
            if ((count == 1) && ((single == NO_ORDINAL) || (single == ordinals[0])))
            {
                single = ordinals[0];
                return;
            }
            bitmap = std::make_unique<roaring::Roaring>();
            if (single != NO_ORDINAL)
                bitmap->add(single);
        }
        bitmap->addMany(count, ordinals);
    }

    void unionInto(roaring::Roaring & dest) const
    {
        if (bitmap)
            dest |= *bitmap;
        else if (single != NO_ORDINAL)
            dest.add(single);
    }

    /// The size of the bitmap is the size of its serialized form plus an estimate for the separate allocations it consists of.
    [[nodiscard]] size_t allocatedBytes() const
    {
        return bitmap ? (sizeof(roaring::Roaring) + bitmap->getSizeInBytes(false) + ESTIMATED_OVERHEAD_OF_BITMAP) : 0;
    }
};


/// A Bloom filter for an unknown number of keys: it adds a segment twice as big when the last one is full.
/// A key is the hash of an identifier.
class GrowingBloomFilter
{
public:
    void add(UInt64 hash)
    {
        if (segments.empty() || (segments.back().count >= segments.back().capacity))
        {
            Segment segment;
            segment.capacity = segments.empty() ? INITIAL_CAPACITY : (segments.back().capacity * 2);
            segment.words.resize_fill(segment.capacity * BITS_PER_KEY / 64);
            segments.push_back(std::move(segment));
        }
        auto & segment = segments.back();
        const UInt64 num_bits = segment.words.size() * 64;
        const UInt64 step = intHash64(hash) | 1;
        for (size_t i = 0; i != NUM_HASHES; ++i)
        {
            const UInt64 bit = (hash + i * step) % num_bits;
            segment.words[bit / 64] |= (1ULL << (bit % 64));
        }
        ++segment.count;
    }

    [[nodiscard]] bool contains(UInt64 hash) const
    {
        const UInt64 step = intHash64(hash) | 1;
        for (const auto & segment : segments)
        {
            const UInt64 num_bits = segment.words.size() * 64;
            bool found = true;
            for (size_t i = 0; (i != NUM_HASHES) && found; ++i)
            {
                const UInt64 bit = (hash + i * step) % num_bits;
                found = (segment.words[bit / 64] & (1ULL << (bit % 64))) != 0;
            }
            if (found)
                return true;
        }
        return false;
    }

    [[nodiscard]] bool empty() const { return segments.empty(); }

    [[nodiscard]] size_t allocatedBytes() const
    {
        size_t res = 0;
        for (const auto & segment : segments)
            res += segment.words.allocated_bytes();
        return res;
    }

private:
    static constexpr size_t INITIAL_CAPACITY = 1 << 16;
    static constexpr size_t BITS_PER_KEY = 24;
    static constexpr size_t NUM_HASHES = 12;

    struct Segment
    {
        PODArray<UInt64> words;
        size_t capacity = 0;
        size_t count = 0;
    };

    std::vector<Segment> segments;
};


void writeVarUIntTo(PODArray<UInt8> & out, UInt64 x)
{
    while (x >= 0x80)
    {
        out.push_back(static_cast<UInt8>((x & 0x7F) | 0x80));
        x >>= 7;
    }
    out.push_back(static_cast<UInt8>(x));
}

UInt64 readVarUIntFrom(const UInt8 *& pos)
{
    UInt64 x = 0;
    for (size_t shift = 0;; shift += 7)
    {
        const UInt8 byte = *pos;
        ++pos;
        x |= static_cast<UInt64>(byte & 0x7F) << shift;
        if (!(byte & 0x80))
            return x;
    }
}


/// A matcher of a selector prepared for the index.
struct PreparedMatcher
{
    UInt32 name = NO_NAME;
    bool negated = false;
    bool is_regexp = false;
    String value;
    std::optional<Strings> alternatives;
    std::unique_ptr<OptimizedRegularExpression> regexp;
    /// Whether the matcher accepts a time series without the tag.
    bool matches_empty = false;

    [[nodiscard]] bool matchesValue(std::string_view tag_value) const
    {
        bool matches = false;
        if (!is_regexp)
            matches = (tag_value == value);
        else if (alternatives)
            matches = std::find(alternatives->begin(), alternatives->end(), tag_value) != alternatives->end();
        else
            matches = regexp->match(tag_value.data(), tag_value.size());
        return matches != negated;
    }
};


/// The data of the index. It is not thread-safe.
class SeriesIndexData
{
public:
    explicit SeriesIndexData(const IDLayout & layout_) : layout(layout_), id_size(layout_.size())
    {
    }

    [[nodiscard]] size_t numSeries() const { return flags.size(); }
    [[nodiscard]] size_t numUnresolved() const { return num_unresolved; }
    [[nodiscard]] const IDLayout & getLayout() const { return layout; }

    static UInt64 hashID(const char * id, size_t size) { return CityHash_v1_0_2::CityHash64(id, size); }

    [[nodiscard]] Ordinal find(const char * id, UInt64 hash) const
    {
        return table.find(hash, [&](Ordinal ordinal) { return memcmp(getID(ordinal), id, id_size) == 0; });
    }

    [[nodiscard]] bool hasTags(Ordinal ordinal) const { return label_offsets[ordinal] != NO_LABELS; }
    [[nodiscard]] Int64 getMinTime(Ordinal ordinal) const { return times[2 * static_cast<size_t>(ordinal)]; }
    [[nodiscard]] Int64 getMaxTime(Ordinal ordinal) const { return times[2 * static_cast<size_t>(ordinal) + 1]; }

    /// Applies rows of the "time ranges" table.
    /// A row of an unknown time series older than `horizon_start` is skipped, and its identifier is added to `out_of_horizon` if set.
    void applyTimeRanges(
        const IColumn & id_column, const IColumn & min_time_column, const IColumn & max_time_column,
        std::optional<Int64> horizon_start, GrowingBloomFilter * out_of_horizon)
    {
        const size_t num_rows = id_column.size();
        IDReader reader{id_column, layout};

        /// The cell of the hash table, the identifier and the time range of a time series are far from each other in the memory,
        /// so they are prefetched for a batch of rows.
        constexpr size_t batch_size = 32;
        char ids[batch_size][MAX_ID_SIZE];
        UInt64 hashes[batch_size];

        for (size_t begin = 0; begin < num_rows; begin += batch_size)
        {
            const size_t count = std::min(batch_size, num_rows - begin);
            for (size_t i = 0; i != count; ++i)
            {
                reader.get(begin + i, ids[i]);
                hashes[i] = hashID(ids[i], id_size);
                table.prefetch(hashes[i]);
            }
            for (size_t i = 0; i != count; ++i)
            {
                const Ordinal candidate = table.findCandidate(hashes[i]);
                if (candidate != NO_ORDINAL)
                {
                    __builtin_prefetch(getID(candidate));
                    __builtin_prefetch(&times[2 * static_cast<size_t>(candidate)]);
                }
            }
            for (size_t i = 0; i != count; ++i)
            {
                const Int64 min_time = min_time_column.getInt(begin + i);
                const Int64 max_time = max_time_column.getInt(begin + i);
                Ordinal ordinal = find(ids[i], hashes[i]);
                if (ordinal == NO_ORDINAL)
                {
                    if (horizon_start && (max_time < *horizon_start))
                    {
                        if (out_of_horizon)
                            out_of_horizon->add(hashes[i]);
                        continue;
                    }
                    ordinal = create(ids[i], hashes[i]);
                    if (!skipped_tags.empty() && skipped_tags.contains(hashes[i]))
                    {
                        flags[ordinal] |= FLAG_UNRESOLVED;
                        ++num_unresolved;
                    }
                }
                Int64 * series_times = &times[2 * static_cast<size_t>(ordinal)];
                series_times[0] = std::min(series_times[0], min_time);
                series_times[1] = std::max(series_times[1], max_time);
            }
        }
    }

    /// Applies rows of the "tags" table: `tags_column` contains all the tags of a time series, including the metric name.
    /// With `out_of_horizon` a row of an unknown time series found in it is skipped.
    void applyTags(const IColumn & id_column, const ColumnMap & tags_column, const GrowingBloomFilter * out_of_horizon)
    {
        const size_t num_rows = id_column.size();
        IDReader reader{id_column, layout};

        const auto & array = tags_column.getNestedColumn();
        const auto & offsets = array.getOffsets();
        const auto & tuple = assert_cast<const ColumnTuple &>(array.getData());
        const auto & names_column = assert_cast<const ColumnString &>(tuple.getColumn(0));
        const auto & values_column = assert_cast<const ColumnString &>(tuple.getColumn(1));

        /// The lists of time series are many and they are far from each other in the memory. The additions to them are collected
        /// for the whole block and sorted, so each list is updated once per block.
        pair_adds.clear();
        name_adds.clear();
        tagged_adds.clear();

        char id[MAX_ID_SIZE];
        for (size_t row = 0; row != num_rows; ++row)
        {
            reader.get(row, id);
            const UInt64 hash = hashID(id, id_size);
            Ordinal ordinal = find(id, hash);
            if (ordinal != NO_ORDINAL)
            {
                if (hasTags(ordinal))
                    continue;
            }
            else
            {
                if (out_of_horizon && out_of_horizon->contains(hash))
                {
                    skipped_tags.add(hash);
                    continue;
                }
                ordinal = create(id, hash);
            }

            pairs_buffer.clear();
            for (size_t i = offsets[static_cast<ssize_t>(row) - 1]; i != offsets[row]; ++i)
                pairs_buffer.push_back(internPair(names_column.getDataAt(i), values_column.getDataAt(i)));
            std::sort(pairs_buffer.begin(), pairs_buffer.end());

            label_offsets[ordinal] = label_bytes.size();
            writeVarUIntTo(label_bytes, pairs_buffer.size());
            UInt32 previous = 0;
            for (UInt32 pair : pairs_buffer)
            {
                writeVarUIntTo(label_bytes, pair - previous);
                previous = pair;
                pair_adds.push_back((static_cast<UInt64>(pair) << 32) | ordinal);
                name_adds.push_back((static_cast<UInt64>(pair_name[pair]) << 32) | ordinal);
            }
            tagged_adds.push_back(ordinal);

            if (flags[ordinal] & FLAG_UNRESOLVED)
            {
                flags[ordinal] &= ~FLAG_UNRESOLVED;
                --num_unresolved;
            }
        }

        addToPostings(pair_adds, [&](UInt32 pair) -> Posting & { return postings[pair]; });
        addToPostings(name_adds, [&](UInt32 name) -> Posting & { return names[name]->series; });
        std::sort(tagged_adds.begin(), tagged_adds.end());
        tagged.addMany(tagged_adds.size(), tagged_adds.data());
        posting_adds_since_calculation += pair_adds.size() + name_adds.size();
    }

    /// Returns false if no time series can match: a matcher requires a tag which no time series has.
    [[nodiscard]] bool prepareMatchers(const PrometheusQueryTree::MatcherList & matchers, std::vector<PreparedMatcher> & res) const
    {
        res.clear();
        res.reserve(matchers.size());
        for (const auto & matcher : matchers)
        {
            PreparedMatcher prepared;
            prepared.negated = (matcher.matcher_type == PrometheusQueryTree::MatcherType::NE)
                || (matcher.matcher_type == PrometheusQueryTree::MatcherType::NRE);
            prepared.is_regexp = (matcher.matcher_type == PrometheusQueryTree::MatcherType::RE)
                || (matcher.matcher_type == PrometheusQueryTree::MatcherType::NRE);
            if (!prepared.is_regexp)
            {
                prepared.value = matcher.label_value;
            }
            else if (auto alternatives = tryGetLiteralAlternatives(matcher.label_value))
            {
                prepared.alternatives = std::move(*alternatives);
            }
            else
            {
                /// The same regular expression and the same options as the function `match` gets in the query to the "tags" table.
                prepared.regexp = std::make_unique<OptimizedRegularExpression>(
                    makeFullMatchRegexp(matcher.label_value),
                    OptimizedRegularExpression::RE_NO_CAPTURE | OptimizedRegularExpression::RE_DOT_NL);
            }
            prepared.matches_empty = prepared.matchesValue({});

            if (const auto * it = name_ids.find(std::string_view{matcher.label_name}))
                prepared.name = it->getMapped();
            else if (!prepared.matches_empty)
                return false;
            else
                continue;

            res.push_back(std::move(prepared));
        }
        return true;
    }

    /// Finds the time series with tags matching all the matchers.
    [[nodiscard]] roaring::Roaring match(const std::vector<PreparedMatcher> & matchers) const
    {
        std::optional<roaring::Roaring> result;
        roaring::Roaring excluded;
        std::vector<const PreparedMatcher *> scans;

        auto intersect = [&](roaring::Roaring && bitmap)
        {
            if (!result)
                result = std::move(bitmap);
            else
                *result &= bitmap;
        };

        /// A matcher which doesn't accept a time series without the tag selects the time series with the accepted values.
        /// A matcher which accepts such time series excludes the time series with the values it doesn't accept.
        for (const auto & matcher : matchers)
        {
            const auto & label = *names[matcher.name];
            if (!matcher.is_regexp)
            {
                roaring::Roaring bitmap;
                if (matcher.value.empty())
                    label.series.unionInto(bitmap);
                else
                    unionValue(label, matcher.value, bitmap);
                if (matcher.matches_empty)
                    excluded |= bitmap;
                else
                    intersect(std::move(bitmap));
            }
            else if (matcher.alternatives)
            {
                roaring::Roaring listed;
                for (const auto & alternative : *matcher.alternatives)
                {
                    if (!alternative.empty())
                        unionValue(label, alternative, listed);
                }
                /// `=~` accepts the listed values, `!~` accepts the other ones.
                const bool accepts_listed = !matcher.negated;
                if (accepts_listed != matcher.matches_empty)
                {
                    if (matcher.matches_empty)
                        excluded |= listed;
                    else
                        intersect(std::move(listed));
                }
                else
                {
                    roaring::Roaring others;
                    label.series.unionInto(others);
                    others -= listed;
                    if (matcher.matches_empty)
                        excluded |= others;
                    else
                        intersect(std::move(others));
                }
            }
            else
            {
                scans.push_back(&matcher);
            }
        }

        for (const auto * matcher : scans)
        {
            const auto & label = *names[matcher->name];

            /// With few candidates it is cheaper to check their values than all the values of the tag.
            if (result && (result->cardinality() * 8 < label.pairs.size()))
            {
                filterCandidates(*result, *matcher);
                continue;
            }

            /// Select the values the matcher accepts if it selects, or the values it doesn't accept if it excludes.
            const bool select_accepted = !matcher->matches_empty;
            std::vector<UInt32> selected_pairs;
            for (UInt32 pair : label.pairs)
            {
                if (matcher->matchesValue(pair_value[pair]) == select_accepted)
                    selected_pairs.push_back(pair);
            }
            if (result && (selected_pairs.size() != label.pairs.size()) && (selected_pairs.size() > 1)
                && (result->cardinality() < selected_pairs.size() * MAX_CANDIDATES_PER_LIST_TO_CHECK))
            {
                filterCandidatesByPairs(*result, selected_pairs, /* keep_with_pairs = */ select_accepted);
                continue;
            }

            roaring::Roaring bitmap;
            if (selected_pairs.size() == label.pairs.size())
            {
                label.series.unionInto(bitmap);
            }
            else
            {
                for (UInt32 pair : selected_pairs)
                    postings[pair].unionInto(bitmap);
            }
            if (matcher->matches_empty)
                excluded |= bitmap;
            else
                intersect(std::move(bitmap));
        }

        if (!result)
            result = tagged;
        *result -= excluded;
        return std::move(*result);
    }

    /// Leaves in `ordinals` the time series from `bitmap` with a time range intersecting [min_time, max_time].
    void filterByTime(const roaring::Roaring & bitmap, std::optional<Int64> min_time, std::optional<Int64> max_time, PaddedPODArray<Ordinal> & ordinals) const
    {
        ordinals.resize(bitmap.cardinality());
        bitmap.toUint32Array(ordinals.data());
        if (!min_time && !max_time)
            return;
        size_t num_selected = 0;
        for (Ordinal ordinal : ordinals)
        {
            if ((!min_time || (getMaxTime(ordinal) >= *min_time)) && (!max_time || (getMinTime(ordinal) <= *max_time)))
                ordinals[num_selected++] = ordinal;
        }
        ordinals.resize(num_selected);
    }

    [[nodiscard]] MutableColumnPtr makeIDColumn(const DataTypePtr & id_type, const PaddedPODArray<Ordinal> & ordinals) const
    {
        if (!layout.first_size)
        {
            auto column = id_type->createColumn();
            column->reserve(ordinals.size());
            for (Ordinal ordinal : ordinals)
                column->insertData(getID(ordinal), layout.second_size);
            return column;
        }

        const auto & element_types = assert_cast<const DataTypeTuple &>(*id_type).getElements();
        auto first = element_types[0]->createColumn();
        auto second = element_types[1]->createColumn();
        first->reserve(ordinals.size());
        second->reserve(ordinals.size());
        for (Ordinal ordinal : ordinals)
        {
            const char * id = getID(ordinal);
            first->insertData(id, layout.first_size);
            second->insertData(id + layout.first_size, layout.second_size);
        }
        MutableColumns columns;
        columns.push_back(std::move(first));
        columns.push_back(std::move(second));
        return ColumnTuple::create(std::move(columns));
    }

    /// Makes the tags of time series. `pool` is used if the time series are many.
    [[nodiscard]] VectorWithMemoryTracking<TimeSeriesSeriesIndex::TagNamesAndValuesPtr> makeTags(const PaddedPODArray<Ordinal> & ordinals, ThreadPool & pool) const
    {
        VectorWithMemoryTracking<TimeSeriesSeriesIndex::TagNamesAndValuesPtr> res(ordinals.size());
        const size_t num_jobs = std::min(MAX_THREADS_TO_MAKE_TAGS_IN_LOOKUP, ordinals.size() / MIN_SERIES_PER_THREAD_TO_MAKE_TAGS);
        if (num_jobs <= 1)
        {
            makeTagsOfRange(ordinals, 0, ordinals.size(), res);
            return res;
        }

        /// The jobs use only what is declared before the runner, which waits for them.
        const auto * ordinals_ptr = &ordinals;
        auto * res_ptr = &res;
        ThreadPoolCallbackRunnerLocal<void> runner(pool, ThreadName::DEFAULT_THREAD_POOL);
        for (size_t job = 0; job != num_jobs; ++job)
        {
            const size_t begin = ordinals.size() * job / num_jobs;
            const size_t end = ordinals.size() * (job + 1) / num_jobs;
            runner.enqueueAndKeepTrack([this, ordinals_ptr, res_ptr, begin, end] { makeTagsOfRange(*ordinals_ptr, begin, end, *res_ptr); });
        }
        runner.waitForAllToFinishAndRethrowFirstError();
        return res;
    }

    /// The size of the allocated memory in bytes. The size of the lists of time series is the one set by `setPostingsBytes`
    /// plus an estimate for the time series added after that.
    [[nodiscard]] size_t bytes() const
    {
        size_t res = id_bytes.allocated_bytes() + table.allocatedBytes() + times.allocated_bytes() + label_offsets.allocated_bytes()
            + label_bytes.allocated_bytes() + flags.allocated_bytes() + arena.allocatedBytes() + skipped_tags.allocatedBytes()
            + pair_name.capacity() * sizeof(UInt32) + pair_value.capacity() * sizeof(std::string_view) + postings.capacity() * sizeof(Posting)
            + name_ids.getBufferSizeInBytes() + postings_bytes + posting_adds_since_calculation * ESTIMATED_BYTES_PER_POSTING_ADD;
        for (const auto & label : names)
            res += sizeof(LabelName) + label->pair_by_value.getBufferSizeInBytes() + label->pairs.capacity() * sizeof(UInt32);
        return res;
    }

    /// Calculates the size of the lists of time series by a pass over all of them.
    [[nodiscard]] size_t calculatePostingsBytes() const
    {
        size_t res = sizeof(roaring::Roaring) + tagged.getSizeInBytes(false);
        for (const auto & posting : postings)
            res += posting.allocatedBytes();
        for (const auto & label : names)
            res += label->series.allocatedBytes();
        return res;
    }

    void setPostingsBytes(size_t postings_bytes_)
    {
        postings_bytes = postings_bytes_;
        posting_adds_since_calculation = 0;
    }

    /// Whether the estimated part of the size is big enough to calculate the size again.
    [[nodiscard]] bool needsPostingsBytes() const
    {
        return posting_adds_since_calculation * ESTIMATED_BYTES_PER_POSTING_ADD * 8 > bytes();
    }

    [[nodiscard]] bool hasEstimatedPostingsBytes() const { return posting_adds_since_calculation != 0; }

private:
    struct LabelName
    {
        std::string_view name;
        /// The position of the name among all the names in the sorted order.
        UInt32 rank = 0;
        /// The values of the tag and the lists of time series having them.
        HashMapWithSavedHash<std::string_view, UInt32> pair_by_value;
        std::vector<UInt32> pairs;
        /// All the time series with the tag.
        Posting series;
    };

    [[nodiscard]] const char * getID(Ordinal ordinal) const { return id_bytes.data() + static_cast<size_t>(ordinal) * id_size; }

    Ordinal create(const char * id, UInt64 hash)
    {
        if (flags.size() >= NO_ORDINAL)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Too many time series in the series index of a TimeSeries table");
        const Ordinal ordinal = static_cast<Ordinal>(flags.size());
        id_bytes.insert(id, id + id_size);
        times.push_back(NO_MIN_TIME);
        times.push_back(NO_MAX_TIME);
        label_offsets.push_back(NO_LABELS);
        flags.push_back(UInt8{0});
        table.insert(hash, ordinal, [&](Ordinal other) { return hashID(getID(other), id_size); });
        return ordinal;
    }

    void makeTagsOfRange(
        const PaddedPODArray<Ordinal> & ordinals, size_t begin, size_t end,
        VectorWithMemoryTracking<TimeSeriesSeriesIndex::TagNamesAndValuesPtr> & res) const
    {
        using TagNamesAndValues = ContextTimeSeriesTagsCollector::TagNamesAndValues;
        std::vector<UInt32> pairs;
        for (size_t i = begin; i != end; ++i)
        {
            pairs.clear();
            forEachPair(ordinals[i], [&](UInt32 pair) { pairs.push_back(pair); });
            /// The tags are sorted by name.
            std::sort(pairs.begin(), pairs.end(),
                [&](UInt32 left, UInt32 right) { return names[pair_name[left]]->rank < names[pair_name[right]]->rank; });
            auto tags = std::make_shared<TagNamesAndValues>();
            tags->reserve(pairs.size());
            for (UInt32 pair : pairs)
                tags->emplace_back(names[pair_name[pair]]->name, pair_value[pair]);
            res[i] = std::move(tags);
        }
    }

    /// Adds time series to lists: an element of `adds` is the number of a list in the upper 32 bits and the ordinal in the lower ones.
    template <typename GetPosting>
    void addToPostings(PODArray<UInt64> & adds, GetPosting && get_posting)
    {
        if (adds.empty())
            return;
        RadixSort<RadixSortUIntTraits<UInt64>>::executeLSD(adds.data(), adds.size());
        ordinals_buffer.clear();
        for (size_t i = 0; i != adds.size(); ++i)
        {
            ordinals_buffer.push_back(static_cast<Ordinal>(adds[i]));
            if ((i + 1 == adds.size()) || ((adds[i + 1] >> 32) != (adds[i] >> 32)))
            {
                get_posting(static_cast<UInt32>(adds[i] >> 32)).addMany(ordinals_buffer.size(), ordinals_buffer.data());
                ordinals_buffer.clear();
            }
        }
    }

    UInt32 internPair(std::string_view name, std::string_view value)
    {
        UInt32 name_id = 0;
        if (const auto * it = name_ids.find(name))
        {
            name_id = it->getMapped();
        }
        else
        {
            name_id = static_cast<UInt32>(names.size());
            auto label = std::make_unique<LabelName>();
            label->name = std::string_view{arena.insert(name.data(), name.size()), name.size()};
            name_ids[label->name] = name_id;
            names.push_back(std::move(label));
            updateRanks();
        }

        auto & label = *names[name_id];
        if (const auto * it = label.pair_by_value.find(value))
            return it->getMapped();

        const UInt32 pair = static_cast<UInt32>(pair_name.size());
        const std::string_view stored_value{arena.insert(value.data(), value.size()), value.size()};
        label.pair_by_value[stored_value] = pair;
        label.pairs.push_back(pair);
        pair_name.push_back(name_id);
        pair_value.push_back(stored_value);
        postings.emplace_back();
        return pair;
    }

    void updateRanks()
    {
        std::vector<UInt32> order(names.size());
        for (UInt32 i = 0; i != order.size(); ++i)
            order[i] = i;
        std::sort(order.begin(), order.end(), [&](UInt32 left, UInt32 right) { return names[left]->name < names[right]->name; });
        for (UInt32 i = 0; i != order.size(); ++i)
            names[order[i]]->rank = i;
    }

    void unionValue(const LabelName & label, std::string_view value, roaring::Roaring & dest) const
    {
        if (const auto * it = label.pair_by_value.find(value))
            postings[it->getMapped()].unionInto(dest);
    }

    template <typename F>
    void forEachPair(Ordinal ordinal, F && f) const
    {
        const UInt8 * pos = label_bytes.data() + label_offsets[ordinal];
        const UInt64 count = readVarUIntFrom(pos);
        UInt32 pair = 0;
        for (UInt64 i = 0; i != count; ++i)
        {
            pair += static_cast<UInt32>(readVarUIntFrom(pos));
            f(pair);
        }
    }

    /// Removes from `candidates` the time series which the matcher doesn't accept.
    void filterCandidates(roaring::Roaring & candidates, const PreparedMatcher & matcher) const
    {
        /// Whether the matcher accepts the value of a pair: 1 if it accepts, 0 if not.
        std::unordered_map<UInt32, bool> accepted_by_pair;
        roaring::Roaring rejected;
        for (Ordinal ordinal : candidates)
        {
            UInt32 found_pair = NO_NAME;
            forEachPair(ordinal, [&](UInt32 pair)
            {
                if (pair_name[pair] == matcher.name)
                    found_pair = pair;
            });
            bool accepted = false;
            if (found_pair == NO_NAME)
            {
                accepted = matcher.matches_empty;
            }
            else
            {
                auto [it, inserted] = accepted_by_pair.try_emplace(found_pair, false);
                if (inserted)
                    it->second = matcher.matchesValue(pair_value[found_pair]);
                accepted = it->second;
            }
            if (!accepted)
                rejected.add(ordinal);
        }
        candidates -= rejected;
    }

    /// Leaves in `candidates` the time series which have one of `pairs` if `keep_with_pairs` is set,
    /// or the time series which have none of them if it is not set.
    void filterCandidatesByPairs(roaring::Roaring & candidates, const std::vector<UInt32> & pairs, bool keep_with_pairs) const
    {
        PODArray<UInt64> is_selected;
        is_selected.resize_fill((pair_name.size() + 63) / 64);
        for (UInt32 pair : pairs)
            is_selected[pair / 64] |= (1ULL << (pair % 64));

        PODArray<Ordinal> rejected;
        for (Ordinal ordinal : candidates)
        {
            bool has_pair = false;
            forEachPair(ordinal, [&](UInt32 pair) { has_pair |= ((is_selected[pair / 64] >> (pair % 64)) & 1) != 0; });
            if (has_pair != keep_with_pairs)
                rejected.push_back(ordinal);
        }
        if (!rejected.empty())
            candidates -= roaring::Roaring(rejected.size(), rejected.data());
    }

    const IDLayout layout;
    const size_t id_size;

    /// The identifier of each time series: `id_size` bytes per time series.
    PODArray<char> id_bytes;
    OrdinalTable table;
    /// The minimum and the maximum time of each time series.
    PODArray<Int64> times;
    /// The tags of each time series: the number of pairs and the sorted numbers of the pairs,
    /// each written as the difference with the previous one in the variable-length format.
    PODArray<UInt64> label_offsets;
    PODArray<UInt8> label_bytes;
    PODArray<UInt8> flags;
    /// The time series with tags.
    roaring::Roaring tagged;
    size_t num_unresolved = 0;

    /// The identifiers of the time series whose rows of the "tags" table were skipped.
    GrowingBloomFilter skipped_tags;

    Arena arena;
    std::vector<std::unique_ptr<LabelName>> names;
    HashMapWithSavedHash<std::string_view, UInt32> name_ids;
    /// The pairs of a tag name and a tag value.
    std::vector<UInt32> pair_name;
    std::vector<std::string_view> pair_value;
    std::vector<Posting> postings;
    size_t postings_bytes = 0;
    size_t posting_adds_since_calculation = 0;

    /// Used by `applyTags` only.
    std::vector<UInt32> pairs_buffer;
    PODArray<UInt64> pair_adds;
    PODArray<UInt64> name_adds;
    PODArray<Ordinal> tagged_adds;
    PODArray<Ordinal> ordinals_buffer;
};


/// The ranges of block numbers of one target table which the index has applied.
class AppliedRanges
{
public:
    enum class Status
    {
        Applied,
        NotApplied,
        /// The index has applied the same blocks before a mutation changed them.
        Changed,
    };

    [[nodiscard]] Status check(const MergeTreePartInfo & info) const
    {
        auto partition_it = partitions.find(info.getPartitionId());
        if (partition_it == partitions.end())
            return Status::NotApplied;
        const auto & ranges = partition_it->second;

        auto next = ranges.upper_bound(info.min_block);
        if (next != ranges.begin())
        {
            auto previous = std::prev(next);
            if (previous->second.max_block >= info.min_block)
            {
                if (!sameData(previous->first, previous->second.mutation, info.mutation))
                    return Status::Changed;
                if (previous->second.max_block >= info.max_block)
                    return Status::Applied;
            }
        }
        for (auto it = next; (it != ranges.end()) && (it->first <= info.max_block); ++it)
        {
            if (!sameData(it->first, it->second.mutation, info.mutation))
                return Status::Changed;
        }
        return Status::NotApplied;
    }

    /// Adds the blocks of a data part which was applied as a whole.
    void add(const MergeTreePartInfo & info)
    {
        auto & ranges = partitions[info.getPartitionId()];
        Int64 min_block = info.min_block;
        Int64 max_block = info.max_block;

        /// The data part replaces the ranges it covers, and extends the adjacent ranges with the same mutation.
        auto it = ranges.upper_bound(min_block);
        if (it != ranges.begin())
        {
            auto previous = std::prev(it);
            if ((previous->second.max_block >= min_block) || ((previous->second.max_block + 1 == min_block) && (previous->second.mutation == info.mutation)))
                it = previous;
        }
        while ((it != ranges.end()) && ((it->first <= max_block) || ((it->first == max_block + 1) && (it->second.mutation == info.mutation))))
        {
            min_block = std::min(min_block, it->first);
            max_block = std::max(max_block, it->second.max_block);
            it = ranges.erase(it);
        }
        ranges[min_block] = Range{.max_block = max_block, .mutation = info.mutation};
    }

    void clear() { partitions.clear(); }

private:
    struct Range
    {
        Int64 max_block = 0;
        Int64 mutation = 0;
    };

    /// A mutation doesn't change the blocks written after it, so two mutation versions older than a block give the same data.
    static bool sameData(Int64 min_block, Int64 applied_mutation, Int64 mutation)
    {
        return (applied_mutation == mutation) || ((applied_mutation < min_block) && (mutation < min_block));
    }

    /// Partition identifier -> the first block of a range -> the range.
    std::unordered_map<String, std::map<Int64, Range>> partitions;
};


/// What the index knows about one target table.
struct TableState
{
    AppliedRanges applied;

    struct KnownPart
    {
        MergeTreePartInfo info;
        size_t rows = 0;
    };
    /// The active data parts at the last synchronization, all of them applied.
    std::unordered_map<String, KnownPart> known_parts;
    /// The metadata of the table the index was built with.
    StorageMetadataPtr metadata;

    void clear()
    {
        applied.clear();
        known_parts.clear();
        metadata = nullptr;
    }
};


/// The target tables of the TimeSeries table and what is needed to read them.
struct TargetTables
{
    StoragePtr tags_storage;
    StoragePtr time_ranges_storage;
    const MergeTreeData * tags = nullptr;
    const MergeTreeData * time_ranges = nullptr;
    StorageMetadataPtr tags_metadata;
    StorageMetadataPtr time_ranges_metadata;
    /// The type of the identifiers without `LowCardinality`.
    DataTypePtr id_type;
    IDLayout id_layout;
    /// The expressions selected from the "tags" table: the identifier and the map of all the tags.
    String tags_select_list;
    /// The time before which the index doesn't hold time series, in the scale of the timestamps of the table.
    std::optional<Int64> horizon_start;
};

using DataPartsVector = MergeTreeData::DataPartsVector;

DataPartsVector getVisibleDataParts(const StoragePtr & storage, const StorageMetadataPtr & metadata, const ContextPtr & context)
{
    auto snapshot = storage->getStorageSnapshot(metadata, context);
    const auto & snapshot_data = assert_cast<const MergeTreeData::SnapshotData &>(*snapshot->data);
    DataPartsVector res;
    if (!snapshot_data.parts)
        return res;
    res.reserve(snapshot_data.parts->size());
    for (const auto & part : *snapshot_data.parts)
        res.push_back(part.data_part);
    return res;
}

bool isSuitableTargetTable(const MergeTreeData & table, const StorageInMemoryMetadata & metadata)
{
    if (metadata.hasAnyTTL())
        return false;
    /// Lightweight updates change rows without a new version of the data parts.
    if (!table.getPatchPartsVectorForInternalUsage().empty())
        return false;
    const auto & merging_params = table.merging_params;
    switch (merging_params.mode)
    {
        case MergeTreeData::MergingParams::Ordinary:
        case MergeTreeData::MergingParams::Aggregating:
            return true;
        case MergeTreeData::MergingParams::Replacing:
            return merging_params.is_deleted_column.empty();
        default:
            return false;
    }
}

}


struct TimeSeriesSeriesIndex::Impl : public WithContext
{
    enum class State
    {
        NotBuilt,
        Building,
        Ready,
    };

    Impl(const StorageTimeSeries & time_series_storage_, const ContextPtr & global_context_, UInt64 max_size_bytes_)
        : WithContext(global_context_)
        , time_series_storage(time_series_storage_)
        , max_size_bytes(max_size_bytes_)
        , log(getLogger("TimeSeriesSeriesIndex (" + time_series_storage_.getStorageID().getNameForLogs() + ")"))
    {
        task = getContext()->getSchedulePool()->createTask(time_series_storage.getStorageID(), log->name(), [this] { runTask(); });
        last_lookup_seconds = nowSeconds();
    }

    ~Impl()
    {
        shutdown();
        resetMetrics(0, 0);
    }

    void shutdown()
    {
        shutdown_called = true;
        if (task)
            task->deactivate();
    }

    using Clock = std::chrono::steady_clock;

    static Int64 nowSeconds()
    {
        return std::chrono::duration_cast<std::chrono::seconds>(Clock::now().time_since_epoch()).count();
    }

    static Int64 nowMilliseconds()
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now().time_since_epoch()).count();
    }

    const StorageTimeSeries & time_series_storage;
    const UInt64 max_size_bytes;
    const LoggerPtr log;

    BackgroundSchedulePoolTaskHolder task;
    std::atomic<bool> shutdown_called = false;
    std::atomic<bool> task_started = false;
    /// The time of the last lookup in seconds of the steady clock.
    std::atomic<Int64> last_lookup_seconds = 0;
    /// The queries don't wait for the index and don't apply data parts before this time, in milliseconds of the steady clock.
    std::atomic<Int64> no_wait_before_ms = 0;

    /// Makes the tags of the time series found by the lookups. The threads stop when there is no job for them.
    ThreadPool lookup_pool{
        CurrentMetrics::TimeSeriesSeriesIndexThreads, CurrentMetrics::TimeSeriesSeriesIndexThreadsActive,
        CurrentMetrics::TimeSeriesSeriesIndexThreadsScheduled,
        MAX_THREADS_TO_MAKE_TAGS, /* max_free_threads = */ 0, /* queue_size = */ 0};

    /// Held while data parts are applied or the index is rebuilt, so only one thread does that at a time.
    std::timed_mutex sync_mutex;

    /// It is changed with both mutexes held, so a thread which holds one of them reads the current value.
    /// A thread which holds none of them can read a value which is not current.
    std::atomic<State> state = State::NotBuilt;

    /// Protects everything below.
    mutable SharedMutex mutex;
    UInt64 epoch = 0;
    std::unique_ptr<SeriesIndexData> data;
    TableState tags_state;
    TableState time_ranges_state;
    DataTypePtr id_type;
    std::optional<Int64> horizon_start;

    /// These are used only with `sync_mutex` held.
    /// The identifiers of the time series which were older than the horizon when the index was built.
    std::unique_ptr<GrowingBloomFilter> out_of_horizon;
    std::optional<Clock::time_point> unresolved_since;
    Clock::time_point no_rebuild_before;
    Clock::time_point size_limit_retry_time;
    Clock::time_point last_postings_bytes_calculation;
    Stopwatch build_watch;
    /// Whether the last call of `synchronize` in the background task has applied data parts.
    bool applied_data_parts = false;

    std::mutex metrics_mutex;
    Int64 reported_series = 0;
    Int64 reported_bytes = 0;

    void resetMetrics(Int64 series, Int64 bytes)
    {
        std::lock_guard lock{metrics_mutex};
        CurrentMetrics::add(CurrentMetrics::TimeSeriesSeriesIndexSeries, series - reported_series);
        CurrentMetrics::add(CurrentMetrics::TimeSeriesSeriesIndexBytes, bytes - reported_bytes);
        reported_series = series;
        reported_bytes = bytes;
    }

    /// Returns the target tables if the index can be built from them.
    /// With `for_building` the function also prepares what is needed to read them.
    std::optional<TargetTables> tryGetTables(const ContextPtr & local_context, bool for_building) const
    {
        if (!time_series_storage.isInnerTable(ViewTarget::Tags) || !time_series_storage.isInnerTable(ViewTarget::TimeRanges))
            return {};

        TargetTables tables;
        tables.tags_storage = time_series_storage.tryGetTargetTable(ViewTarget::Tags, local_context);
        tables.time_ranges_storage = time_series_storage.tryGetTargetTable(ViewTarget::TimeRanges, local_context);
        if (!tables.tags_storage || !tables.time_ranges_storage)
            return {};

        tables.tags = dynamic_cast<const MergeTreeData *>(tables.tags_storage.get());
        tables.time_ranges = dynamic_cast<const MergeTreeData *>(tables.time_ranges_storage.get());
        if (!tables.tags || !tables.time_ranges)
            return {};

        auto tags_metadata = tables.tags_storage->getInMemoryMetadataPtr(local_context, false);
        auto time_ranges_metadata = tables.time_ranges_storage->getInMemoryMetadataPtr(local_context, false);
        tables.tags_metadata = tags_metadata;
        tables.time_ranges_metadata = time_ranges_metadata;
        if (!isSuitableTargetTable(*tables.tags, *tables.tags_metadata) || !isSuitableTargetTable(*tables.time_ranges, *tables.time_ranges_metadata))
            return {};

        const auto & tags_columns = tables.tags_metadata->getColumns();
        const auto & time_ranges_columns = tables.time_ranges_metadata->getColumns();
        if (!tags_columns.hasPhysical(TimeSeriesColumnNames::ID) || !tags_columns.hasPhysical(TimeSeriesColumnNames::Tags)
            || !tags_columns.hasPhysical(TimeSeriesColumnNames::MetricName) || !time_ranges_columns.hasPhysical(TimeSeriesColumnNames::ID)
            || !time_ranges_columns.hasPhysical(TimeSeriesColumnNames::MinTime) || !time_ranges_columns.hasPhysical(TimeSeriesColumnNames::MaxTime))
            return {};

        if (time_ranges_columns.getPhysical(TimeSeriesColumnNames::MinTime).type->isNullable()
            || time_ranges_columns.getPhysical(TimeSeriesColumnNames::MaxTime).type->isNullable())
            return {};

        tables.id_type = recursiveRemoveLowCardinality(tags_columns.getPhysical(TimeSeriesColumnNames::ID).type);
        if (!tables.id_type->equals(*recursiveRemoveLowCardinality(time_ranges_columns.getPhysical(TimeSeriesColumnNames::ID).type)))
            return {};
        auto id_layout = tryGetIDLayout(tables.id_type);
        if (!id_layout)
            return {};
        tables.id_layout = *id_layout;

        if (!for_building)
            return tables;

        auto time_series_settings = time_series_storage.getStorageSettings();

        /// The same expression as in the query to the "tags" table made by `StorageTimeSeriesSelector`.
        String all_tags = fmt::format("timeSeriesTagsToMap({}, {}, {}",
            backQuote(TimeSeriesColumnNames::Tags), quoteString(TimeSeriesTagNames::MetricName), backQuote(TimeSeriesColumnNames::MetricName));
        const Map & tags_to_columns = (*time_series_settings)[TimeSeriesSetting::tags_to_columns];
        for (const auto & tag_name_and_column_name : tags_to_columns)
        {
            const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
            all_tags += fmt::format(", {}, {}", quoteString(tuple.at(0).safeGet<String>()), backQuote(tuple.at(1).safeGet<String>()));
        }
        all_tags += ")";
        tables.tags_select_list = fmt::format("{}, {}", backQuote(TimeSeriesColumnNames::ID), all_tags);

        tables.horizon_start = getHorizonStart(local_context);
        return tables;
    }

    ContextMutablePtr makeContextForReading(size_t num_threads) const
    {
        auto local_context = Context::createCopy(getContext());
        local_context->makeQueryContext();
        local_context->setCurrentQueryId("");
        local_context->setSetting("max_threads", UInt64{num_threads});
        /// The index is not built by a query, so the limits of a query don't apply.
        local_context->setSetting("max_execution_time", UInt64{0});
        local_context->setSetting("max_rows_to_read", UInt64{0});
        local_context->setSetting("max_bytes_to_read", UInt64{0});
        local_context->setSetting("max_result_rows", UInt64{0});
        local_context->setSetting("max_result_bytes", UInt64{0});
        local_context->setSetting("use_query_condition_cache", false);
        /// Each listing of the data parts must see the current ones.
        local_context->setSetting("enable_shared_storage_snapshot_in_query", false);
        return local_context;
    }

    /// Reads the specified data parts of a target table and passes their rows to `apply_block` until it returns false.
    /// The blocks come in any order if `num_threads` is more than one. Returns the number of rows read.
    template <typename ApplyBlock>
    size_t readDataParts(
        const StorageID & table_id, const String & select_list, const DataPartsVector & parts, const ContextPtr & local_context,
        size_t num_threads, ApplyBlock && apply_block) const
    {
        String part_names;
        for (const auto & part : parts)
        {
            if (!part_names.empty())
                part_names += ", ";
            part_names += quoteString(part->name);
        }
        String query = fmt::format("SELECT {} FROM {} WHERE _part IN ({})", select_list, table_id.getFullTableName(), part_names);

        ParserSelectWithUnionQuery parser;
        ASTPtr ast = parseQuery(parser, query, /* max_query_size = */ 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

        InterpreterSelectQueryAnalyzer interpreter(ast, local_context, SelectQueryOptions{});
        auto io = interpreter.execute();

        size_t num_rows = 0;
        auto pull_all = [&](auto & executor)
        {
            Block block;
            while (executor.pull(block))
            {
                if (!block.rows())
                    continue;
                num_rows += block.rows();
                if (shutdown_called || !apply_block(block))
                {
                    executor.cancel();
                    break;
                }
            }
        };

        if (num_threads > 1)
        {
            PullingAsyncPipelineExecutor executor(io.pipeline);
            pull_all(executor);
        }
        else
        {
            PullingPipelineExecutor executor(io.pipeline);
            pull_all(executor);
        }
        return num_rows;
    }

    /// Applies the specified data parts of a target table, and marks as applied the ones which were read for sure.
    /// Returns false if the index has reached its size limit or is shut down.
    bool applyDataParts(
        const TargetTables & tables, bool is_tags_table, const DataPartsVector & parts_, const ContextPtr & local_context, size_t num_threads)
    {
        Stopwatch watch;
        SCOPE_EXIT({ ProfileEvents::increment(ProfileEvents::TimeSeriesSeriesIndexApplyMicroseconds, watch.elapsedMicroseconds()); });

        const auto & storage = is_tags_table ? tables.tags_storage : tables.time_ranges_storage;
        const auto & metadata = is_tags_table ? tables.tags_metadata : tables.time_ranges_metadata;
        const auto table_id = storage->getStorageID();
        const String select_list = is_tags_table
            ? tables.tags_select_list
            : fmt::format("{}, {}, {}", backQuote(TimeSeriesColumnNames::ID), backQuote(TimeSeriesColumnNames::MinTime), backQuote(TimeSeriesColumnNames::MaxTime));

        /// The new data parts are small, and the queries wait for them. A big data part is the result of a merge.
        DataPartsVector parts = parts_;
        std::stable_sort(parts.begin(), parts.end(),
            [](const DataPartPtr & left, const DataPartPtr & right) { return left->rows_count < right->rows_count; });

        size_t begin = 0;
        while (begin != parts.size())
        {
            if (shutdown_called)
                return false;

            size_t end = begin;
            size_t rows_in_batch = 0;
            DataPartsVector batch;
            while ((end != parts.size()) && (batch.empty() || (rows_in_batch + parts[end]->rows_count <= MAX_ROWS_IN_BATCH)))
            {
                rows_in_batch += parts[end]->rows_count;
                if (parts[end]->rows_count)
                    batch.push_back(parts[end]);
                ++end;
            }

            size_t num_rows = 0;
            bool size_limit_reached = false;
            if (!batch.empty())
            {
                num_rows = readDataParts(table_id, select_list, batch, local_context, num_threads, [&](const Block & block)
                {
                    auto id_column = recursiveRemoveLowCardinality(block.getByPosition(0).column->convertToFullColumnIfConst());
                    std::lock_guard lock{mutex};
                    if (is_tags_table)
                    {
                        auto tags_column = block.getByPosition(1).column->convertToFullColumnIfConst();
                        data->applyTags(*id_column, assert_cast<const ColumnMap &>(*tags_column), out_of_horizon.get());
                    }
                    else
                    {
                        auto min_time_column = block.getByPosition(1).column->convertToFullColumnIfConst();
                        auto max_time_column = block.getByPosition(2).column->convertToFullColumnIfConst();
                        data->applyTimeRanges(*id_column, *min_time_column, *max_time_column, horizon_start, out_of_horizon.get());
                    }
                    size_limit_reached = (data->bytes() > max_size_bytes);
                    return !size_limit_reached;
                });
            }
            if (size_limit_reached || shutdown_called)
                return false;

            /// A data part which was active before the query and is active after it was read by the query as a whole.
            std::unordered_set<String> active_names;
            for (const auto & part : getVisibleDataParts(storage, metadata, local_context))
                active_names.insert(part->name);

            {
                std::lock_guard lock{mutex};
                auto & table_state = is_tags_table ? tags_state : time_ranges_state;
                for (size_t i = begin; i != end; ++i)
                {
                    const auto & part = parts[i];
                    if (!active_names.contains(part->name))
                        continue;
                    table_state.applied.add(part->info);
                    table_state.known_parts[part->name] = TableState::KnownPart{.info = part->info, .rows = part->rows_count};
                }
            }

            ProfileEvents::increment(ProfileEvents::TimeSeriesSeriesIndexAppliedParts, end - begin);
            ProfileEvents::increment(ProfileEvents::TimeSeriesSeriesIndexAppliedRows, num_rows);
            applied_data_parts = true;
            begin = end;
        }
        return true;
    }

    struct TableCheck
    {
        DataPartsVector not_applied;
        size_t not_applied_rows = 0;
        /// The table has lost rows the index has applied, or a mutation has changed them.
        bool changed = false;
    };

    /// Compares the active data parts of a target table with what the index has applied.
    static TableCheck checkTable(const TableState & table_state, const DataPartsVector & parts, const StorageMetadataPtr & metadata)
    {
        TableCheck res;
        if (table_state.metadata && (table_state.metadata.get() != metadata.get()))
            res.changed = true;

        std::unordered_set<std::string_view> active_names;
        for (const auto & part : parts)
        {
            active_names.insert(part->name);
            if (part->info.isPatch())
            {
                res.changed = true;
                continue;
            }
            switch (table_state.applied.check(part->info))
            {
                case AppliedRanges::Status::Applied:
                    break;
                case AppliedRanges::Status::NotApplied:
                    res.not_applied.push_back(part);
                    res.not_applied_rows += part->rows_count;
                    break;
                case AppliedRanges::Status::Changed:
                    res.changed = true;
                    break;
            }
        }

        /// A data part the index has applied must be active or merged into an active data part with rows.
        for (const auto & [name, known_part] : table_state.known_parts)
        {
            if (active_names.contains(name))
                continue;
            bool merged = false;
            for (const auto & part : parts)
            {
                if (part->info.contains(known_part.info) && (part->rows_count || !known_part.rows))
                {
                    merged = true;
                    break;
                }
            }
            if (!merged)
            {
                res.changed = true;
                break;
            }
        }
        return res;
    }

    /// Remembers the active data parts which are applied, to notice when one of them disappears.
    static void updateKnownParts(TableState & table_state, const DataPartsVector & parts)
    {
        table_state.known_parts.clear();
        for (const auto & part : parts)
        {
            if (table_state.applied.check(part->info) == AppliedRanges::Status::Applied)
                table_state.known_parts[part->name] = TableState::KnownPart{.info = part->info, .rows = part->rows_count};
        }
    }

    /// Drops the data of the index. Requires `sync_mutex`.
    void reset(State new_state)
    {
        std::lock_guard lock{mutex};
        data = nullptr;
        tags_state.clear();
        time_ranges_state.clear();
        out_of_horizon = nullptr;
        unresolved_since.reset();
        state = new_state;
        ++epoch;
        resetMetrics(0, 0);
    }

    /// Starts building the index from the current state of the tables. Requires `sync_mutex`.
    void startBuilding(const TargetTables & tables)
    {
        reset(State::Building);
        std::lock_guard lock{mutex};
        data = std::make_unique<SeriesIndexData>(tables.id_layout);
        id_type = tables.id_type;
        horizon_start = tables.horizon_start;
        tags_state.metadata = tables.tags_metadata;
        time_ranges_state.metadata = tables.time_ranges_metadata;
        if (horizon_start)
            out_of_horizon = std::make_unique<GrowingBloomFilter>();
        ProfileEvents::increment(ProfileEvents::TimeSeriesSeriesIndexRebuilds);
        build_watch.restart();
        LOG_DEBUG(log, "Building the series index");
    }

    /// Calculates the size of the lists of time series if it is time to do that. Requires `sync_mutex`:
    /// no other thread changes the data, so the calculation doesn't stop the lookups.
    void updatePostingsBytes(bool force)
    {
        const auto now = Clock::now();
        size_t postings_bytes = 0;
        {
            std::shared_lock lock{mutex};
            if (!data->hasEstimatedPostingsBytes())
                return;
            if (!force && !data->needsPostingsBytes() && (now - last_postings_bytes_calculation < POSTINGS_BYTES_CALCULATION_INTERVAL))
                return;
            postings_bytes = data->calculatePostingsBytes();
        }
        std::lock_guard lock{mutex};
        data->setPostingsBytes(postings_bytes);
        last_postings_bytes_calculation = now;
    }

    /// Brings the index up to date with the tables. Requires `sync_mutex`.
    /// With `max_rows` the function doesn't build the index and doesn't apply more rows than that: a query uses it.
    /// With `immediately` the function doesn't wait for the time when the next rebuild is allowed.
    /// Returns whether the index is ready and all the data parts found by the function are applied.
    bool synchronize(std::optional<size_t> max_rows, bool immediately)
    {
        if (shutdown_called)
            return false;

        /// The memory of the index doesn't belong to the query which applies data parts to it.
        MemoryTrackerSwitcher memory_tracker_switcher{&total_memory_tracker};

        auto tables = tryGetTables(getContext(), /* for_building = */ true);
        if (!tables)
        {
            if (state != State::NotBuilt)
                reset(State::NotBuilt);
            return false;
        }

        /// A query applies a few rows, so it reads them itself.
        const size_t num_threads = max_rows ? 1 : NUM_THREADS_TO_READ;
        auto local_context = makeContextForReading(num_threads);
        size_t num_builds = 0;
        size_t num_time_ranges_passes = 0;
        size_t num_passes_when_ready = 0;
        bool all_applied = false;

        while (true)
        {
            if (state == State::NotBuilt)
            {
                const auto now = Clock::now();
                /// The tables can change again and again while the index is built, the next call builds it then.
                if (max_rows || (now < size_limit_retry_time) || (!immediately && (now < no_rebuild_before)) || (num_builds == 3))
                    return false;
                no_rebuild_before = now + MIN_TIME_BETWEEN_REBUILDS;
                startBuilding(*tables);
                ++num_builds;
                num_time_ranges_passes = 0;
            }

            auto time_ranges_parts = getVisibleDataParts(tables->time_ranges_storage, tables->time_ranges_metadata, local_context);
            auto tags_parts = getVisibleDataParts(tables->tags_storage, tables->tags_metadata, local_context);

            TableCheck time_ranges_check;
            TableCheck tags_check;
            {
                std::shared_lock lock{mutex};
                time_ranges_check = checkTable(time_ranges_state, time_ranges_parts, tables->time_ranges_metadata);
                tags_check = checkTable(tags_state, tags_parts, tables->tags_metadata);
            }

            if (time_ranges_check.changed || tags_check.changed)
            {
                LOG_DEBUG(log, "The target tables have changed, the series index will be rebuilt");
                reset(State::NotBuilt);
                continue;
            }

            {
                std::lock_guard lock{mutex};
                updateKnownParts(time_ranges_state, time_ranges_parts);
                updateKnownParts(tags_state, tags_parts);
            }

            if (state == State::Building)
            {
                if (max_rows)
                    return false;

                /// The time ranges are applied before the tags, so the tags of the time series older than the horizon are skipped.
                /// New data parts appear all the time: the ones left after a few passes are applied after the tags.
                if (!time_ranges_check.not_applied.empty() && (num_time_ranges_passes < 3))
                {
                    if (!applyDataParts(*tables, /* is_tags_table = */ false, time_ranges_check.not_applied, local_context, num_threads))
                        break;
                    ++num_time_ranges_passes;
                    continue;
                }
                if (!applyDataParts(*tables, /* is_tags_table = */ true, tags_check.not_applied, local_context, num_threads))
                    break;
                updatePostingsBytes(/* force = */ true);
                {
                    std::lock_guard lock{mutex};
                    out_of_horizon = nullptr;
                    state = State::Ready;
                }
                LOG_INFO(log, "Built the series index in {} seconds: {} time series, {} bytes",
                    build_watch.elapsedSeconds(), data->numSeries(), data->bytes());
                continue;
            }

            if (time_ranges_check.not_applied.empty() && tags_check.not_applied.empty())
            {
                all_applied = true;
                break;
            }

            if (max_rows && (time_ranges_check.not_applied_rows + tags_check.not_applied_rows > *max_rows))
                return false;

            /// New data parts appear all the time: the next call applies the next ones.
            if (num_passes_when_ready == 3)
                break;
            ++num_passes_when_ready;

            /// The "time ranges" table is listed first and applied last: the row with the tags of a time series is written before
            /// the row with its time range, so the time series with a time range already have their tags.
            if (!applyDataParts(*tables, /* is_tags_table = */ true, tags_check.not_applied, local_context, num_threads)
                || !applyDataParts(*tables, /* is_tags_table = */ false, time_ranges_check.not_applied, local_context, num_threads))
                break;
        }

        if (shutdown_called)
            return false;

        /// The background task calculates the size, a query uses the estimate.
        if (!max_rows)
            updatePostingsBytes(/* force = */ false);

        size_t num_series = 0;
        size_t bytes = 0;
        size_t num_unresolved = 0;
        {
            std::shared_lock lock{mutex};
            num_series = data->numSeries();
            bytes = data->bytes();
            num_unresolved = data->numUnresolved();
        }
        resetMetrics(num_series, bytes);

        const auto now = Clock::now();
        if (bytes > max_size_bytes)
        {
            LOG_WARNING(log, "The series index has {} time series and takes {} bytes, which is more than the limit of {} bytes. "
                "The index is dropped and will be built again in {} seconds",
                num_series, bytes, max_size_bytes, std::chrono::duration_cast<std::chrono::seconds>(TIME_TO_RETRY_AFTER_SIZE_LIMIT).count());
            reset(State::NotBuilt);
            size_limit_retry_time = now + TIME_TO_RETRY_AFTER_SIZE_LIMIT;
            return false;
        }

        if (!num_unresolved)
        {
            unresolved_since.reset();
        }
        else if (!unresolved_since)
        {
            unresolved_since = now;
        }
        else if (now - *unresolved_since > MAX_UNRESOLVED_TIME)
        {
            LOG_DEBUG(log, "{} time series have a time range and no tags in the series index, the index will be rebuilt", num_unresolved);
            reset(State::NotBuilt);
            return false;
        }

        return (state == State::Ready) && all_applied;
    }

    void runTask()
    {
        if (shutdown_called)
            return;

        bool idle = false;
        bool applied_now = false;
        try
        {
            std::unique_lock sync_lock{sync_mutex};
            idle = (nowSeconds() - last_lookup_seconds > std::chrono::duration_cast<std::chrono::seconds>(MAX_IDLE_TIME).count());
            if (idle)
            {
                LOG_DEBUG(log, "The series index is not used, dropping it");
                reset(State::NotBuilt);
                task_started = false;
            }
            else
            {
                applied_data_parts = false;
                synchronize({}, /* immediately = */ false);
                applied_now = applied_data_parts;
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, "While updating the series index");
        }

        if (!idle && !shutdown_called)
            task->scheduleAfter(applied_now ? POLL_INTERVAL_AFTER_APPLY_MS : POLL_INTERVAL_MS);
    }

    void ensureTaskStarted()
    {
        bool expected = false;
        if (task_started.compare_exchange_strong(expected, true))
            task->activateAndSchedule();
    }

    enum class LookupStatus
    {
        Found,
        /// The index cannot answer the query whatever data parts it applies.
        CannotAnswer,
        NotBuilt,
        /// The index is built, and the tables have data parts it has not applied.
        NotApplied,
        /// The index is built, and it must be rebuilt or it waits for the tags of some time series.
        Stale,
    };

    /// Returns the start of the time the index can hold now, in the scale of the timestamps of the table:
    /// the time series without samples in the "recent samples" table are not kept. Returns nothing if there is no such table.
    std::optional<Int64> getHorizonStart(const ContextPtr & local_context) const
    {
        auto time_series_settings = time_series_storage.getStorageSettings();
        const UInt64 recent_samples_ttl_seconds = (*time_series_settings)[TimeSeriesSetting::recent_samples_ttl_seconds];
        if (!recent_samples_ttl_seconds || !time_series_storage.hasTarget(ViewTarget::RecentSamples))
            return {};

        auto time_series_metadata = time_series_storage.getInMemoryMetadataPtr(local_context, false);
        auto [timestamp_type, value_type] = splitTimeSeriesType(
            time_series_metadata->columns.get(TimeSeriesColumnNames::getOuterSamples(time_series_storage.getVersion())).type);
        const UInt32 scale = tryGetDecimalScale(*timestamp_type).value_or(0);
        const Int64 horizon_seconds = static_cast<Int64>(std::time(nullptr)) - static_cast<Int64>(recent_samples_ttl_seconds);
        return horizon_seconds * DecimalUtils::scaleMultiplier<Int64>(scale);
    }

    /// Finds the time series if the index is built and has applied all the data parts visible to the query.
    /// `parts_context` is the context of the query with the setting `enable_shared_storage_snapshot_in_query` disabled.
    LookupStatus tryLookupImpl(
        const PrometheusQueryTree::MatcherList & matchers,
        std::optional<Int64> min_time,
        std::optional<Int64> max_time,
        const ContextPtr & parts_context,
        LookupResult & res,
        size_t & not_applied_rows)
    {
        not_applied_rows = 0;
        auto tables = tryGetTables(parts_context, /* for_building = */ false);
        if (!tables)
            return LookupStatus::CannotAnswer;

        /// A query doesn't wait for the lock while the index is built.
        if (state != State::Ready)
            return LookupStatus::NotBuilt;

        std::shared_lock lock{mutex};
        if (state != State::Ready)
            return LookupStatus::NotBuilt;

        /// The index holds only the time series with a time range after the horizon.
        if (horizon_start && (!min_time || (*min_time < *horizon_start)))
            return LookupStatus::CannotAnswer;

        /// The data parts are listed with the lock held, so the data parts the index knows were active before that:
        /// a known data part which is not in the list has been merged or removed.
        auto time_ranges_parts = getVisibleDataParts(tables->time_ranges_storage, tables->time_ranges_metadata, parts_context);
        auto tags_parts = getVisibleDataParts(tables->tags_storage, tables->tags_metadata, parts_context);
        auto time_ranges_check = checkTable(time_ranges_state, time_ranges_parts, tables->time_ranges_metadata);
        auto tags_check = checkTable(tags_state, tags_parts, tables->tags_metadata);
        if (time_ranges_check.changed || tags_check.changed)
            return LookupStatus::Stale;
        if (!time_ranges_check.not_applied.empty() || !tags_check.not_applied.empty())
        {
            not_applied_rows = time_ranges_check.not_applied_rows + tags_check.not_applied_rows;
            return LookupStatus::NotApplied;
        }
        if (data->numUnresolved())
            return LookupStatus::Stale;

        std::vector<PreparedMatcher> prepared_matchers;
        PaddedPODArray<Ordinal> ordinals;
        if (data->prepareMatchers(matchers, prepared_matchers))
            data->filterByTime(data->match(prepared_matchers), min_time, max_time, ordinals);
        res.ids = data->makeIDColumn(id_type, ordinals);
        res.tags = data->makeTags(ordinals, lookup_pool);
        return LookupStatus::Found;
    }
};


TimeSeriesSeriesIndex::TimeSeriesSeriesIndex(const StorageTimeSeries & time_series_storage_, const ContextPtr & global_context_, UInt64 max_size_bytes_)
    : impl(std::make_unique<Impl>(time_series_storage_, global_context_, max_size_bytes_))
{
}

TimeSeriesSeriesIndex::~TimeSeriesSeriesIndex() = default;

void TimeSeriesSeriesIndex::shutdown()
{
    impl->shutdown();
}

bool TimeSeriesSeriesIndex::prepareLookup(std::optional<Int64> min_time, const ContextPtr & query_context)
{
    if (impl->shutdown_called)
        return false;

    /// A query for a time range which the index never holds doesn't make the server build the index.
    if (auto horizon_start = impl->getHorizonStart(query_context); horizon_start && (!min_time || (*min_time < *horizon_start)))
        return false;

    impl->last_lookup_seconds = Impl::nowSeconds();
    impl->ensureTaskStarted();
    return impl->state == Impl::State::Ready;
}

std::optional<TimeSeriesSeriesIndex::LookupResult> TimeSeriesSeriesIndex::tryLookup(
    const PrometheusQueryTree::MatcherList & matchers,
    std::optional<Int64> min_time,
    std::optional<Int64> max_time,
    const ContextPtr & query_context,
    bool wait)
{
    if (impl->shutdown_called)
        return {};

    /// A query for a time range which the index never holds doesn't make the server build the index.
    if (auto horizon_start = impl->getHorizonStart(query_context); horizon_start && (!min_time || (*min_time < *horizon_start)))
        return {};

    impl->last_lookup_seconds = Impl::nowSeconds();
    impl->ensureTaskStarted();

    /// The data parts must be listed anew at every attempt.
    auto parts_context = Context::createCopy(query_context);
    parts_context->setSetting("enable_shared_storage_snapshot_in_query", false);

    /// The background task builds the index and applies the data parts. A query which doesn't wait applies only a few rows itself,
    /// and it reads the tables if the index is not built or is far behind them.
    const size_t max_lookups = wait ? 100 : MAX_LOOKUPS_IN_QUERY;
    for (size_t num_lookups = 1;; ++num_lookups)
    {
        LookupResult res;
        size_t not_applied_rows = 0;
        auto status = impl->tryLookupImpl(matchers, min_time, max_time, parts_context, res, not_applied_rows);
        if (status == Impl::LookupStatus::Found)
            return res;
        if ((status == Impl::LookupStatus::CannotAnswer) || impl->shutdown_called || (num_lookups == max_lookups))
            return {};

        if (wait)
        {
            std::unique_lock sync_lock{impl->sync_mutex};
            impl->synchronize({}, /* immediately = */ true);
            continue;
        }

        if (status != Impl::LookupStatus::NotApplied)
            return {};

        /// The index is behind the tables. The query brings it up to date if the new rows are few.
        /// If that takes time and doesn't help, the next queries don't try for a while.
        if (not_applied_rows > MAX_ROWS_TO_APPLY_IN_QUERY)
            return {};
        const Int64 now_ms = Impl::nowMilliseconds();
        if (now_ms < impl->no_wait_before_ms)
            return {};
        std::unique_lock sync_lock{impl->sync_mutex, std::defer_lock};
        if (!sync_lock.try_lock_for(MAX_WAIT_IN_QUERY) || !impl->synchronize(MAX_ROWS_TO_APPLY_IN_QUERY, /* immediately = */ false))
        {
            impl->no_wait_before_ms = now_ms + TIME_WITHOUT_WAIT_AFTER_MISS_MS;
            return {};
        }
    }
}

bool TimeSeriesSeriesIndex::contains(const ColumnPtr & id_column, PaddedPODArray<UInt8> & res) const
{
    res.clear();
    std::shared_lock lock{impl->mutex};
    if (impl->state != Impl::State::Ready)
        return false;

    auto full_column = recursiveRemoveLowCardinality(id_column->convertToFullColumnIfConst());
    const auto & layout = impl->data->getLayout();
    IDReader reader{*full_column, layout};
    char id[MAX_ID_SIZE];
    res.resize(full_column->size());
    for (size_t row = 0; row != res.size(); ++row)
    {
        reader.get(row, id);
        res[row] = (impl->data->find(id, SeriesIndexData::hashID(id, layout.size())) != NO_ORDINAL);
    }
    return true;
}

UInt64 TimeSeriesSeriesIndex::getEpoch() const
{
    std::shared_lock lock{impl->mutex};
    return impl->epoch;
}

bool TimeSeriesSeriesIndex::isMissExplained(const ColumnPtr & id_column, size_t row, std::optional<Int64> min_time, std::optional<Int64> max_time) const
{
    std::shared_lock lock{impl->mutex};
    if ((impl->state != Impl::State::Ready) || !impl->horizon_start || !max_time)
        return false;

    auto full_column = recursiveRemoveLowCardinality(id_column->convertToFullColumnIfConst());
    const auto & layout = impl->data->getLayout();
    IDReader reader{*full_column, layout};
    char id[MAX_ID_SIZE];
    reader.get(row, id);
    const Ordinal ordinal = impl->data->find(id, SeriesIndexData::hashID(id, layout.size()));
    if ((ordinal == NO_ORDINAL) || !impl->data->hasTags(ordinal))
        return false;

    /// Every row with a time range after the horizon is applied, so the maximum time is exact,
    /// and no applied row intersects the range if the minimum time is after it.
    return (!min_time || (impl->data->getMaxTime(ordinal) >= *min_time)) && (impl->data->getMinTime(ordinal) > *max_time);
}

}
