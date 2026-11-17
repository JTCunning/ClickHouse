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
#include <Core/ColumnWithTypeAndName.h>
#include <Core/DecimalFunctions.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/QueryMetadataCache.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/StorageValues.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesIDGenerator.h>
#include <Storages/TimeSeries/TimeSeriesIDRange.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <Storages/TimeSeries/splitTimeSeriesType.h>
#include <Storages/TimeSeries/tryGetLiteralAlternatives.h>

#include <base/scope_guard.h>
#include <base/unaligned.h>
#include <city.h>
#include <roaring/roaring.hh>

#include <array>
#include <chrono>
#include <condition_variable>
#include <ctime>
#include <map>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>


namespace ProfileEvents
{
    extern const Event TimeSeriesSeriesIndexAppliedParts;
    extern const Event TimeSeriesSeriesIndexAppliedRows;
    extern const Event TimeSeriesSeriesIndexApplyMicroseconds;
    extern const Event TimeSeriesSeriesIndexRebuilds;
    extern const Event TimeSeriesSeriesIndexResolveMicroseconds;
    extern const Event TimeSeriesSeriesIndexWaitMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric TimeSeriesSeriesIndexBytes;
    extern const Metric TimeSeriesSeriesIndexSeries;
    extern const Metric TimeSeriesSeriesIndexThreads;
    extern const Metric TimeSeriesSeriesIndexThreadsActive;
    extern const Metric TimeSeriesSeriesIndexThreadsScheduled;
    extern const Metric TimeSeriesSeriesIndexUnresolved;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsUInt64 time_series_series_index_max_wait_ms;
}

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsASTFunction id_generator;
    extern const TimeSeriesSettingsUInt64 recent_samples_ttl_seconds;
    extern const TimeSeriesSettingsUInt64 tags_deduplication_cache_expiration_seconds;
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
/// The index has not dropped any time series, and it has admitted every time series it has seen.
constexpr Int64 NO_WATERMARK = std::numeric_limits<Int64>::min();

/// The time series has a time range and no tags, and the index has not looked for its tags in the "tags" table yet:
/// the row with its tags could come before the index knew the time series.
constexpr UInt8 FLAG_UNRESOLVED = 1;
/// The index has dropped the time series. Its data stays until the shard is rebuilt.
constexpr UInt8 FLAG_DEAD = 2;
/// The time series was added after the index had dropped or had not admitted some time series,
/// so the index could have ignored its older rows: its minimum time can be any time up to the watermark.
constexpr UInt8 FLAG_MIN_TIME_UNKNOWN = 4;
/// The number of sweeps since the maximum time of the time series has grown, in three bits.
constexpr UInt8 AGE_SHIFT = 3;
constexpr UInt8 MAX_AGE = 7;
constexpr UInt8 AGE_MASK = MAX_AGE << AGE_SHIFT;
/// There are four sweeps per idle limit. With this age the time series has been idle for the limit or longer.
constexpr UInt8 AGE_TO_EVICT = 5;
constexpr size_t SWEEPS_PER_IDLE_LIMIT = 4;

/// The time series are divided into shards by the first component of the identifier, which is the hash of the metric name.
/// Each shard has its own lock, its own lists of time series and its own tag values.
constexpr size_t NUM_SHARDS_LOG2 = 3;
constexpr size_t NUM_SHARDS = 1 << NUM_SHARDS_LOG2;

/// The background task looks for new data parts with this period,
/// and with the shorter one for some time after it has applied some: the table gets inserts, so more of them come soon.
constexpr size_t POLL_INTERVAL_MS = 500;
constexpr size_t POLL_INTERVAL_AFTER_APPLY_MS = 20;
constexpr auto TIME_OF_SHORT_POLL_INTERVAL_AFTER_APPLY = std::chrono::seconds(2);
/// A query applies no more rows than this of the data parts it needs. The background task applies the other ones.
constexpr size_t MAX_ROWS_TO_APPLY_IN_QUERY = 500'000;
/// After a query has waited for the index in vain, the next queries don't wait for the index and don't apply
/// data parts for this time: they use the index only if it has applied everything.
constexpr Int64 TIME_WITHOUT_WAIT_AFTER_MISS_MS = 1000;
/// A matcher with a regular expression selects the time series from the lists of the values it accepts. To unite many lists
/// is slow, so with fewer candidates per list than this the candidates are checked instead.
constexpr size_t MAX_CANDIDATES_PER_LIST_TO_CHECK = 128;
/// The background task reads data parts with this number of threads. A query reads them with one thread.
constexpr size_t NUM_THREADS_TO_READ = 8;
/// The background task applies the data parts of the "time ranges" table with this number of threads if they have many rows:
/// each insert writes a row for every time series it has samples of, so this table gets most of the rows.
constexpr size_t NUM_THREADS_TO_APPLY = 4;
constexpr size_t MIN_ROWS_PER_THREAD_TO_APPLY = 100'000;
/// The tags of the time series found by a lookup are made by a few threads if there are many time series.
/// The lookups of one table share this number of threads.
constexpr size_t MAX_THREADS_TO_MAKE_TAGS = 16;
constexpr size_t MAX_THREADS_TO_MAKE_TAGS_IN_LOOKUP = 8;
constexpr size_t MIN_SERIES_PER_THREAD_TO_MAKE_TAGS = 4096;
/// One query to a target table reads no more rows than this, if it reads more than one data part.
/// It is also the number of rows the background task applies before it lists the data parts again.
constexpr size_t MAX_ROWS_IN_BATCH = 20'000'000;
/// The minimum time between two rebuilds caused by a change of the tables.
constexpr auto MIN_TIME_BETWEEN_REBUILDS = std::chrono::seconds(10);
/// The index is built again this long after it reached its size limit.
constexpr auto TIME_TO_RETRY_AFTER_SIZE_LIMIT = std::chrono::seconds(3600);
/// The index frees its memory if no query used it this long.
constexpr auto MAX_IDLE_TIME = std::chrono::seconds(3600);
/// The size of the lists of time series is calculated by a pass over all of them, which is slow. Between two passes
/// the size is estimated: a time series added to a list takes about this number of bytes.
constexpr size_t ESTIMATED_BYTES_PER_POSTING_ADD = 4;
/// A list of time series with more of them than `MAX_SIZE_OF_SMALL_LIST` is a bitmap, which keeps its data in several allocated pieces.
constexpr size_t ESTIMATED_OVERHEAD_OF_BITMAP = 96;
constexpr UInt32 MAX_SIZE_OF_SMALL_LIST = 32;
/// The size of the lists of time series is calculated again after this time, or earlier if the index has grown by 1/8.
constexpr auto POSTINGS_BYTES_CALCULATION_INTERVAL = std::chrono::seconds(600);

/// The index keeps the time series with samples newer than its horizon. The horizon starts at the retention
/// of the "recent samples" table. When the index takes more than this part of its size limit, the horizon gets shorter
/// by a quarter, but not shorter than this number of seconds or the idle limit.
constexpr double SIZE_RATIO_TO_SHORTEN_HORIZON = 0.9;
constexpr UInt64 MIN_HORIZON_SECONDS = 6 * 3600;
/// A shard is rebuilt from the time series it has not dropped when the dropped ones are more than this part of it,
/// or more than the second part when the index takes more than `SIZE_RATIO_TO_SHORTEN_HORIZON` of its size limit.
constexpr double MAX_DEAD_RATIO = 0.25;
constexpr double MAX_DEAD_RATIO_NEAR_SIZE_LIMIT = 1.0 / 16;
/// The time series are swept not more often than this.
constexpr Int64 MIN_SWEEP_INTERVAL_MS = 250;

/// The tags of a time series without a time range wait for its time range in a set which is replaced by an empty one
/// after this time or when it takes this number of bytes. A shard keeps the current set and the previous one.
constexpr Int64 PENDING_TAGS_ROTATION_INTERVAL_MS = 10'000;
constexpr size_t MAX_PENDING_TAGS_BYTES = 1 << 20;

/// The tags of a time series without tags are read from the "tags" table after this time: usually the data part
/// with the tags is applied earlier. The next attempts wait twice as long each, up to the maximum.
constexpr Int64 MIN_TIME_TO_RESOLVE_MS = 1000;
constexpr Int64 MAX_TIME_BETWEEN_ATTEMPTS_TO_RESOLVE_MS = 60'000;
constexpr size_t MAX_SERIES_TO_RESOLVE_IN_QUERY = 10'000;


/// The identifiers are kept as their native binary representation. The supported type is a tuple of UInt64
/// and a fixed-size value of 8 or 16 bytes, where the first component is calculated from the metric name alone.
struct IDLayout
{
    static constexpr size_t first_size = sizeof(UInt64);
    size_t second_size = 0;
    [[nodiscard]] size_t size() const { return first_size + second_size; }
};

constexpr size_t MAX_ID_SIZE = 24;

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
    const auto * tuple_type = typeid_cast<const DataTypeTuple *>(id_type.get());
    if (!tuple_type)
        return {};
    const auto & elements = tuple_type->getElements();
    if ((elements.size() != 2) || !WhichDataType{*elements[0]}.isUInt64())
        return {};
    size_t second_size = getIDComponentSize(*elements[1]);
    if (!second_size)
        return {};
    return IDLayout{.second_size = second_size};
}

UInt64 getFirstComponent(const char * id)
{
    return unalignedLoad<UInt64>(id);
}

size_t getShardNumber(UInt64 first_component)
{
    return intHash64(first_component) >> (64 - NUM_SHARDS_LOG2);
}

/// Reads identifiers from a full column of the identifier type.
class IDReader
{
public:
    IDReader(const IColumn & column, const IDLayout & layout_) : layout(layout_)
    {
        const auto & tuple = assert_cast<const ColumnTuple &>(column);
        first = tuple.getColumn(0).getRawData().data();
        second = tuple.getColumn(1).getRawData().data();
    }

    [[nodiscard]] UInt64 getFirstComponent(size_t row) const { return unalignedLoad<UInt64>(first + row * IDLayout::first_size); }

    void get(size_t row, char * dest) const
    {
        memcpy(dest, first + row * IDLayout::first_size, IDLayout::first_size);
        memcpy(dest + IDLayout::first_size, second + row * layout.second_size, layout.second_size);
    }

private:
    const IDLayout layout;
    const char * first = nullptr;
    const char * second = nullptr;
};


/// Reads times from a column of the "time ranges" table.
class TimeReader
{
public:
    explicit TimeReader(const IColumn & column_) : column(column_)
    {
        if (column.isFixedAndContiguous() && (column.sizeOfValueIfFixed() == sizeof(Int64)))
            data = column.getRawData().data();
    }

    [[nodiscard]] Int64 get(size_t row) const
    {
        return data ? unalignedLoad<Int64>(data + row * sizeof(Int64)) : column.getInt(row);
    }

private:
    const IColumn & column;
    const char * data = nullptr;
};


/// The arrays of a shard grow by 1/8 when they are full: an array which doubles keeps a half of its memory unused on average.
template <typename Array>
void reserveForMore(Array & array, size_t count, size_t min_growth)
{
    const size_t size = array.size() + count;
    if (size <= array.capacity())
        return;
    const size_t new_capacity = size + std::max(size / 8, min_growth);
    if constexpr (requires { array.reserve_exact(new_capacity); })
        array.reserve_exact(new_capacity);
    else
        array.reserve(new_capacity);
}


/// An open-addressing hash table from a key to a number. A cell has 8 bytes: 32 bits of the hash of the key and the number plus one.
/// The keys themselves are stored by the caller.
class NumberTable
{
public:
    explicit NumberTable(size_t initial_num_cells, size_t expected_count = 0)
    {
        size_t num_cells = initial_num_cells;
        while (num_cells * 3 < expected_count * 4 + 4)
            num_cells *= 2;
        cells.resize_exact(num_cells);
        memset(cells.data(), 0, cells.size() * sizeof(UInt64));
    }

    void prefetch(UInt64 hash) const { __builtin_prefetch(&cells[hash & (cells.size() - 1)]); }

    template <typename Equal>
    UInt32 find(UInt64 hash, Equal && equal) const
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
                const UInt32 number = static_cast<UInt32>(cell) - 1;
                if (equal(number))
                    return number;
            }
        }
    }

    /// Returns the first number which `find` would check, to prefetch its data.
    [[nodiscard]] UInt32 findCandidate(UInt64 hash) const
    {
        const size_t mask = cells.size() - 1;
        const UInt64 tag = hash >> 32;
        for (size_t pos = hash & mask;; pos = (pos + 1) & mask)
        {
            const UInt64 cell = cells[pos];
            if (!cell)
                return NO_ORDINAL;
            if ((cell >> 32) == tag)
                return static_cast<UInt32>(cell) - 1;
        }
    }

    template <typename GetHash>
    void insert(UInt64 hash, UInt32 number, GetHash && get_hash)
    {
        if ((count + 1) * 4 > cells.size() * 3)
        {
            PODArray<UInt64> old_cells;
            old_cells.swap(cells);
            cells.resize_exact(old_cells.size() * 2);
            memset(cells.data(), 0, cells.size() * sizeof(UInt64));
            for (UInt64 cell : old_cells)
            {
                if (cell)
                {
                    const UInt32 old_number = static_cast<UInt32>(cell) - 1;
                    place(get_hash(old_number), old_number);
                }
            }
        }
        place(hash, number);
        ++count;
    }

    [[nodiscard]] size_t allocatedBytes() const { return cells.allocated_bytes(); }

private:
    void place(UInt64 hash, UInt32 number)
    {
        const size_t mask = cells.size() - 1;
        size_t pos = hash & mask;
        while (cells[pos])
            pos = (pos + 1) & mask;
        cells[pos] = ((hash >> 32) << 32) | (static_cast<UInt64>(number) + 1);
    }

    PODArray<UInt64> cells;
    size_t count = 0;
};


/// The list of the time series with one pair of a tag name and a tag value: nothing, one time series,
/// a sorted array of a few of them, or a bitmap.
class Posting
{
public:
    Posting() = default;
    Posting(const Posting &) = delete;
    Posting & operator=(const Posting &) = delete;
    Posting & operator=(Posting &&) = delete;

    Posting(Posting && other) noexcept
        : size_or_single(std::exchange(other.size_or_single, NO_ORDINAL))
        , capacity(std::exchange(other.capacity, 0))
        , data(std::exchange(other.data, nullptr))
    {
    }

    ~Posting()
    {
        if (capacity == IS_BITMAP)
            delete bitmap();
        else if (capacity)
            delete[] list();
    }

    /// Adds sorted ordinals.
    void addMany(size_t count, const Ordinal * ordinals)
    {
        if (!count)
            return;
        if (capacity == IS_BITMAP)
        {
            bitmap()->addMany(count, ordinals);
            return;
        }

        const size_t old_size = capacity ? size_or_single : ((size_or_single == NO_ORDINAL) ? 0 : 1);
        if (!old_size && (count == 1))
        {
            size_or_single = ordinals[0];
            return;
        }

        if (old_size + count > MAX_SIZE_OF_SMALL_LIST)
        {
            auto new_bitmap = std::make_unique<roaring::Roaring>();
            if (capacity)
                new_bitmap->addMany(old_size, list());
            else if (old_size)
                new_bitmap->add(size_or_single);
            new_bitmap->addMany(count, ordinals);
            if (capacity)
                delete[] list();
            data = new_bitmap.release();
            capacity = IS_BITMAP;
            return;
        }

        if (old_size + count > capacity)
        {
            UInt32 new_capacity = 4;
            while (new_capacity < old_size + count)
                new_capacity *= 2;
            auto new_list = std::make_unique<Ordinal[]>(new_capacity);
            if (capacity)
            {
                memcpy(new_list.get(), list(), old_size * sizeof(Ordinal));
                delete[] list();
            }
            else if (old_size)
            {
                new_list[0] = size_or_single;
            }
            data = new_list.release();
            capacity = new_capacity;
        }

        /// The new ordinals are usually greater than the old ones. A time series which got its tags late has a smaller one.
        Ordinal * begin = list();
        memcpy(begin + old_size, ordinals, count * sizeof(Ordinal));
        size_t new_size = old_size + count;
        if (old_size && (begin[old_size] <= begin[old_size - 1]))
        {
            std::inplace_merge(begin, begin + old_size, begin + new_size);
            new_size = std::unique(begin, begin + new_size) - begin;
        }
        size_or_single = static_cast<UInt32>(new_size);
    }

    void unionInto(roaring::Roaring & dest) const
    {
        if (capacity == IS_BITMAP)
            dest |= *bitmap();
        else if (capacity)
            dest.addMany(size_or_single, list());
        else if (size_or_single != NO_ORDINAL)
            dest.add(size_or_single);
    }

    /// The size of a bitmap is the size of its serialized form plus an estimate for the separate allocations it consists of.
    [[nodiscard]] size_t allocatedBytes() const
    {
        if (capacity == IS_BITMAP)
            return sizeof(roaring::Roaring) + bitmap()->getSizeInBytes(false) + ESTIMATED_OVERHEAD_OF_BITMAP;
        return capacity * sizeof(Ordinal);
    }

private:
    static constexpr UInt32 IS_BITMAP = std::numeric_limits<UInt32>::max();

    [[nodiscard]] roaring::Roaring * bitmap() const { return static_cast<roaring::Roaring *>(data); }
    [[nodiscard]] Ordinal * list() const { return static_cast<Ordinal *>(data); }

    /// The only time series if `capacity` is zero, or the size of the array.
    UInt32 size_or_single = NO_ORDINAL;
    /// Zero, or the capacity of the array, or `IS_BITMAP`.
    UInt32 capacity = 0;
    void * data = nullptr;
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
    String label_name;
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

using PreparedMatchers = std::vector<PreparedMatcher>;

PreparedMatchers prepareMatchers(const PrometheusQueryTree::MatcherList & matchers)
{
    PreparedMatchers res;
    res.reserve(matchers.size());
    for (const auto & matcher : matchers)
    {
        PreparedMatcher prepared;
        prepared.label_name = matcher.label_name;
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
        res.push_back(std::move(prepared));
    }
    return res;
}


/// The time series of the metrics with one value of `getShardNumber`. It is not thread-safe.
class SeriesShard
{
public:
    using Tags = std::vector<std::pair<std::string_view, std::string_view>>;

    /// A matcher with the number of its tag name in this shard.
    struct BoundMatcher
    {
        const PreparedMatcher * matcher = nullptr;
        UInt32 name = NO_NAME;
    };

    struct UnresolvedSeries
    {
        /// The times in milliseconds of the steady clock.
        Int64 next_attempt_ms = 0;
        Int64 time_between_attempts_ms = 0;
    };

    explicit SeriesShard(const IDLayout & layout_, size_t expected_series = 0)
        : layout(layout_), id_size(layout_.size()), table(1 << 10, expected_series)
    {
        if (expected_series)
        {
            const size_t capacity = expected_series + expected_series / 16 + 1024;
            id_bytes.reserve_exact(capacity * id_size);
            times.reserve_exact(capacity * 2);
            label_offsets.reserve_exact(capacity);
            flags.reserve_exact(capacity);
        }
    }

    [[nodiscard]] size_t numOrdinals() const { return flags.size(); }
    [[nodiscard]] size_t numSeries() const { return flags.size() - num_dead; }
    [[nodiscard]] size_t numUnresolved() const { return unresolved.size(); }
    [[nodiscard]] Int64 getWatermark() const { return watermark; }

    static UInt64 hashID(const char * id, size_t size) { return CityHash_v1_0_2::CityHash64(id, size); }

    [[nodiscard]] Ordinal find(const char * id, UInt64 hash) const
    {
        return table.find(hash, [&](Ordinal ordinal) { return memcmp(getID(ordinal), id, id_size) == 0; });
    }

    [[nodiscard]] bool isDead(Ordinal ordinal) const { return (flags[ordinal] & FLAG_DEAD) != 0; }
    [[nodiscard]] bool hasTags(Ordinal ordinal) const { return label_offsets[ordinal] != NO_LABELS; }
    [[nodiscard]] Int64 getMinTime(Ordinal ordinal) const { return times[2 * static_cast<size_t>(ordinal)]; }
    [[nodiscard]] Int64 getMaxTime(Ordinal ordinal) const { return times[2 * static_cast<size_t>(ordinal) + 1]; }

    /// Whether the index can answer a query for the time series with a time range starting at `min_time`:
    /// the time series which the index has dropped or has not admitted have no samples after the watermark.
    [[nodiscard]] bool canAnswer(std::optional<Int64> min_time) const
    {
        return (watermark == NO_WATERMARK) || (min_time && (*min_time > watermark));
    }

    /// Applies rows with the whole time range of each time series, when the index is built: `rows` are the numbers of the rows
    /// of this shard. A time series with the maximum time before `horizon_start` is not admitted.
    /// A time series with the maximum time before `idle_start` is idle for the idle limit already.
    void applyWholeTimeRanges(
        const IDReader & reader, const TimeReader & min_times, const TimeReader & max_times, const PaddedPODArray<UInt32> & rows,
        std::optional<Int64> horizon_start, Int64 idle_start)
    {
        char id[MAX_ID_SIZE];
        for (UInt32 row : rows)
        {
            const Int64 min_time = min_times.get(row);
            const Int64 max_time = max_times.get(row);
            if (horizon_start && (max_time < *horizon_start))
            {
                watermark = std::max(watermark, max_time);
                continue;
            }
            reader.get(row, id);
            const UInt64 hash = hashID(id, id_size);
            Ordinal ordinal = find(id, hash);
            if (ordinal == NO_ORDINAL)
                ordinal = create(id, hash);
            Int64 * series_times = &times[2 * static_cast<size_t>(ordinal)];
            series_times[0] = std::min(series_times[0], min_time);
            series_times[1] = std::max(series_times[1], max_time);
            if (max_time < idle_start)
                flags[ordinal] = static_cast<UInt8>((flags[ordinal] & ~AGE_MASK) | (AGE_TO_EVICT << AGE_SHIFT));
        }
    }

    /// Applies rows of the "time ranges" table: `rows` are the numbers of the rows of this shard.
    /// A row of an unknown time series not newer than the watermark is ignored. A new time series gets its tags
    /// from the pending ones, or it is unresolved until a row with its tags comes. While the index is built (`building`),
    /// the tags are applied after all the time ranges, so a new time series just has no tags.
    void applyTimeRanges(
        const IDReader & reader, const TimeReader & min_times, const TimeReader & max_times, const PaddedPODArray<UInt32> & rows,
        bool building, Int64 now_ms)
    {
        /// The cell of the hash table, the identifier, the time range and the flags of a time series are far from each other
        /// in the memory, so they are prefetched for a batch of rows.
        constexpr size_t batch_size = 32;
        char ids[batch_size][MAX_ID_SIZE];
        UInt64 hashes[batch_size];

        const size_t num_rows = rows.size();
        for (size_t begin = 0; begin < num_rows; begin += batch_size)
        {
            const size_t count = std::min(batch_size, num_rows - begin);
            for (size_t i = 0; i != count; ++i)
            {
                reader.get(rows[begin + i], ids[i]);
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
                    __builtin_prefetch(&flags[candidate]);
                }
            }
            for (size_t i = 0; i != count; ++i)
            {
                const size_t row = rows[begin + i];
                const Int64 min_time = min_times.get(row);
                const Int64 max_time = max_times.get(row);
                const bool covered_by_watermark = (watermark != NO_WATERMARK) && (max_time <= watermark);
                Ordinal ordinal = find(ids[i], hashes[i]);
                bool is_new = false;
                if (ordinal == NO_ORDINAL)
                {
                    if (covered_by_watermark)
                        continue;
                    ordinal = create(ids[i], hashes[i]);
                    is_new = true;
                }
                else if (flags[ordinal] & FLAG_DEAD)
                {
                    /// The time series has returned before the shard was rebuilt: its tags are still here.
                    if (covered_by_watermark)
                        continue;
                    flags[ordinal] &= ~(FLAG_DEAD | AGE_MASK);
                    --num_dead;
                    is_new = true;
                }
                if (is_new)
                {
                    if (watermark != NO_WATERMARK)
                        flags[ordinal] |= FLAG_MIN_TIME_UNKNOWN;
                    if (!building && !hasTags(ordinal) && !takePendingTags(ordinal, ids[i]))
                        addUnresolved(ordinal, now_ms);
                }
                Int64 * series_times = &times[2 * static_cast<size_t>(ordinal)];
                series_times[0] = std::min(series_times[0], min_time);
                if (max_time > series_times[1])
                {
                    series_times[1] = max_time;
                    if (flags[ordinal] & AGE_MASK)
                        flags[ordinal] &= ~AGE_MASK;
                }
            }
        }
        flushPostingAdds();
    }

    /// Applies rows of the "tags" table: `tags_column` contains all the tags of a time series, including the metric name,
    /// and `rows` are the numbers of the rows of this shard. The tags of an unknown time series are not added to the index:
    /// they wait for its time range for a limited time, or they are skipped while the index is built (`building`).
    void applyTags(const IDReader & reader, const ColumnMap & tags_column, const PaddedPODArray<UInt32> & rows, bool building, Int64 now_ms)
    {
        const auto & array = tags_column.getNestedColumn();
        const auto & offsets = array.getOffsets();
        const auto & tuple = assert_cast<const ColumnTuple &>(array.getData());
        const auto & names_column = assert_cast<const ColumnString &>(tuple.getColumn(0));
        const auto & values_column = assert_cast<const ColumnString &>(tuple.getColumn(1));

        char id[MAX_ID_SIZE];
        for (UInt32 row : rows)
        {
            reader.get(row, id);
            const Ordinal ordinal = find(id, hashID(id, id_size));
            /// A dropped time series without tags is as an unknown one: its tags wait until it returns.
            const bool is_known = (ordinal != NO_ORDINAL) && !isDead(ordinal);
            if ((ordinal != NO_ORDINAL) ? hasTags(ordinal) : building)
                continue;

            tags_buffer.clear();
            for (size_t i = offsets[static_cast<ssize_t>(row) - 1]; i != offsets[row]; ++i)
                tags_buffer.emplace_back(names_column.getDataAt(i), values_column.getDataAt(i));

            if (is_known)
                setTags(ordinal, tags_buffer);
            else
                addPendingTags(id, tags_buffer, now_ms);
        }
        flushPostingAdds();
    }

    /// Drops the time series with the maximum time before `horizon_start` which are idle for the idle limit.
    /// With `increase_age` the function counts one more sweep for each time series. Returns the number of the dropped ones.
    size_t sweep(std::optional<Int64> horizon_start, bool increase_age)
    {
        size_t num_dropped = 0;
        for (Ordinal ordinal = 0; ordinal != flags.size(); ++ordinal)
        {
            UInt8 & series_flags = flags[ordinal];
            if (series_flags & FLAG_DEAD)
                continue;
            UInt8 age = (series_flags >> AGE_SHIFT) & MAX_AGE;
            if (increase_age && (age < MAX_AGE))
            {
                ++age;
                series_flags = static_cast<UInt8>((series_flags & ~AGE_MASK) | (age << AGE_SHIFT));
            }
            if (horizon_start && (age >= AGE_TO_EVICT) && (getMaxTime(ordinal) < *horizon_start))
            {
                if (series_flags & FLAG_UNRESOLVED)
                    removeUnresolved(ordinal);
                series_flags |= FLAG_DEAD;
                ++num_dead;
                watermark = std::max(watermark, getMaxTime(ordinal));
                ++num_dropped;
            }
        }
        return num_dropped;
    }

    /// Makes a shard with the time series which are not dropped. The pending tags stay in this shard, see `takePendingFrom`.
    [[nodiscard]] std::unique_ptr<SeriesShard> rebuild() const
    {
        auto res = std::make_unique<SeriesShard>(layout, numSeries());
        res->watermark = watermark;
        Tags tags;
        size_t num_series_with_tags = 0;
        for (Ordinal ordinal = 0; ordinal != flags.size(); ++ordinal)
        {
            if (flags[ordinal] & FLAG_DEAD)
                continue;
            const char * id = getID(ordinal);
            const Ordinal new_ordinal = res->create(id, hashID(id, id_size));
            res->times[2 * static_cast<size_t>(new_ordinal)] = getMinTime(ordinal);
            res->times[2 * static_cast<size_t>(new_ordinal) + 1] = getMaxTime(ordinal);
            res->flags[new_ordinal] = flags[ordinal];
            if (flags[ordinal] & FLAG_UNRESOLVED)
            {
                res->unresolved.emplace(new_ordinal, unresolved.at(ordinal));
                ++res->unresolved_by_first_component[getFirstComponent(id)];
            }
            if (hasTags(ordinal))
            {
                tags.clear();
                forEachPair(ordinal, [&](UInt32 pair) { tags.emplace_back(names[pair_name[pair]]->name, pair_value[pair]); });
                res->setTags(new_ordinal, tags);
                if (++num_series_with_tags % 65536 == 0)
                    res->flushPostingAdds();
            }
        }
        res->flushPostingAdds();
        res->setPostingsBytes(res->calculatePostingsBytes());
        return res;
    }

    void takePendingFrom(SeriesShard & other)
    {
        pending[0] = std::move(other.pending[0]);
        pending[1] = std::move(other.pending[1]);
        pending_rotation_time_ms = other.pending_rotation_time_ms;
    }

    /// Forgets the tags which wait for a time range for more than one rotation interval.
    void rotatePendingIfNeeded(Int64 now_ms)
    {
        if ((pending[0] || pending[1]) && (now_ms - pending_rotation_time_ms >= PENDING_TAGS_ROTATION_INTERVAL_MS))
            rotatePending(now_ms);
    }

    [[nodiscard]] bool hasUnresolvedOf(UInt64 first_component) const { return unresolved_by_first_component.contains(first_component); }

    template <typename F>
    void forEachFirstComponentOfUnresolved(F && f) const
    {
        for (const auto & [first_component, count] : unresolved_by_first_component)
            f(first_component);
    }

    /// Appends to `ids` the identifiers of the time series without tags whose tags should be read from the "tags" table now,
    /// and sets the time of the next attempt for them. With `all` the function takes every time series without tags.
    void takeUnresolvedToResolve(Int64 now_ms, bool all, size_t max_count, PODArray<char> & ids)
    {
        for (auto & [ordinal, series] : unresolved)
        {
            if (ids.size() >= max_count * id_size)
                break;
            if (!all && (now_ms < series.next_attempt_ms))
                continue;
            series.time_between_attempts_ms = std::min(series.time_between_attempts_ms * 2, MAX_TIME_BETWEEN_ATTEMPTS_TO_RESOLVE_MS);
            series.next_attempt_ms = now_ms + series.time_between_attempts_ms;
            const char * id = getID(ordinal);
            ids.insert(id, id + id_size);
        }
    }

    /// The "tags" table has no row for this time series: a row which comes later is applied as usual.
    void forgetUnresolved(const char * id)
    {
        const Ordinal ordinal = find(id, hashID(id, id_size));
        if ((ordinal != NO_ORDINAL) && (flags[ordinal] & FLAG_UNRESOLVED))
            removeUnresolved(ordinal);
    }

    /// Finds the tag names of the matchers in this shard.
    /// Returns false if no time series can match: a matcher requires a tag which no time series has.
    [[nodiscard]] bool bindMatchers(const PreparedMatchers & matchers, std::vector<BoundMatcher> & res) const
    {
        res.clear();
        res.reserve(matchers.size());
        for (const auto & matcher : matchers)
        {
            if (const auto * it = name_ids.find(std::string_view{matcher.label_name}))
                res.push_back(BoundMatcher{.matcher = &matcher, .name = it->getMapped()});
            else if (!matcher.matches_empty)
                return false;
        }
        return true;
    }

    /// Finds the time series with tags matching all the matchers.
    [[nodiscard]] roaring::Roaring match(const std::vector<BoundMatcher> & matchers) const
    {
        std::optional<roaring::Roaring> result;
        roaring::Roaring excluded;
        std::vector<const BoundMatcher *> scans;

        auto intersect = [&](roaring::Roaring && bitmap)
        {
            if (!result)
                result = std::move(bitmap);
            else
                *result &= bitmap;
        };

        /// A matcher which doesn't accept a time series without the tag selects the time series with the accepted values.
        /// A matcher which accepts such time series excludes the time series with the values it doesn't accept.
        for (const auto & bound : matchers)
        {
            const auto & matcher = *bound.matcher;
            const auto & label = *names[bound.name];
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
                scans.push_back(&bound);
            }
        }

        for (const auto * bound : scans)
        {
            const auto & matcher = *bound->matcher;
            const auto & label = *names[bound->name];

            /// With few candidates it is cheaper to check their values than all the values of the tag.
            if (result && (result->cardinality() * 8 < label.pairs.size()))
            {
                filterCandidates(*result, *bound);
                continue;
            }

            /// Select the values the matcher accepts if it selects, or the values it doesn't accept if it excludes.
            const bool select_accepted = !matcher.matches_empty;
            std::vector<UInt32> selected_pairs;
            for (UInt32 pair : label.pairs)
            {
                if (matcher.matchesValue(pair_value[pair]) == select_accepted)
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
            if (matcher.matches_empty)
                excluded |= bitmap;
            else
                intersect(std::move(bitmap));
        }

        if (!result)
            result = tagged;
        *result -= excluded;
        return std::move(*result);
    }

    /// Leaves in `ordinals` the time series from `bitmap` which are not dropped and have a time range intersecting [min_time, max_time].
    void filterByTime(const roaring::Roaring & bitmap, std::optional<Int64> min_time, std::optional<Int64> max_time, PaddedPODArray<Ordinal> & ordinals) const
    {
        ordinals.resize(bitmap.cardinality());
        bitmap.toUint32Array(ordinals.data());
        size_t num_selected = 0;
        for (Ordinal ordinal : ordinals)
        {
            const UInt8 series_flags = flags[ordinal];
            if (series_flags & FLAG_DEAD)
                continue;
            if (min_time && (getMaxTime(ordinal) < *min_time))
                continue;
            if (max_time && !(series_flags & FLAG_MIN_TIME_UNKNOWN) && (getMinTime(ordinal) > *max_time))
                continue;
            ordinals[num_selected++] = ordinal;
        }
        ordinals.resize(num_selected);
    }

    void appendIDs(const PaddedPODArray<Ordinal> & ordinals, IColumn & first_column, IColumn & second_column) const
    {
        for (Ordinal ordinal : ordinals)
        {
            const char * id = getID(ordinal);
            first_column.insertData(id, IDLayout::first_size);
            second_column.insertData(id + IDLayout::first_size, layout.second_size);
        }
    }

    /// Appends the tags of time series to `res`. `pool` is used if the time series are many.
    void appendTags(const PaddedPODArray<Ordinal> & ordinals, ThreadPool & pool, VectorWithMemoryTracking<TimeSeriesSeriesIndex::TagNamesAndValuesPtr> & res) const
    {
        const size_t offset = res.size();
        res.resize(offset + ordinals.size());
        const size_t num_jobs = std::min(MAX_THREADS_TO_MAKE_TAGS_IN_LOOKUP, ordinals.size() / MIN_SERIES_PER_THREAD_TO_MAKE_TAGS);
        if (num_jobs <= 1)
        {
            makeTagsOfRange(ordinals, 0, ordinals.size(), res, offset);
            return;
        }

        /// The jobs use only what is declared before the runner, which waits for them.
        const auto * ordinals_ptr = &ordinals;
        auto * res_ptr = &res;
        ThreadPoolCallbackRunnerLocal<void> runner(pool, ThreadName::DEFAULT_THREAD_POOL);
        for (size_t job = 0; job != num_jobs; ++job)
        {
            const size_t begin = ordinals.size() * job / num_jobs;
            const size_t end = ordinals.size() * (job + 1) / num_jobs;
            runner.enqueueAndKeepTrack(
                [this, ordinals_ptr, res_ptr, begin, end, offset] { makeTagsOfRange(*ordinals_ptr, begin, end, *res_ptr, offset); });
        }
        runner.waitForAllToFinishAndRethrowFirstError();
    }

    /// The size of the allocated memory in bytes. The size of the lists of time series is the one set by `setPostingsBytes`
    /// plus an estimate for the time series added after that.
    [[nodiscard]] size_t bytes() const
    {
        size_t res = id_bytes.allocated_bytes() + table.allocatedBytes() + times.allocated_bytes() + label_offsets.allocated_bytes()
            + label_bytes.allocated_bytes() + flags.allocated_bytes() + arena.allocatedBytes()
            + pair_name.capacity() * sizeof(UInt32) + pair_value.capacity() * sizeof(std::string_view) + postings.capacity() * sizeof(Posting)
            + name_ids.getBufferSizeInBytes() + postings_bytes + posting_adds_since_calculation * ESTIMATED_BYTES_PER_POSTING_ADD
            + unresolved.size() * BYTES_PER_UNRESOLVED;
        for (const auto & label : names)
            res += sizeof(LabelName) + label->pair_by_value.allocatedBytes() + label->pairs.capacity() * sizeof(UInt32);
        for (const auto & generation : pending)
        {
            if (generation)
                res += generation->bytes();
        }
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
        NumberTable pair_by_value{8};
        std::vector<UInt32> pairs;
        /// All the time series with the tag.
        Posting series;
    };

    /// The tags which wait for the time range of their time series: the identifier -> the tags written by `addPendingTags`.
    struct PendingTags
    {
        Arena arena{4096, 2, 1 << 16};
        HashMapWithSavedHash<std::string_view, std::string_view> tags_by_id;
        [[nodiscard]] size_t bytes() const { return arena.allocatedBytes() + tags_by_id.getBufferSizeInBytes(); }
    };

    /// An estimate for an element of `unresolved` and its part of `unresolved_by_first_component`.
    static constexpr size_t BYTES_PER_UNRESOLVED = 96;

    [[nodiscard]] const char * getID(Ordinal ordinal) const { return id_bytes.data() + static_cast<size_t>(ordinal) * id_size; }

    static UInt64 hashValue(std::string_view value) { return CityHash_v1_0_2::CityHash64(value.data(), value.size()); }

    Ordinal create(const char * id, UInt64 hash)
    {
        if (flags.size() >= NO_ORDINAL)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Too many time series in the series index of a TimeSeries table");
        const Ordinal ordinal = static_cast<Ordinal>(flags.size());
        reserveForMore(id_bytes, id_size, 1024 * id_size);
        reserveForMore(times, 2, 2048);
        reserveForMore(label_offsets, 1, 1024);
        reserveForMore(flags, 1, 1024);
        id_bytes.insert(id, id + id_size);
        times.push_back(NO_MIN_TIME);
        times.push_back(NO_MAX_TIME);
        label_offsets.push_back(NO_LABELS);
        flags.push_back(UInt8{0});
        table.insert(hash, ordinal, [&](Ordinal other) { return hashID(getID(other), id_size); });
        return ordinal;
    }

    void addUnresolved(Ordinal ordinal, Int64 now_ms)
    {
        flags[ordinal] |= FLAG_UNRESOLVED;
        unresolved.emplace(
            ordinal, UnresolvedSeries{.next_attempt_ms = now_ms + MIN_TIME_TO_RESOLVE_MS, .time_between_attempts_ms = MIN_TIME_TO_RESOLVE_MS / 2});
        ++unresolved_by_first_component[getFirstComponent(getID(ordinal))];
    }

    void removeUnresolved(Ordinal ordinal)
    {
        flags[ordinal] &= ~FLAG_UNRESOLVED;
        unresolved.erase(ordinal);
        auto it = unresolved_by_first_component.find(getFirstComponent(getID(ordinal)));
        if (--it->second == 0)
            unresolved_by_first_component.erase(it);
    }

    /// Sets the tags of a time series which has none. The lists of time series are updated by `flushPostingAdds`.
    void setTags(Ordinal ordinal, const Tags & tags)
    {
        pairs_buffer.clear();
        for (const auto & [name, value] : tags)
            pairs_buffer.push_back(internPair(name, value));
        std::sort(pairs_buffer.begin(), pairs_buffer.end());

        /// A number takes five bytes at most.
        reserveForMore(label_bytes, 5 * (pairs_buffer.size() + 1), 16384);
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
            removeUnresolved(ordinal);
    }

    /// The lists of time series are many and they are far from each other in the memory. The additions to them are collected
    /// for a block of rows and sorted, so each list is updated once per block.
    void flushPostingAdds()
    {
        if (tagged_adds.empty())
            return;
        addToPostings(pair_adds, [&](UInt32 pair) -> Posting & { return postings[pair]; });
        addToPostings(name_adds, [&](UInt32 name) -> Posting & { return names[name]->series; });
        std::sort(tagged_adds.begin(), tagged_adds.end());
        tagged.addMany(tagged_adds.size(), tagged_adds.data());
        posting_adds_since_calculation += pair_adds.size() + name_adds.size();
        pair_adds.clear();
        name_adds.clear();
        tagged_adds.clear();
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

    void rotatePending(Int64 now_ms)
    {
        pending[1] = std::move(pending[0]);
        pending[0] = nullptr;
        pending_rotation_time_ms = now_ms;
    }

    void addPendingTags(const char * id, const Tags & tags, Int64 now_ms)
    {
        const std::string_view key{id, id_size};
        if (pending[0] && pending[0]->tags_by_id.has(key))
            return;
        if (pending[0] && (pending[0]->bytes() > MAX_PENDING_TAGS_BYTES))
            rotatePending(now_ms);
        if (!pending[0])
        {
            pending[0] = std::make_unique<PendingTags>();
            if (!pending[1])
                pending_rotation_time_ms = now_ms;
        }

        pending_buffer.clear();
        writeVarUIntTo(pending_buffer, tags.size());
        for (const auto & [name, value] : tags)
        {
            for (std::string_view string : {name, value})
            {
                writeVarUIntTo(pending_buffer, string.size());
                pending_buffer.insert(string.data(), string.data() + string.size());
            }
        }
        auto & generation = *pending[0];
        const std::string_view stored_key{generation.arena.insert(id, id_size), id_size};
        const char * stored_tags = generation.arena.insert(reinterpret_cast<const char *>(pending_buffer.data()), pending_buffer.size());
        generation.tags_by_id[stored_key] = std::string_view{stored_tags, pending_buffer.size()};
    }

    /// Sets the tags of a new time series if they have come before its time range.
    bool takePendingTags(Ordinal ordinal, const char * id)
    {
        const std::string_view key{id, id_size};
        for (const auto & generation : pending)
        {
            if (!generation)
                continue;
            const auto * it = generation->tags_by_id.find(key);
            if (!it)
                continue;
            const auto * pos = reinterpret_cast<const UInt8 *>(it->getMapped().data());
            const UInt64 count = readVarUIntFrom(pos);
            tags_buffer.clear();
            for (UInt64 i = 0; i != count; ++i)
            {
                std::string_view strings[2];
                for (auto & string : strings)
                {
                    const UInt64 size = readVarUIntFrom(pos);
                    string = std::string_view{reinterpret_cast<const char *>(pos), size};
                    pos += size;
                }
                tags_buffer.emplace_back(strings[0], strings[1]);
            }
            setTags(ordinal, tags_buffer);
            return true;
        }
        return false;
    }

    void makeTagsOfRange(
        const PaddedPODArray<Ordinal> & ordinals, size_t begin, size_t end,
        VectorWithMemoryTracking<TimeSeriesSeriesIndex::TagNamesAndValuesPtr> & res, size_t offset) const
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
            res[offset + i] = std::move(tags);
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
        const UInt64 hash = hashValue(value);
        const UInt32 found = label.pair_by_value.find(hash, [&](UInt32 pair) { return pair_value[pair] == value; });
        if (found != NO_ORDINAL)
            return found;

        const UInt32 pair = static_cast<UInt32>(pair_name.size());
        const std::string_view stored_value{arena.insert(value.data(), value.size()), value.size()};
        reserveForMore(pair_name, 1, 256);
        reserveForMore(pair_value, 1, 256);
        reserveForMore(postings, 1, 256);
        reserveForMore(label.pairs, 1, 16);
        pair_name.push_back(name_id);
        pair_value.push_back(stored_value);
        postings.emplace_back();
        label.pairs.push_back(pair);
        label.pair_by_value.insert(hash, pair, [&](UInt32 other) { return hashValue(pair_value[other]); });
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
        const UInt32 pair = label.pair_by_value.find(hashValue(value), [&](UInt32 other) { return pair_value[other] == value; });
        if (pair != NO_ORDINAL)
            postings[pair].unionInto(dest);
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
    void filterCandidates(roaring::Roaring & candidates, const BoundMatcher & bound) const
    {
        /// Whether the matcher accepts the value of a pair: 1 if it accepts, 0 if not.
        std::unordered_map<UInt32, bool> accepted_by_pair;
        roaring::Roaring rejected;
        for (Ordinal ordinal : candidates)
        {
            UInt32 found_pair = NO_NAME;
            forEachPair(ordinal, [&](UInt32 pair)
            {
                if (pair_name[pair] == bound.name)
                    found_pair = pair;
            });
            bool accepted = false;
            if (found_pair == NO_NAME)
            {
                accepted = bound.matcher->matches_empty;
            }
            else
            {
                auto [it, inserted] = accepted_by_pair.try_emplace(found_pair, false);
                if (inserted)
                    it->second = bound.matcher->matchesValue(pair_value[found_pair]);
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
    NumberTable table;
    /// The minimum and the maximum time of each time series.
    PODArray<Int64> times;
    /// The tags of each time series: the number of pairs and the sorted numbers of the pairs,
    /// each written as the difference with the previous one in the variable-length format.
    PODArray<UInt64> label_offsets;
    PODArray<UInt8> label_bytes;
    PODArray<UInt8> flags;
    /// The time series with tags.
    roaring::Roaring tagged;
    size_t num_dead = 0;
    /// The newest maximum time of the time series which the shard has dropped or has not admitted.
    Int64 watermark = NO_WATERMARK;

    /// The time series with `FLAG_UNRESOLVED`, and the number of them for each first component of the identifier.
    std::unordered_map<Ordinal, UnresolvedSeries> unresolved;
    std::unordered_map<UInt64, size_t> unresolved_by_first_component;

    /// The current set of pending tags and the previous one.
    std::unique_ptr<PendingTags> pending[2];
    Int64 pending_rotation_time_ms = 0;

    /// The tag names and the tag values. A piece of memory for them is not bigger than 1 MiB.
    Arena arena{4096, 2, 1 << 20};
    std::vector<std::unique_ptr<LabelName>> names;
    HashMapWithSavedHash<std::string_view, UInt32> name_ids;
    /// The pairs of a tag name and a tag value.
    std::vector<UInt32> pair_name;
    std::vector<std::string_view> pair_value;
    std::vector<Posting> postings;
    size_t postings_bytes = 0;
    size_t posting_adds_since_calculation = 0;

    /// Used while rows are applied.
    Tags tags_buffer;
    std::vector<UInt32> pairs_buffer;
    PODArray<UInt64> pair_adds;
    PODArray<UInt64> name_adds;
    PODArray<Ordinal> tagged_adds;
    PODArray<Ordinal> ordinals_buffer;
    PODArray<UInt8> pending_buffer;
};


/// The metric names of the time series the index has seen, and the first components of their identifiers.
/// The first component is calculated from the metric name alone, so a selector of metrics reads only the shards of these metrics.
/// The names stay here after the index has dropped the time series: the tags of a time series which has returned
/// are read from the "tags" table by the metric name.
class MetricNames
{
public:
    /// Remembers the metric name of each row of a block of the "tags" table.
    void add(const IDReader & reader, const ColumnString & metric_name_column)
    {
        const size_t num_rows = metric_name_column.size();
        UInt64 last_first_component = 0;
        std::string_view last_name;
        for (size_t row = 0; row != num_rows; ++row)
        {
            const UInt64 first_component = reader.getFirstComponent(row);
            const std::string_view name = metric_name_column.getDataAt(row);
            if (row && (first_component == last_first_component) && (name == last_name))
                continue;
            last_first_component = first_component;
            last_name = name;
            addOne(name, first_component);
        }
    }

    /// Returns the first components of the identifiers of the metrics selected by the matchers, or nothing if the matchers
    /// don't select metrics by name.
    [[nodiscard]] std::optional<std::vector<UInt64>> select(const PreparedMatchers & matchers) const
    {
        const PreparedMatcher * name_matcher = nullptr;
        for (const auto & matcher : matchers)
        {
            if ((matcher.label_name == TimeSeriesTagNames::MetricName) && !matcher.negated && !matcher.matches_empty)
            {
                name_matcher = &matcher;
                break;
            }
        }
        if (!name_matcher)
            return {};

        std::vector<UInt64> res;
        std::shared_lock lock{mutex};
        auto add_name = [&](const String & name)
        {
            if (auto it = first_components_by_name.find(name); it != first_components_by_name.end())
                res.insert(res.end(), it->second.begin(), it->second.end());
        };
        if (!name_matcher->is_regexp)
        {
            add_name(name_matcher->value);
        }
        else if (name_matcher->alternatives)
        {
            for (const auto & alternative : *name_matcher->alternatives)
                add_name(alternative);
        }
        else
        {
            for (const auto & [name, first_components] : first_components_by_name)
            {
                if (name_matcher->matchesValue(name))
                    res.insert(res.end(), first_components.begin(), first_components.end());
            }
        }
        std::sort(res.begin(), res.end());
        res.erase(std::unique(res.begin(), res.end()), res.end());
        return res;
    }

    [[nodiscard]] bool has(UInt64 first_component) const
    {
        std::shared_lock lock{mutex};
        return names_by_first_component.contains(first_component);
    }

    /// Appends the metric names with this first component of the identifier. Returns false if there are none.
    bool get(UInt64 first_component, std::set<String> & names) const
    {
        std::shared_lock lock{mutex};
        auto it = names_by_first_component.find(first_component);
        if (it == names_by_first_component.end())
            return false;
        names.insert(it->second.begin(), it->second.end());
        return true;
    }

    [[nodiscard]] size_t bytes() const { return allocated_bytes.load(std::memory_order_relaxed); }

private:
    void addOne(std::string_view name, UInt64 first_component)
    {
        {
            std::shared_lock lock{mutex};
            if (auto it = names_by_first_component.find(first_component); it != names_by_first_component.end())
            {
                if (std::find(it->second.begin(), it->second.end(), name) != it->second.end())
                    return;
            }
        }
        std::lock_guard lock{mutex};
        auto & names = names_by_first_component[first_component];
        if (std::find(names.begin(), names.end(), name) != names.end())
            return;
        names.emplace_back(name);
        first_components_by_name[String{name}].push_back(first_component);
        allocated_bytes += 2 * (name.size() + BYTES_PER_NAME);
    }

    /// An estimate for an element of a map without its string.
    static constexpr size_t BYTES_PER_NAME = 96;

    mutable SharedMutex mutex;
    std::unordered_map<String, std::vector<UInt64>> first_components_by_name;
    std::unordered_map<UInt64, Strings> names_by_first_component;
    std::atomic<size_t> allocated_bytes = 0;
};


/// A shard with its lock, and the numbers which are read without the lock.
struct Shard
{
    mutable SharedMutex mutex;
    std::unique_ptr<SeriesShard> series;

    std::atomic<size_t> bytes = 0;
    std::atomic<size_t> num_series = 0;
    std::atomic<size_t> num_ordinals = 0;
    std::atomic<size_t> num_unresolved = 0;
    std::atomic<Int64> watermark = NO_WATERMARK;

    /// Requires the lock.
    void updateNumbers()
    {
        bytes = series->bytes();
        num_series = series->numSeries();
        num_ordinals = series->numOrdinals();
        num_unresolved = series->numUnresolved();
        watermark = series->getWatermark();
    }
};


/// The data of the index.
struct SeriesIndexData
{
    explicit SeriesIndexData(const IDLayout & layout_) : layout(layout_)
    {
        for (auto & shard : shards)
        {
            shard.series = std::make_unique<SeriesShard>(layout);
            shard.updateNumbers();
        }
    }

    const IDLayout layout;
    std::array<Shard, NUM_SHARDS> shards;
    MetricNames metric_names;

    [[nodiscard]] size_t bytes() const
    {
        size_t res = metric_names.bytes();
        for (const auto & shard : shards)
            res += shard.bytes.load(std::memory_order_relaxed);
        return res;
    }

    [[nodiscard]] size_t numSeries() const
    {
        size_t res = 0;
        for (const auto & shard : shards)
            res += shard.num_series.load(std::memory_order_relaxed);
        return res;
    }

    [[nodiscard]] size_t numUnresolved() const
    {
        size_t res = 0;
        for (const auto & shard : shards)
            res += shard.num_unresolved.load(std::memory_order_relaxed);
        return res;
    }

    /// The oldest watermark of the shards, or nothing if a shard has none: a query for an older time cannot be answered.
    [[nodiscard]] Int64 getMinWatermark() const
    {
        Int64 res = std::numeric_limits<Int64>::max();
        for (const auto & shard : shards)
            res = std::min(res, shard.watermark.load(std::memory_order_relaxed));
        return res;
    }

    /// Divides the rows of a block between the shards.
    void splitRows(const IDReader & reader, size_t num_rows, std::array<PaddedPODArray<UInt32>, NUM_SHARDS> & rows_by_shard) const
    {
        for (auto & rows : rows_by_shard)
            rows.clear();
        UInt64 last_first_component = 0;
        size_t last_shard = 0;
        for (size_t row = 0; row != num_rows; ++row)
        {
            const UInt64 first_component = reader.getFirstComponent(row);
            if (!row || (first_component != last_first_component))
            {
                last_first_component = first_component;
                last_shard = getShardNumber(first_component);
            }
            rows_by_shard[last_shard].push_back(static_cast<UInt32>(row));
        }
    }
};


/// The ranges of block numbers of one target table which the index has applied.
class AppliedRanges
{
    struct Range
    {
        Int64 max_block = 0;
        Int64 mutation = 0;
        /// The number of rows the index has applied from these blocks.
        size_t rows = 0;
    };

    using Ranges = std::map<Int64, Range>;

    /// Returns the first range which has blocks of the data part, or the first range after its blocks.
    template <typename RangesType>
    static auto firstIntersecting(RangesType & ranges, const MergeTreePartInfo & info)
    {
        auto it = ranges.upper_bound(info.min_block);
        if (it != ranges.begin())
        {
            auto previous = std::prev(it);
            if (previous->second.max_block >= info.min_block)
                it = previous;
        }
        return it;
    }

public:
    enum class Status
    {
        Applied,
        NotApplied,
        /// The index has applied rows of these blocks, and the data part does not have them as they were:
        /// a mutation changed them, or the data part is an empty one put over removed data parts.
        Changed,
    };

    /// `rows` is the number of rows of the data part.
    [[nodiscard]] Status check(const MergeTreePartInfo & info, size_t rows) const
    {
        auto partition_it = partitions.find(info.getPartitionId());
        if (partition_it == partitions.end())
            return Status::NotApplied;
        const auto & ranges = partition_it->second;

        Status res = Status::NotApplied;
        for (auto it = firstIntersecting(ranges, info); (it != ranges.end()) && (it->first <= info.max_block); ++it)
        {
            const auto & range = it->second;
            if (!sameData(it->first, range.mutation, info.mutation))
                return Status::Changed;
            /// `TRUNCATE` and `DROP PARTITION` of a `MergeTree` table put empty data parts with the same blocks over the removed ones.
            if (!rows && range.rows)
                return Status::Changed;
            if ((it->first <= info.min_block) && (range.max_block >= info.max_block))
                res = Status::Applied;
        }
        return res;
    }

    /// Whether the index has applied rows of some blocks of the data part.
    [[nodiscard]] bool hasRowsOf(const MergeTreePartInfo & info) const
    {
        auto partition_it = partitions.find(info.getPartitionId());
        if (partition_it == partitions.end())
            return false;
        const auto & ranges = partition_it->second;
        for (auto it = firstIntersecting(ranges, info); (it != ranges.end()) && (it->first <= info.max_block); ++it)
        {
            if (it->second.rows)
                return true;
        }
        return false;
    }

    /// Adds the blocks of a data part which was applied as a whole. `rows` is the number of the applied rows.
    void add(const MergeTreePartInfo & info, size_t rows)
    {
        auto & ranges = partitions[info.getPartitionId()];
        Int64 min_block = info.min_block;
        Int64 max_block = info.max_block;
        size_t total_rows = rows;

        /// The data part replaces what it covers. A range which is covered only in part was joined from adjacent data parts:
        /// the rest of it stays.
        std::vector<std::pair<Int64, Range>> rest;
        for (auto it = firstIntersecting(ranges, info); (it != ranges.end()) && (it->first <= max_block);)
        {
            const Int64 first = it->first;
            const Range range = it->second;
            it = ranges.erase(it);
            if (first < min_block)
                rest.emplace_back(first, Range{.max_block = min_block - 1, .mutation = range.mutation, .rows = range.rows});
            if (range.max_block > max_block)
                rest.emplace_back(max_block + 1, Range{.max_block = range.max_block, .mutation = range.mutation, .rows = range.rows});
        }
        for (const auto & [first, range] : rest)
            ranges[first] = range;

        /// The data part is joined with an adjacent range with the same mutation. A range without rows and a range with rows
        /// stay separate: see `check`.
        auto can_join = [&](const Range & range) { return (range.mutation == info.mutation) && (!range.rows == !rows); };
        if (auto next = ranges.find(max_block + 1); (next != ranges.end()) && can_join(next->second))
        {
            max_block = next->second.max_block;
            total_rows += next->second.rows;
            ranges.erase(next);
        }
        if (auto next = ranges.lower_bound(min_block); next != ranges.begin())
        {
            auto previous = std::prev(next);
            if ((previous->second.max_block + 1 == min_block) && can_join(previous->second))
            {
                min_block = previous->first;
                total_rows += previous->second.rows;
                ranges.erase(previous);
            }
        }
        ranges[min_block] = Range{.max_block = max_block, .mutation = info.mutation, .rows = total_rows};
    }

    void clear() { partitions.clear(); }

private:
    /// A mutation doesn't change the blocks written after it, so two mutation versions older than a block give the same data.
    static bool sameData(Int64 min_block, Int64 applied_mutation, Int64 mutation)
    {
        return (applied_mutation == mutation) || ((applied_mutation < min_block) && (mutation < min_block));
    }

    /// Partition identifier -> the first block of a range -> the range.
    std::unordered_map<String, Ranges> partitions;
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
    /// The active data parts at the last synchronization which have blocks with applied rows.
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
    /// The expressions selected from the "tags" table: the identifier, the map of all the tags, and the metric name.
    String tags_select_list;
};

using DataPartsVector = MergeTreeData::DataPartsVector;

/// The active data parts of a table at some moment. The snapshot holds them, so they can be read after a merge has replaced them.
struct PartsList
{
    StorageSnapshotPtr snapshot;
    DataPartsVector parts;
};

/// With the context of a query and the setting `enable_shared_storage_snapshot_in_query` the function returns
/// the snapshot of the query. With this setting disabled each call sees the current data parts.
PartsList listDataParts(const StoragePtr & storage, const StorageMetadataPtr & metadata, const ContextPtr & context)
{
    PartsList res;
    res.snapshot = storage->getStorageSnapshot(metadata, context);
    const auto & snapshot_data = assert_cast<const MergeTreeData::SnapshotData &>(*res.snapshot->data);
    if (snapshot_data.parts)
    {
        res.parts.reserve(snapshot_data.parts->size());
        for (const auto & part : *snapshot_data.parts)
            res.parts.push_back(part.data_part);
    }
    return res;
}

void sortByRows(DataPartsVector & parts)
{
    std::stable_sort(parts.begin(), parts.end(),
        [](const MergeTreeData::DataPartPtr & left, const MergeTreeData::DataPartPtr & right) { return left->rows_count < right->rows_count; });
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
        resetMetrics(0, 0, 0);
    }

    void shutdown()
    {
        shutdown_called = true;
        notifyChange();
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

    /// Applies the data parts of the "time ranges" table for the background task.
    ThreadPool apply_pool{
        CurrentMetrics::TimeSeriesSeriesIndexThreads, CurrentMetrics::TimeSeriesSeriesIndexThreadsActive,
        CurrentMetrics::TimeSeriesSeriesIndexThreadsScheduled,
        NUM_THREADS_TO_APPLY, /* max_free_threads = */ 0, /* queue_size = */ 0};

    /// Held by the thread which brings the whole index up to date: the background task, or a query which checks the index.
    /// Only this thread builds the index, drops it, drops time series and rebuilds shards.
    /// Other queries apply the data parts they need without this mutex.
    std::mutex follower_mutex;

    /// Held in the shared mode while data parts are applied, and in the exclusive mode while the index is dropped
    /// or a shard is rebuilt.
    SharedMutex maintenance_mutex;

    /// These two are changed with `mutex` held in the exclusive mode.
    std::atomic<State> state = State::NotBuilt;
    std::atomic<UInt64> epoch = 0;

    /// Protects the pointer to the data and what the index knows about the tables. The shards have their own locks.
    mutable SharedMutex mutex;
    std::shared_ptr<SeriesIndexData> data;
    TableState tags_state;
    TableState time_ranges_state;
    DataTypePtr id_type;

    /// These are used only with `follower_mutex` held.
    Clock::time_point no_rebuild_before;
    Clock::time_point size_limit_retry_time;
    Clock::time_point last_postings_bytes_calculation;
    Stopwatch build_watch;
    /// Whether the last call of `synchronize` has left data parts which are not applied.
    bool has_more_to_apply = false;
    /// The last time when `synchronize` applied data parts.
    Clock::time_point last_apply_time;
    /// Whether the index which is built now has applied the whole time ranges of the time series.
    bool whole_time_ranges_applied = false;
    /// The index drops the time series without samples in the last seconds of this number. Nothing if the table
    /// has no "recent samples" table: the index keeps all the time series then. It is kept when the index is built again.
    bool horizon_is_set = false;
    std::optional<UInt64> horizon_seconds;
    Int64 last_sweep_ms = 0;
    Int64 last_pending_rotation_ms = 0;

    /// The data parts which some thread applies now: a data part is never read by two threads at the same time.
    /// The first set is for the "time ranges" table, the second one is for the "tags" table.
    std::mutex apply_mutex;
    std::condition_variable apply_cv;
    std::unordered_set<String> parts_in_flight[2];
    /// It grows when data parts become applied and when the state of the index changes.
    UInt64 change_version = 0;

    std::mutex metrics_mutex;
    Int64 reported_series = 0;
    Int64 reported_bytes = 0;
    Int64 reported_unresolved = 0;

    void resetMetrics(Int64 series, Int64 bytes, Int64 unresolved)
    {
        std::lock_guard lock{metrics_mutex};
        CurrentMetrics::add(CurrentMetrics::TimeSeriesSeriesIndexSeries, series - reported_series);
        CurrentMetrics::add(CurrentMetrics::TimeSeriesSeriesIndexBytes, bytes - reported_bytes);
        CurrentMetrics::add(CurrentMetrics::TimeSeriesSeriesIndexUnresolved, unresolved - reported_unresolved);
        reported_series = series;
        reported_bytes = bytes;
        reported_unresolved = unresolved;
    }

    UInt64 getChangeVersion()
    {
        std::lock_guard lock{apply_mutex};
        return change_version;
    }

    void notifyChange()
    {
        {
            std::lock_guard lock{apply_mutex};
            ++change_version;
        }
        apply_cv.notify_all();
    }

    /// Waits until something changes after `version` was read. Returns false if nothing has changed before the deadline.
    bool waitForChange(UInt64 version, Clock::time_point deadline)
    {
        std::unique_lock lock{apply_mutex};
        return apply_cv.wait_until(lock, deadline, [&] { return (change_version != version) || shutdown_called; });
    }

    /// Registers the data parts which no thread applies now as being applied by the caller, and returns them.
    /// `parts` are in the order of preference. With `max_rows` the function takes no more rows than that,
    /// but it takes the first data part anyway if `at_least_one` is set.
    DataPartsVector claimDataParts(bool is_tags_table, const DataPartsVector & parts, std::optional<size_t> max_rows, bool at_least_one)
    {
        DataPartsVector res;
        size_t rows = 0;
        std::lock_guard lock{apply_mutex};
        auto & in_flight = parts_in_flight[is_tags_table];
        for (const auto & part : parts)
        {
            if (in_flight.contains(part->name))
                continue;
            if (max_rows && (rows + part->rows_count > *max_rows) && !(at_least_one && res.empty()))
                break;
            rows += part->rows_count;
            in_flight.insert(part->name);
            res.push_back(part);
        }
        return res;
    }

    void releaseDataParts(bool is_tags_table, const DataPartsVector & parts)
    {
        {
            std::lock_guard lock{apply_mutex};
            for (const auto & part : parts)
                parts_in_flight[is_tags_table].erase(part->name);
            ++change_version;
        }
        apply_cv.notify_all();
    }

    [[nodiscard]] std::shared_ptr<SeriesIndexData> getData() const
    {
        std::shared_lock lock{mutex};
        return data;
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
        if (!isString(removeLowCardinality(tags_columns.getPhysical(TimeSeriesColumnNames::MetricName).type)))
            return {};

        /// The index is built by a query which reads the "time ranges" table in the order of the identifiers.
        const auto sorting_key_columns = tables.time_ranges_metadata->getSortingKeyColumns();
        if (sorting_key_columns.empty() || (sorting_key_columns.front() != TimeSeriesColumnNames::ID))
            return {};

        tables.id_type = recursiveRemoveLowCardinality(tags_columns.getPhysical(TimeSeriesColumnNames::ID).type);
        if (!tables.id_type->equals(*recursiveRemoveLowCardinality(time_ranges_columns.getPhysical(TimeSeriesColumnNames::ID).type)))
            return {};
        auto id_layout = tryGetIDLayout(tables.id_type);
        if (!id_layout)
            return {};
        tables.id_layout = *id_layout;

        auto time_series_settings = time_series_storage.getStorageSettings();

        /// The id generator is found as an insert finds it: the setting, then the default expression of the column, then the default generator.
        ASTPtr id_generator = (*time_series_settings)[TimeSeriesSetting::id_generator].value;
        if (!id_generator)
        {
            if (const auto * id_column = tags_columns.tryGet(TimeSeriesColumnNames::ID))
                id_generator = id_column->default_desc.expression;
        }
        if (!id_generator)
            id_generator = TimeSeriesIDGenerator::tryGetDefault(tables.id_type);
        if (!isFirstComponentOfTimeSeriesIDMadeFromMetricName(id_generator, tables.id_type))
            return {};

        if (!for_building)
            return tables;

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
        tables.tags_select_list = fmt::format("{}, {}, {}", backQuote(TimeSeriesColumnNames::ID), all_tags, backQuote(TimeSeriesColumnNames::MetricName));
        return tables;
    }

    /// The number of the units of the timestamps of the table in one second.
    Int64 getTimestampMultiplier(const ContextPtr & local_context) const
    {
        auto time_series_metadata = time_series_storage.getInMemoryMetadataPtr(local_context, false);
        auto [timestamp_type, value_type] = splitTimeSeriesType(
            time_series_metadata->columns.get(TimeSeriesColumnNames::getOuterSamples(time_series_storage.getVersion())).type);
        const UInt32 scale = tryGetDecimalScale(*timestamp_type).value_or(0);
        return DecimalUtils::scaleMultiplier<Int64>(scale);
    }

    /// The retention of the "recent samples" table in seconds, or nothing if there is no such table.
    std::optional<UInt64> getRecentSamplesSeconds() const
    {
        auto time_series_settings = time_series_storage.getStorageSettings();
        const UInt64 recent_samples_ttl_seconds = (*time_series_settings)[TimeSeriesSetting::recent_samples_ttl_seconds];
        if (!recent_samples_ttl_seconds || !time_series_storage.hasTarget(ViewTarget::RecentSamples))
            return {};
        return recent_samples_ttl_seconds;
    }

    /// The time before which the "recent samples" table has no samples, in the scale of the timestamps of the table.
    std::optional<Int64> getStartOfRecentSamples(const ContextPtr & local_context) const
    {
        auto recent_samples_seconds = getRecentSamplesSeconds();
        if (!recent_samples_seconds)
            return {};
        return (static_cast<Int64>(std::time(nullptr)) - static_cast<Int64>(*recent_samples_seconds)) * getTimestampMultiplier(local_context);
    }

    /// A time series which got no samples for this number of seconds can be dropped. A writer skips the row of the "tags" table
    /// while its cache holds the time series. After twice the lifetime of the cache no writer holds a dropped time series,
    /// so a time series which returns comes with its tags.
    UInt64 getIdleLimitSeconds() const
    {
        auto time_series_settings = time_series_storage.getStorageSettings();
        return 2 * (*time_series_settings)[TimeSeriesSetting::tags_deduplication_cache_expiration_seconds];
    }

    /// The time before which the index drops the time series, in the scale of the timestamps of the table. Requires `follower_mutex`.
    std::optional<Int64> getHorizonStart() const
    {
        if (!horizon_seconds)
            return {};
        return (static_cast<Int64>(std::time(nullptr)) - static_cast<Int64>(*horizon_seconds)) * getTimestampMultiplier(getContext());
    }

    /// Makes the horizon shorter by a quarter. Returns false if it has its minimum length. Requires `follower_mutex`.
    bool shortenHorizon()
    {
        if (!horizon_seconds)
            return false;
        const UInt64 min_horizon_seconds = std::max(MIN_HORIZON_SECONDS, getIdleLimitSeconds());
        if (*horizon_seconds <= min_horizon_seconds)
            return false;
        horizon_seconds = std::max(min_horizon_seconds, *horizon_seconds / 4 * 3);
        LOG_INFO(log, "The series index is near its size limit of {} bytes, it will keep the time series with samples in the last {} seconds",
            max_size_bytes, *horizon_seconds);
        return true;
    }

    /// A context to list the data parts of the target tables: each listing sees the current ones.
    ContextMutablePtr makeContextForListing(const ContextPtr & base_context) const
    {
        auto local_context = Context::createCopy(base_context);
        local_context->setSetting("enable_shared_storage_snapshot_in_query", false);
        return local_context;
    }

    /// A context for a query of the index to a target table.
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
        local_context->setSetting("max_rows_to_group_by", UInt64{0});
        local_context->setSetting("use_query_condition_cache", false);
        return local_context;
    }

    /// Makes the queries with this context read the data parts held by `snapshot`, so they read them
    /// even if a merge has replaced them.
    static void setSnapshotToRead(const ContextMutablePtr & local_context, const StoragePtr & storage, const StorageSnapshotPtr & snapshot)
    {
        local_context->setSetting("enable_shared_storage_snapshot_in_query", true);
        auto metadata_cache = std::make_shared<QueryMetadataCache>();
        local_context->setQueryMetadataCache(metadata_cache);
        auto [snapshots, lock] = metadata_cache->getStorageSnapshotCache();
        snapshots->emplace(storage.get(), snapshot);
    }

    static String makeListOfPartNames(const DataPartsVector & parts)
    {
        String res;
        for (const auto & part : parts)
        {
            if (!res.empty())
                res += ", ";
            res += quoteString(part->name);
        }
        return res;
    }

    /// Executes a query and passes the blocks of its result to `apply_block` until it returns false.
    /// The blocks come in any order if `num_threads` is more than one.
    template <typename ApplyBlock>
    void executeQuery(const String & query, const ContextMutablePtr & local_context, size_t num_threads, ApplyBlock && apply_block) const
    {
        ParserSelectWithUnionQuery parser;
        ASTPtr ast = parseQuery(parser, query, /* max_query_size = */ 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

        InterpreterSelectQueryAnalyzer interpreter(ast, local_context, SelectQueryOptions{});
        auto io = interpreter.execute();

        auto pull_all = [&](auto & executor)
        {
            Block block;
            while (executor.pull(block))
            {
                if (!block.rows())
                    continue;
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
    }

    using RowsByShard = std::array<PaddedPODArray<UInt32>, NUM_SHARDS>;

    /// Applies a block with the columns of `TargetTables::tags_select_list`.
    static void applyTagsBlock(SeriesIndexData & index_data, const Block & block, bool building, RowsByShard & rows_by_shard)
    {
        auto id_column = recursiveRemoveLowCardinality(block.getByPosition(0).column->convertToFullColumnIfConst());
        auto tags_column = block.getByPosition(1).column->convertToFullColumnIfConst();
        auto metric_name_column = recursiveRemoveLowCardinality(block.getByPosition(2).column->convertToFullColumnIfConst());
        IDReader reader{*id_column, index_data.layout};

        /// The metric names are remembered before the tags are applied: a time series with tags has its metric name known.
        index_data.metric_names.add(reader, assert_cast<const ColumnString &>(*metric_name_column));

        index_data.splitRows(reader, id_column->size(), rows_by_shard);
        const Int64 now_ms = nowMilliseconds();
        for (size_t i = 0; i != NUM_SHARDS; ++i)
        {
            if (rows_by_shard[i].empty())
                continue;
            auto & shard = index_data.shards[i];
            std::lock_guard lock{shard.mutex};
            shard.series->applyTags(reader, assert_cast<const ColumnMap &>(*tags_column), rows_by_shard[i], building, now_ms);
            shard.updateNumbers();
        }
    }

    /// Applies a block with the identifier, the minimum time and the maximum time.
    static void applyTimeRangesBlock(SeriesIndexData & index_data, const Block & block, bool building, RowsByShard & rows_by_shard)
    {
        auto id_column = recursiveRemoveLowCardinality(block.getByPosition(0).column->convertToFullColumnIfConst());
        auto min_time_column = block.getByPosition(1).column->convertToFullColumnIfConst();
        auto max_time_column = block.getByPosition(2).column->convertToFullColumnIfConst();
        IDReader reader{*id_column, index_data.layout};
        TimeReader min_times{*min_time_column};
        TimeReader max_times{*max_time_column};

        index_data.splitRows(reader, id_column->size(), rows_by_shard);
        const Int64 now_ms = nowMilliseconds();
        for (size_t i = 0; i != NUM_SHARDS; ++i)
        {
            if (rows_by_shard[i].empty())
                continue;
            auto & shard = index_data.shards[i];
            std::lock_guard lock{shard.mutex};
            shard.series->applyTimeRanges(reader, min_times, max_times, rows_by_shard[i], building, now_ms);
            shard.updateNumbers();
        }
    }

    struct DataToApply
    {
        std::shared_ptr<SeriesIndexData> data;
        bool building = false;
    };

    /// Returns the data of the index for a thread which applies data parts and holds `maintenance_mutex`,
    /// or nothing if the index was dropped.
    DataToApply getDataToApply(UInt64 expected_epoch) const
    {
        std::shared_lock lock{mutex};
        if ((epoch != expected_epoch) || (state == State::NotBuilt))
            return {};
        return DataToApply{.data = data, .building = (state == State::Building)};
    }

    /// Applies data parts of a target table which the caller has claimed (see `claimDataParts`),
    /// and marks as applied the ones which were read as a whole. `snapshot` is the snapshot of the table which holds these data parts.
    /// Returns false if the index was dropped meanwhile, has reached its size limit, or is shut down.
    bool applyDataParts(
        const TargetTables & tables, bool is_tags_table, const StorageSnapshotPtr & snapshot, const DataPartsVector & parts,
        UInt64 expected_epoch, size_t num_threads)
    {
        SCOPE_EXIT({ releaseDataParts(is_tags_table, parts); });

        Stopwatch watch;
        SCOPE_EXIT({ ProfileEvents::increment(ProfileEvents::TimeSeriesSeriesIndexApplyMicroseconds, watch.elapsedMicroseconds()); });

        /// The thread which drops the index or rebuilds a shard holds this mutex for a short time. A query doesn't wait for it.
        std::shared_lock maintenance_lock{maintenance_mutex, std::try_to_lock};
        if (!maintenance_lock.owns_lock())
            return false;
        const auto [index_data, building] = getDataToApply(expected_epoch);
        if (!index_data)
            return false;

        const auto & storage = is_tags_table ? tables.tags_storage : tables.time_ranges_storage;
        const String select_list = is_tags_table
            ? tables.tags_select_list
            : fmt::format("{}, {}, {}", backQuote(TimeSeriesColumnNames::ID), backQuote(TimeSeriesColumnNames::MinTime), backQuote(TimeSeriesColumnNames::MaxTime));
        RowsByShard rows_by_shard;

        size_t begin = 0;
        while (begin != parts.size())
        {
            if (shutdown_called)
                return false;

            /// A data part with a mask of deleted rows is read alone: the number of its rows which exist is not known in advance.
            const bool with_deleted_rows = parts[begin]->hasLightweightDelete();
            size_t end = begin;
            size_t rows_in_batch = 0;
            DataPartsVector batch;
            while ((end != parts.size())
                && ((end == begin)
                    || (!with_deleted_rows && !parts[end]->hasLightweightDelete() && (rows_in_batch + parts[end]->rows_count <= MAX_ROWS_IN_BATCH))))
            {
                rows_in_batch += parts[end]->rows_count;
                if (parts[end]->rows_count)
                    batch.push_back(parts[end]);
                ++end;
            }

            size_t num_rows_read = 0;
            size_t num_rows_applied = 0;
            bool size_limit_reached = false;
            if (!batch.empty())
            {
                /// With `with_deleted_rows` the query reads the rows deleted by lightweight deletes too,
                /// and the last column of a block tells which rows exist.
                auto local_context = makeContextForReading(num_threads);
                local_context->setSetting("apply_deleted_mask", !with_deleted_rows);
                setSnapshotToRead(local_context, storage, snapshot);
                const String query = fmt::format("SELECT {}{} FROM {} WHERE _part IN ({})",
                    select_list, with_deleted_rows ? ", _row_exists" : "", storage->getStorageID().getFullTableName(), makeListOfPartNames(batch));

                executeQuery(query, local_context, num_threads, [&](Block & block)
                {
                    num_rows_read += block.rows();
                    if (with_deleted_rows)
                    {
                        auto exists_column = block.getByPosition(block.columns() - 1).column->convertToFullColumnIfConst();
                        const auto & filter = assert_cast<const ColumnUInt8 &>(*exists_column).getData();
                        block.erase(block.columns() - 1);
                        for (size_t i = 0; i != block.columns(); ++i)
                        {
                            auto & column = block.getByPosition(i).column;
                            column = column->convertToFullColumnIfConst()->filter(filter, -1);
                        }
                        if (!block.rows())
                            return true;
                    }
                    num_rows_applied += block.rows();

                    if (is_tags_table)
                        applyTagsBlock(*index_data, block, building, rows_by_shard);
                    else
                        applyTimeRangesBlock(*index_data, block, building, rows_by_shard);
                    size_limit_reached = (index_data->bytes() > max_size_bytes);
                    return !size_limit_reached;
                });
            }
            if (size_limit_reached || shutdown_called)
                return false;

            /// The query reads the data parts held by the snapshot, so it reads all their rows.
            /// The data parts are not marked as applied if it did not: they are read again then.
            if (num_rows_read != rows_in_batch)
            {
                LOG_DEBUG(log, "Read {} rows instead of {} from {} data parts of {}, will read them again",
                    num_rows_read, rows_in_batch, batch.size(), storage->getStorageID().getNameForLogs());
                begin = end;
                continue;
            }

            {
                std::lock_guard lock{mutex};
                auto & table_state = is_tags_table ? tags_state : time_ranges_state;
                for (size_t i = begin; i != end; ++i)
                {
                    const auto & part = parts[i];
                    const size_t rows = with_deleted_rows ? num_rows_applied : part->rows_count;
                    table_state.applied.add(part->info, rows);
                    if (rows)
                        table_state.known_parts[part->name] = TableState::KnownPart{.info = part->info, .rows = part->rows_count};
                }
            }

            ProfileEvents::increment(ProfileEvents::TimeSeriesSeriesIndexAppliedParts, end - begin);
            ProfileEvents::increment(ProfileEvents::TimeSeriesSeriesIndexAppliedRows, num_rows_read);
            notifyChange();
            begin = end;
        }
        return true;
    }

    /// Applies data parts of a target table which the caller has claimed with a few threads if they have many rows:
    /// each thread applies a share of the data parts. A shard is locked for the rows of one block only,
    /// so the threads apply rows to different shards at the same time. Requires `follower_mutex`.
    bool applyDataPartsInParallel(
        const TargetTables & tables, bool is_tags_table, const StorageSnapshotPtr & snapshot, const DataPartsVector & parts, UInt64 expected_epoch)
    {
        size_t num_rows = 0;
        for (const auto & part : parts)
            num_rows += part->rows_count;
        const size_t num_jobs = std::min({NUM_THREADS_TO_APPLY, parts.size(), num_rows / MIN_ROWS_PER_THREAD_TO_APPLY});
        if (num_jobs <= 1)
            return applyDataParts(tables, is_tags_table, snapshot, parts, expected_epoch, NUM_THREADS_TO_READ);

        /// The data parts are in the order of their sizes, and they go to the jobs in turn.
        std::vector<DataPartsVector> parts_of_jobs(num_jobs);
        for (size_t i = 0; i != parts.size(); ++i)
            parts_of_jobs[i % num_jobs].push_back(parts[i]);
        const size_t num_threads_to_read = std::max<size_t>(1, NUM_THREADS_TO_READ / num_jobs);
        std::atomic<bool> all_applied = true;

        /// The jobs use only what is declared before the runner, which waits for them.
        const auto * tables_ptr = &tables;
        const auto * parts_of_jobs_ptr = &parts_of_jobs;
        auto * all_applied_ptr = &all_applied;
        ThreadPoolCallbackRunnerLocal<void> runner(apply_pool, ThreadName::DEFAULT_THREAD_POOL);
        for (size_t job = 0; job != num_jobs; ++job)
        {
            runner.enqueueAndKeepTrack(
                [this, tables_ptr, parts_of_jobs_ptr, all_applied_ptr, is_tags_table, snapshot, expected_epoch, num_threads_to_read, job]
                {
                    /// The memory of the index doesn't belong to the query which applies data parts to it.
                    MemoryTrackerSwitcher memory_tracker_switcher{&total_memory_tracker};
                    if (!applyDataParts(*tables_ptr, is_tags_table, snapshot, (*parts_of_jobs_ptr)[job], expected_epoch, num_threads_to_read))
                        *all_applied_ptr = false;
                });
        }
        runner.waitForAllToFinishAndRethrowFirstError();
        return all_applied;
    }

    /// Applies the data parts of the "time ranges" table which the caller has claimed, when the index starts building.
    /// One query aggregates the rows of each time series in the order of the identifiers, so the index knows
    /// the whole time range of a time series when it decides whether to admit it, and it keeps no data for the other ones.
    /// The data parts with a mask of deleted rows are left for `applyDataParts`.
    /// Returns false if the index was dropped meanwhile, has reached its size limit, or is shut down. Requires `follower_mutex`.
    bool applyWholeTimeRanges(const TargetTables & tables, const StorageSnapshotPtr & snapshot, const DataPartsVector & parts, UInt64 expected_epoch)
    {
        SCOPE_EXIT({ releaseDataParts(/* is_tags_table = */ false, parts); });

        Stopwatch watch;
        SCOPE_EXIT({ ProfileEvents::increment(ProfileEvents::TimeSeriesSeriesIndexApplyMicroseconds, watch.elapsedMicroseconds()); });

        std::shared_lock maintenance_lock{maintenance_mutex};
        const auto index_data = getDataToApply(expected_epoch).data;
        if (!index_data)
            return false;

        DataPartsVector batch;
        DataPartsVector parts_to_mark;
        size_t rows_in_batch = 0;
        for (const auto & part : parts)
        {
            if (part->hasLightweightDelete())
                continue;
            parts_to_mark.push_back(part);
            rows_in_batch += part->rows_count;
            if (part->rows_count)
                batch.push_back(part);
        }

        const Int64 multiplier = getTimestampMultiplier(getContext());
        const Int64 now = static_cast<Int64>(std::time(nullptr));
        const std::optional<Int64> horizon_start = getHorizonStart();
        const Int64 idle_start = (now - static_cast<Int64>(getIdleLimitSeconds())) * multiplier;

        size_t num_rows_read = 0;
        bool size_limit_reached = false;
        if (!batch.empty())
        {
            auto local_context = makeContextForReading(NUM_THREADS_TO_READ);
            local_context->setSetting("optimize_aggregation_in_order", true);
            setSnapshotToRead(local_context, tables.time_ranges_storage, snapshot);
            const String query = fmt::format("SELECT {0}, min({1}), max({2}), count() FROM {3} WHERE _part IN ({4}) GROUP BY {0}",
                backQuote(TimeSeriesColumnNames::ID), backQuote(TimeSeriesColumnNames::MinTime), backQuote(TimeSeriesColumnNames::MaxTime),
                tables.time_ranges_storage->getStorageID().getFullTableName(), makeListOfPartNames(batch));

            RowsByShard rows_by_shard;
            executeQuery(query, local_context, NUM_THREADS_TO_READ, [&](Block & block)
            {
                auto id_column = recursiveRemoveLowCardinality(block.getByPosition(0).column->convertToFullColumnIfConst());
                auto min_time_column = block.getByPosition(1).column->convertToFullColumnIfConst();
                auto max_time_column = block.getByPosition(2).column->convertToFullColumnIfConst();
                auto count_column = block.getByPosition(3).column->convertToFullColumnIfConst();
                for (UInt64 count : assert_cast<const ColumnUInt64 &>(*count_column).getData())
                    num_rows_read += count;

                IDReader reader{*id_column, index_data->layout};
                TimeReader min_times{*min_time_column};
                TimeReader max_times{*max_time_column};
                index_data->splitRows(reader, id_column->size(), rows_by_shard);
                for (size_t i = 0; i != NUM_SHARDS; ++i)
                {
                    if (rows_by_shard[i].empty())
                        continue;
                    auto & shard = index_data->shards[i];
                    std::lock_guard lock{shard.mutex};
                    shard.series->applyWholeTimeRanges(reader, min_times, max_times, rows_by_shard[i], horizon_start, idle_start);
                    shard.updateNumbers();
                }
                size_limit_reached = (index_data->bytes() > max_size_bytes);
                return !size_limit_reached;
            });
        }
        if (size_limit_reached || shutdown_called)
            return false;

        if (num_rows_read != rows_in_batch)
        {
            LOG_DEBUG(log, "Read {} rows instead of {} from {} data parts of {}, will read them again",
                num_rows_read, rows_in_batch, batch.size(), tables.time_ranges_storage->getStorageID().getNameForLogs());
            return true;
        }

        {
            std::lock_guard lock{mutex};
            for (const auto & part : parts_to_mark)
            {
                time_ranges_state.applied.add(part->info, part->rows_count);
                if (part->rows_count)
                    time_ranges_state.known_parts[part->name] = TableState::KnownPart{.info = part->info, .rows = part->rows_count};
            }
        }

        ProfileEvents::increment(ProfileEvents::TimeSeriesSeriesIndexAppliedParts, parts_to_mark.size());
        ProfileEvents::increment(ProfileEvents::TimeSeriesSeriesIndexAppliedRows, num_rows_read);
        notifyChange();
        return true;
    }

    struct TableCheck
    {
        /// The data parts which are not applied, the ones with fewer rows first.
        DataPartsVector not_applied;
        size_t not_applied_rows = 0;
        /// The table has lost rows the index has applied, or a mutation has changed them.
        bool changed = false;
    };

    /// Finds which of the data parts are not applied.
    static TableCheck checkCoverage(const TableState & table_state, const DataPartsVector & parts)
    {
        TableCheck res;
        for (const auto & part : parts)
        {
            if (part->info.isPatch())
            {
                res.changed = true;
                continue;
            }
            switch (table_state.applied.check(part->info, part->rows_count))
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
        sortByRows(res.not_applied);
        return res;
    }

    /// Compares the active data parts of a target table with what the index has applied.
    /// `parts` must be listed after the last change of the known data parts.
    static TableCheck checkTable(const TableState & table_state, const DataPartsVector & parts, const StorageMetadataPtr & metadata)
    {
        TableCheck res = checkCoverage(table_state, parts);
        if (table_state.metadata && (table_state.metadata.get() != metadata.get()))
            res.changed = true;
        if (res.changed)
            return res;

        std::unordered_set<std::string_view> active_names;
        for (const auto & part : parts)
            active_names.insert(part->name);

        /// A data part with applied rows must be active or merged into an active data part with rows:
        /// `DROP PART` and `DROP PARTITION` leave nothing or an empty data part instead of it.
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

    /// Remembers the active data parts which have blocks with applied rows, to notice when one of them disappears.
    /// A data part which is not applied as a whole is remembered too: it can be a merge of applied data parts.
    static void updateKnownParts(TableState & table_state, const DataPartsVector & parts)
    {
        table_state.known_parts.clear();
        for (const auto & part : parts)
        {
            if (table_state.applied.hasRowsOf(part->info))
                table_state.known_parts[part->name] = TableState::KnownPart{.info = part->info, .rows = part->rows_count};
        }
    }

    /// Drops the data of the index. Requires `follower_mutex`.
    void reset(State new_state)
    {
        {
            std::lock_guard maintenance_lock{maintenance_mutex};
            std::lock_guard lock{mutex};
            data = nullptr;
            tags_state.clear();
            time_ranges_state.clear();
            state = new_state;
            ++epoch;
            resetMetrics(0, 0, 0);
        }
        notifyChange();
    }

    /// Starts building the index from the current state of the tables. Requires `follower_mutex`.
    void startBuilding(const TargetTables & tables)
    {
        reset(State::Building);
        if (!horizon_is_set)
        {
            horizon_seconds = getRecentSamplesSeconds();
            horizon_is_set = true;
        }
        whole_time_ranges_applied = false;
        std::lock_guard lock{mutex};
        data = std::make_shared<SeriesIndexData>(tables.id_layout);
        id_type = tables.id_type;
        tags_state.metadata = tables.tags_metadata;
        time_ranges_state.metadata = tables.time_ranges_metadata;
        ProfileEvents::increment(ProfileEvents::TimeSeriesSeriesIndexRebuilds);
        build_watch.restart();
        LOG_DEBUG(log, "Building the series index");
    }

    /// Makes the index ready for the queries after all the data parts are applied. Requires `follower_mutex`.
    /// A time series without tags has no row in the data parts of the "tags" table which are applied.
    void finishBuilding()
    {
        auto index_data = getData();
        updatePostingsBytes(/* force = */ true);
        last_sweep_ms = nowMilliseconds();
        {
            std::lock_guard lock{mutex};
            state = State::Ready;
        }
        notifyChange();
        LOG_INFO(log, "Built the series index in {} seconds: {} time series, {} bytes",
            build_watch.elapsedSeconds(), index_data->numSeries(), index_data->bytes());
    }

    /// Calculates the size of the lists of time series of the shards if it is time to do that. Requires `follower_mutex`.
    /// The lookups go on meanwhile, and the threads which apply data parts wait.
    void updatePostingsBytes(bool force)
    {
        auto index_data = getData();
        const auto now = Clock::now();
        const bool all = force || (now - last_postings_bytes_calculation >= POSTINGS_BYTES_CALCULATION_INTERVAL);
        for (auto & shard : index_data->shards)
        {
            {
                std::shared_lock lock{shard.mutex};
                if (!shard.series->hasEstimatedPostingsBytes() || (!all && !shard.series->needsPostingsBytes()))
                    continue;
            }
            std::lock_guard maintenance_lock{maintenance_mutex};
            size_t postings_bytes = 0;
            {
                std::shared_lock lock{shard.mutex};
                postings_bytes = shard.series->calculatePostingsBytes();
            }
            std::lock_guard lock{shard.mutex};
            shard.series->setPostingsBytes(postings_bytes);
            shard.updateNumbers();
        }
        if (all)
            last_postings_bytes_calculation = now;
    }

    [[nodiscard]] bool isNearSizeLimit(const SeriesIndexData & index_data) const
    {
        return static_cast<double>(index_data.bytes()) > static_cast<double>(max_size_bytes) * SIZE_RATIO_TO_SHORTEN_HORIZON;
    }

    /// Drops the time series which are older than the horizon and idle, then rebuilds the shards where the dropped time series
    /// are more than `max_dead_ratio` of all. Requires `follower_mutex`.
    void sweep(SeriesIndexData & index_data, bool increase_age, double max_dead_ratio)
    {
        const std::optional<Int64> horizon_start = getHorizonStart();
        for (auto & shard : index_data.shards)
        {
            std::lock_guard lock{shard.mutex};
            shard.series->sweep(horizon_start, increase_age);
            shard.updateNumbers();
        }

        for (auto & shard : index_data.shards)
        {
            const size_t num_ordinals = shard.num_ordinals;
            const size_t num_dead = num_ordinals - shard.num_series;
            if (!num_dead || (static_cast<double>(num_dead) <= static_cast<double>(num_ordinals) * max_dead_ratio))
                continue;

            /// The threads which apply data parts wait. The lookups go on until the new shard replaces the old one.
            std::lock_guard maintenance_lock{maintenance_mutex};
            std::unique_ptr<SeriesShard> rebuilt;
            {
                std::shared_lock lock{shard.mutex};
                rebuilt = shard.series->rebuild();
            }
            std::lock_guard lock{shard.mutex};
            rebuilt->takePendingFrom(*shard.series);
            shard.series = std::move(rebuilt);
            shard.updateNumbers();
        }
    }

    /// The periodic work on a ready index. Requires `follower_mutex`.
    void maintain(SeriesIndexData & index_data)
    {
        const Int64 now_ms = nowMilliseconds();
        if (now_ms - last_pending_rotation_ms >= PENDING_TAGS_ROTATION_INTERVAL_MS / 4)
        {
            last_pending_rotation_ms = now_ms;
            for (auto & shard : index_data.shards)
            {
                std::lock_guard lock{shard.mutex};
                shard.series->rotatePendingIfNeeded(now_ms);
                shard.updateNumbers();
            }
        }

        const Int64 sweep_interval_ms
            = std::max(static_cast<Int64>(getIdleLimitSeconds() * 1000 / SWEEPS_PER_IDLE_LIMIT), MIN_SWEEP_INTERVAL_MS);
        if (now_ms - last_sweep_ms >= sweep_interval_ms)
        {
            last_sweep_ms = now_ms;
            const bool near_size_limit = isNearSizeLimit(index_data);
            if (near_size_limit)
                shortenHorizon();
            sweep(index_data, /* increase_age = */ true, near_size_limit ? MAX_DEAD_RATIO_NEAR_SIZE_LIMIT : MAX_DEAD_RATIO);
        }
    }

    /// The "tags" table has no rows for these time series.
    static void forgetUnresolved(SeriesIndexData & index_data, const PODArray<char> & ids)
    {
        const size_t id_size = index_data.layout.size();
        for (const char * id = ids.data(); id != ids.data() + ids.size(); id += id_size)
        {
            auto & shard = index_data.shards[getShardNumber(getFirstComponent(id))];
            std::lock_guard lock{shard.mutex};
            shard.series->forgetUnresolved(id);
            shard.updateNumbers();
        }
    }

    /// Reads from the "tags" table the tags of the time series which have a time range and no tags. The row with the tags
    /// of such a time series could come when the index did not know the time series: the index had dropped it,
    /// and the writer skipped the row when the time series returned. The metric names of these time series are known
    /// by the first components of their identifiers, so the query reads a range of the primary key.
    /// A time series of an unknown metric has no row in the data parts which are applied. A row which comes later
    /// is applied as usual, so the time series waits no more after this function.
    /// With `all` the function doesn't wait for the time of the next attempt. Requires `follower_mutex`.
    void resolve(const TargetTables & tables, SeriesIndexData & index_data, bool all)
    {
        if (!index_data.numUnresolved())
            return;

        const Int64 now_ms = nowMilliseconds();
        PODArray<char> ids;
        for (auto & shard : index_data.shards)
        {
            if (!shard.num_unresolved)
                continue;
            std::lock_guard lock{shard.mutex};
            shard.series->takeUnresolvedToResolve(now_ms, all, MAX_SERIES_TO_RESOLVE_IN_QUERY, ids);
        }
        if (ids.empty())
            return;

        Stopwatch watch;
        SCOPE_EXIT({ ProfileEvents::increment(ProfileEvents::TimeSeriesSeriesIndexResolveMicroseconds, watch.elapsedMicroseconds()); });

        const size_t id_size = index_data.layout.size();
        std::set<String> metric_names;
        PODArray<char> ids_of_known_metrics;
        PODArray<char> ids_of_unknown_metrics;
        for (const char * id = ids.data(); id != ids.data() + ids.size(); id += id_size)
        {
            const bool is_known = index_data.metric_names.get(getFirstComponent(id), metric_names);
            (is_known ? ids_of_known_metrics : ids_of_unknown_metrics).insert(id, id + id_size);
        }

        forgetUnresolved(index_data, ids_of_unknown_metrics);
        if (ids_of_known_metrics.empty())
        {
            notifyChange();
            return;
        }

        const auto & element_types = assert_cast<const DataTypeTuple &>(*tables.id_type).getElements();
        auto first_column = element_types[0]->createColumn();
        auto second_column = element_types[1]->createColumn();
        for (const char * id = ids_of_known_metrics.data(); id != ids_of_known_metrics.data() + ids_of_known_metrics.size(); id += id_size)
        {
            first_column->insertData(id, IDLayout::first_size);
            second_column->insertData(id + IDLayout::first_size, index_data.layout.second_size);
        }
        MutableColumns id_columns;
        id_columns.push_back(std::move(first_column));
        id_columns.push_back(std::move(second_column));
        Block ids_block;
        ids_block.insert(ColumnWithTypeAndName{ColumnTuple::create(std::move(id_columns)), tables.id_type, TimeSeriesColumnNames::ID});

        auto local_context = makeContextForReading(/* num_threads = */ 1);
        const String ids_table_name = "_time_series_series_index_ids";
        local_context->addExternalTable(
            ids_table_name,
            TemporaryTableHolder(
                local_context,
                [&](const StorageID & table_id)
                {
                    return std::make_shared<StorageValues>(
                        table_id, ColumnsDescription{NamesAndTypesList{{TimeSeriesColumnNames::ID, tables.id_type}}}, ids_block);
                }));

        String list_of_metric_names;
        for (const auto & metric_name : metric_names)
        {
            if (!list_of_metric_names.empty())
                list_of_metric_names += ", ";
            list_of_metric_names += quoteString(metric_name);
        }
        const String query = fmt::format("SELECT {} FROM {} WHERE {} IN ({}) AND {} IN (SELECT {} FROM {})",
            tables.tags_select_list, tables.tags_storage->getStorageID().getFullTableName(),
            backQuote(TimeSeriesColumnNames::MetricName), list_of_metric_names,
            backQuote(TimeSeriesColumnNames::ID), backQuote(TimeSeriesColumnNames::ID), backQuote(ids_table_name));

        RowsByShard rows_by_shard;
        executeQuery(query, local_context, /* num_threads = */ 1, [&](Block & block)
        {
            applyTagsBlock(index_data, block, /* building = */ false, rows_by_shard);
            return true;
        });
        if (shutdown_called)
            return;

        /// The query has read every row of these time series which the table had. The ones which got no tags have no rows.
        forgetUnresolved(index_data, ids_of_known_metrics);
        notifyChange();
    }

    /// Brings the index up to date with the tables: builds it if it is not built, and applies the new data parts.
    /// Requires `follower_mutex`. With `immediately` the function doesn't wait for the time when the next rebuild is allowed
    /// or when the tags of a time series are read from the table.
    /// Returns whether the index is ready and all the data parts found by the function are applied.
    bool synchronize(bool immediately)
    {
        has_more_to_apply = false;
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

        auto listing_context = makeContextForListing(getContext());
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
                if ((now < size_limit_retry_time) || (!immediately && (now < no_rebuild_before)) || (num_builds == 3))
                    return false;
                no_rebuild_before = now + MIN_TIME_BETWEEN_REBUILDS;
                startBuilding(*tables);
                ++num_builds;
                num_time_ranges_passes = 0;
            }
            const UInt64 current_epoch = epoch;

            PartsList time_ranges_list;
            PartsList tags_list;
            TableCheck time_ranges_check;
            TableCheck tags_check;
            {
                /// The data parts are listed with the lock held: a query can apply a data part and make it known at any other moment.
                std::lock_guard lock{mutex};
                /// The row with the tags of a time series is written before the row with its time range. The "time ranges" table
                /// is listed first, so the list of the "tags" table has the tags of every time series of the first list.
                time_ranges_list = listDataParts(tables->time_ranges_storage, tables->time_ranges_metadata, listing_context);
                tags_list = listDataParts(tables->tags_storage, tables->tags_metadata, listing_context);
                time_ranges_check = checkTable(time_ranges_state, time_ranges_list.parts, tables->time_ranges_metadata);
                tags_check = checkTable(tags_state, tags_list.parts, tables->tags_metadata);
                if (!time_ranges_check.changed && !tags_check.changed)
                {
                    updateKnownParts(time_ranges_state, time_ranges_list.parts);
                    updateKnownParts(tags_state, tags_list.parts);
                }
            }

            if (time_ranges_check.changed || tags_check.changed)
            {
                LOG_DEBUG(log, "The target tables have changed, the series index will be rebuilt");
                reset(State::NotBuilt);
                continue;
            }

            if (state == State::Building)
            {
                /// The time ranges are applied before the tags, so the index keeps the tags of the time series it has admitted only.
                if (!whole_time_ranges_applied)
                {
                    auto parts = claimDataParts(/* is_tags_table = */ false, time_ranges_check.not_applied, {}, false);
                    if (!applyWholeTimeRanges(*tables, time_ranges_list.snapshot, parts, current_epoch))
                        break;
                    whole_time_ranges_applied = true;
                    ++num_time_ranges_passes;
                    continue;
                }
                /// New data parts appear all the time: the ones left after a few passes are applied after the tags.
                if (!time_ranges_check.not_applied.empty() && (num_time_ranges_passes < 3))
                {
                    auto parts = claimDataParts(/* is_tags_table = */ false, time_ranges_check.not_applied, {}, false);
                    if (!applyDataPartsInParallel(*tables, /* is_tags_table = */ false, time_ranges_list.snapshot, parts, current_epoch))
                        break;
                    ++num_time_ranges_passes;
                    continue;
                }
                auto parts = claimDataParts(/* is_tags_table = */ true, tags_check.not_applied, {}, false);
                if (!applyDataParts(*tables, /* is_tags_table = */ true, tags_list.snapshot, parts, current_epoch, NUM_THREADS_TO_READ))
                    break;
                finishBuilding();
                continue;
            }

            if (time_ranges_check.not_applied.empty() && tags_check.not_applied.empty())
            {
                all_applied = true;
                break;
            }

            /// New data parts appear all the time: the next call applies the next ones.
            has_more_to_apply = true;
            if (num_passes_when_ready == 3)
                break;
            ++num_passes_when_ready;

            /// The data parts with fewer rows are applied first: they are the new ones, and the queries wait for them.
            /// A big data part is the result of a merge. One pass applies a limited number of rows, then the data parts are listed again.
            /// The time ranges are applied before the tags: a merged data part of the "tags" table has the tags of every time series
            /// the table ever had, and the index takes the tags of the time series with a time range only.
            auto time_ranges_parts
                = claimDataParts(/* is_tags_table = */ false, time_ranges_check.not_applied, MAX_ROWS_IN_BATCH, /* at_least_one = */ true);
            if (!applyDataPartsInParallel(*tables, /* is_tags_table = */ false, time_ranges_list.snapshot, time_ranges_parts, current_epoch))
                break;
            auto tags_parts = claimDataParts(/* is_tags_table = */ true, tags_check.not_applied, MAX_ROWS_IN_BATCH, /* at_least_one = */ true);
            if (!applyDataParts(*tables, /* is_tags_table = */ true, tags_list.snapshot, tags_parts, current_epoch, NUM_THREADS_TO_READ))
                break;

            /// Other threads apply the rest of the data parts.
            if (tags_parts.empty() && time_ranges_parts.empty())
                break;
            last_apply_time = Clock::now();
        }

        if (shutdown_called)
            return false;

        auto index_data = getData();
        if (!index_data)
            return false;
        if (state == State::Ready)
        {
            resolve(*tables, *index_data, /* all = */ immediately);
            maintain(*index_data);
        }
        updatePostingsBytes(/* force = */ false);

        /// Over the size limit the index keeps fewer time series, or it is dropped if it cannot.
        while (index_data->bytes() > max_size_bytes)
        {
            const bool shortened = shortenHorizon();
            if (shortened && (state == State::Ready))
            {
                sweep(*index_data, /* increase_age = */ false, /* max_dead_ratio = */ 0);
                continue;
            }
            if (shortened)
            {
                reset(State::NotBuilt);
                no_rebuild_before = Clock::now();
            }
            else
            {
                LOG_WARNING(log, "The series index has {} time series and takes {} bytes, which is more than the limit of {} bytes. "
                    "The index is dropped and will be built again in {} seconds",
                    index_data->numSeries(), index_data->bytes(), max_size_bytes,
                    std::chrono::duration_cast<std::chrono::seconds>(TIME_TO_RETRY_AFTER_SIZE_LIMIT).count());
                reset(State::NotBuilt);
                size_limit_retry_time = Clock::now() + TIME_TO_RETRY_AFTER_SIZE_LIMIT;
            }
            has_more_to_apply = false;
            return false;
        }

        resetMetrics(index_data->numSeries(), index_data->bytes(), index_data->numUnresolved());
        return (state == State::Ready) && all_applied;
    }

    void runTask()
    {
        if (shutdown_called)
            return;

        bool idle = false;
        bool poll_soon = false;
        try
        {
            std::lock_guard follower_lock{follower_mutex};
            idle = (nowSeconds() - last_lookup_seconds > std::chrono::duration_cast<std::chrono::seconds>(MAX_IDLE_TIME).count());
            if (idle)
            {
                LOG_DEBUG(log, "The series index is not used, dropping it");
                reset(State::NotBuilt);
                task_started = false;
            }
            else
            {
                synchronize(/* immediately = */ false);
                poll_soon = has_more_to_apply || (Clock::now() - last_apply_time < TIME_OF_SHORT_POLL_INTERVAL_AFTER_APPLY);
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, "While updating the series index");
        }

        if (!idle && !shutdown_called)
            task->scheduleAfter(poll_soon ? POLL_INTERVAL_AFTER_APPLY_MS : POLL_INTERVAL_MS);
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
        /// The index is built, and a time series which the query can select has no tags in the index yet.
        Unresolved,
        /// The index is built, and it must be rebuilt.
        Stale,
    };

    /// Whether a ready index can have the time series with a time range starting at `min_time`.
    [[nodiscard]] bool canAnswer(std::optional<Int64> min_time) const
    {
        auto index_data = getData();
        if (!index_data)
            return false;
        const Int64 watermark = index_data->getMinWatermark();
        return (watermark == NO_WATERMARK) || (min_time && (*min_time > watermark));
    }

    /// The data parts of the target tables which a query must find in the index: the ones in the snapshots of these tables
    /// which the query would read them with. A data part written after that has no samples which the query reads,
    /// so the query doesn't wait for it. With the setting `enable_shared_storage_snapshot_in_query` all the selectors
    /// of a query have the same snapshots, as they have the same snapshot of the "samples" table.
    struct QueryParts
    {
        bool listed = false;
        PartsList time_ranges;
        PartsList tags;
        /// The epoch of the index at the last lookup.
        UInt64 epoch = 0;
    };

    /// Finds the time series if the index is built and has applied all the data parts of the query.
    /// `parts_context` is `query_context` with the setting `enable_shared_storage_snapshot_in_query` disabled.
    LookupStatus tryLookupImpl(
        const PreparedMatchers & matchers,
        std::optional<Int64> min_time,
        std::optional<Int64> max_time,
        const ContextPtr & query_context,
        const ContextPtr & parts_context,
        QueryParts & query_parts,
        LookupResult & res)
    {
        auto tables = tryGetTables(parts_context, /* for_building = */ false);
        if (!tables)
            return LookupStatus::CannotAnswer;

        /// A query doesn't wait for the lock while the index is built.
        if (state != State::Ready)
            return LookupStatus::NotBuilt;

        std::shared_ptr<SeriesIndexData> index_data;
        DataTypePtr current_id_type;
        {
            std::shared_lock lock{mutex};
            if (state != State::Ready)
                return LookupStatus::NotBuilt;
            query_parts.epoch = epoch;

            /// The data parts are listed with the lock held, so the data parts the index knows were active before that:
            /// a known data part which is not in the list has been merged or removed.
            auto time_ranges_list = listDataParts(tables->time_ranges_storage, tables->time_ranges_metadata, parts_context);
            auto tags_list = listDataParts(tables->tags_storage, tables->tags_metadata, parts_context);
            auto time_ranges_check = checkTable(time_ranges_state, time_ranges_list.parts, tables->time_ranges_metadata);
            auto tags_check = checkTable(tags_state, tags_list.parts, tables->tags_metadata);
            if (time_ranges_check.changed || tags_check.changed)
                return LookupStatus::Stale;

            if (!query_parts.listed)
            {
                query_parts.time_ranges = listDataParts(tables->time_ranges_storage, tables->time_ranges_metadata, query_context);
                query_parts.tags = listDataParts(tables->tags_storage, tables->tags_metadata, query_context);
                query_parts.listed = true;
            }

            /// The data parts written after the snapshots of the query don't matter.
            time_ranges_check = checkCoverage(time_ranges_state, query_parts.time_ranges.parts);
            tags_check = checkCoverage(tags_state, query_parts.tags.parts);
            if (time_ranges_check.changed || tags_check.changed)
                return LookupStatus::Stale;
            if (!time_ranges_check.not_applied.empty() || !tags_check.not_applied.empty())
                return LookupStatus::NotApplied;

            index_data = data;
            current_id_type = id_type;
        }

        /// The index has applied the data parts of the query, so every time series of these data parts which is not older
        /// than the watermark of its shard is in the index, with its tags or without them. The shards are read after that
        /// with their own locks: a time series dropped meanwhile is older than the watermark, which is checked with each shard.

        /// A time series without tags of an unknown metric can belong to any metric.
        if (index_data->numUnresolved())
        {
            std::vector<UInt64> first_components;
            for (const auto & shard : index_data->shards)
            {
                if (!shard.num_unresolved)
                    continue;
                std::shared_lock lock{shard.mutex};
                shard.series->forEachFirstComponentOfUnresolved([&](UInt64 first_component) { first_components.push_back(first_component); });
            }
            for (UInt64 first_component : first_components)
            {
                if (!index_data->metric_names.has(first_component))
                    return LookupStatus::Unresolved;
            }
        }

        /// The matchers of a metric name select the shards to read. Other matchers read all the shards.
        const auto first_components = index_data->metric_names.select(matchers);
        std::array<std::vector<UInt64>, NUM_SHARDS> first_components_by_shard;
        if (first_components)
        {
            for (UInt64 first_component : *first_components)
                first_components_by_shard[getShardNumber(first_component)].push_back(first_component);
        }

        const auto & element_types = assert_cast<const DataTypeTuple &>(*current_id_type).getElements();
        auto first_column = element_types[0]->createColumn();
        auto second_column = element_types[1]->createColumn();
        std::vector<SeriesShard::BoundMatcher> bound_matchers;
        PaddedPODArray<Ordinal> ordinals;

        for (size_t i = 0; i != NUM_SHARDS; ++i)
        {
            if (first_components && first_components_by_shard[i].empty())
                continue;

            const auto & shard = index_data->shards[i];
            std::shared_lock lock{shard.mutex};
            const auto & series = *shard.series;
            if (!series.canAnswer(min_time))
                return LookupStatus::CannotAnswer;

            if (!first_components)
            {
                if (series.numUnresolved())
                    return LookupStatus::Unresolved;
            }
            else
            {
                for (UInt64 first_component : first_components_by_shard[i])
                {
                    if (series.hasUnresolvedOf(first_component))
                        return LookupStatus::Unresolved;
                }
            }

            if (!series.bindMatchers(matchers, bound_matchers))
                continue;
            series.filterByTime(series.match(bound_matchers), min_time, max_time, ordinals);
            series.appendIDs(ordinals, *first_column, *second_column);
            series.appendTags(ordinals, lookup_pool, res.tags);
        }

        MutableColumns id_columns;
        id_columns.push_back(std::move(first_column));
        id_columns.push_back(std::move(second_column));
        res.ids = ColumnTuple::create(std::move(id_columns));
        return LookupStatus::Found;
    }

    /// Applies the data parts of a query which are not applied and which no other thread applies, if they have few rows.
    /// The background task applies the other ones.
    void applyInQuery(const QueryParts & query_parts)
    {
        /// The memory of the index doesn't belong to the query which applies data parts to it.
        MemoryTrackerSwitcher memory_tracker_switcher{&total_memory_tracker};

        auto tables = tryGetTables(getContext(), /* for_building = */ true);
        if (!tables)
            return;

        size_t rows_left = MAX_ROWS_TO_APPLY_IN_QUERY;
        for (bool is_tags_table : {false, true})
        {
            const auto & list = is_tags_table ? query_parts.tags : query_parts.time_ranges;
            DataPartsVector not_applied;
            {
                std::shared_lock lock{mutex};
                if ((epoch != query_parts.epoch) || (state != State::Ready))
                    return;
                not_applied = checkCoverage(is_tags_table ? tags_state : time_ranges_state, list.parts).not_applied;
            }
            auto parts = claimDataParts(is_tags_table, not_applied, rows_left, /* at_least_one = */ false);
            if (parts.empty())
                continue;
            for (const auto & part : parts)
                rows_left -= part->rows_count;
            if (!applyDataParts(*tables, is_tags_table, list.snapshot, parts, query_parts.epoch, /* num_threads = */ 1))
                return;
        }
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

    if (impl->state != Impl::State::Ready)
    {
        /// A query for a time range older than the samples of the "recent samples" table doesn't make the server build the index:
        /// the index is built for the queries of recent samples.
        if (auto start = impl->getStartOfRecentSamples(query_context); start && (!min_time || (*min_time < *start)))
            return false;
    }

    impl->last_lookup_seconds = Impl::nowSeconds();
    impl->ensureTaskStarted();
    return (impl->state == Impl::State::Ready) && impl->canAnswer(min_time);
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

    if (!wait && (impl->state != Impl::State::Ready))
    {
        if (auto start = impl->getStartOfRecentSamples(query_context); start && (!min_time || (*min_time < *start)))
            return {};
    }

    impl->last_lookup_seconds = Impl::nowSeconds();
    impl->ensureTaskStarted();

    auto parts_context = impl->makeContextForListing(query_context);
    const UInt64 max_wait_ms = query_context->getSettingsRef()[Setting::time_series_series_index_max_wait_ms];
    const auto prepared_matchers = prepareMatchers(matchers);

    Impl::QueryParts query_parts;
    Stopwatch wait_watch;
    bool waited = false;
    Impl::Clock::time_point deadline;
    SCOPE_EXIT({
        if (waited)
            ProfileEvents::increment(ProfileEvents::TimeSeriesSeriesIndexWaitMicroseconds, wait_watch.elapsedMicroseconds());
    });

    /// A query which checks the index builds it and applies all the data parts itself.
    size_t lookups_left = wait ? 100 : std::numeric_limits<size_t>::max();
    while (true)
    {
        /// A change after this moment ends the wait below.
        const UInt64 change_version = impl->getChangeVersion();

        LookupResult res;
        auto status = impl->tryLookupImpl(prepared_matchers, min_time, max_time, query_context, parts_context, query_parts, res);
        if (status == Impl::LookupStatus::Found)
        {
            res.waited = waited;
            return res;
        }
        if ((status == Impl::LookupStatus::CannotAnswer) || impl->shutdown_called || !--lookups_left)
            return {};

        if (wait)
        {
            std::lock_guard follower_lock{impl->follower_mutex};
            impl->synchronize(/* immediately = */ true);
            continue;
        }

        /// The index is built by the background task, and a query doesn't wait for that.
        if ((status != Impl::LookupStatus::NotApplied) && (status != Impl::LookupStatus::Unresolved))
            return {};

        /// The index has not applied some data parts of the query. The query applies the ones with few rows itself,
        /// and waits for the threads which apply the other ones. After the time limit the query reads the tables.
        if (!waited)
        {
            const Int64 now_ms = Impl::nowMilliseconds();
            if (!max_wait_ms || (now_ms < impl->no_wait_before_ms))
                return {};
            waited = true;
            wait_watch.restart();
            deadline = Impl::Clock::now() + std::chrono::milliseconds(max_wait_ms);
            impl->task->schedule();
            if (status == Impl::LookupStatus::NotApplied)
                impl->applyInQuery(query_parts);
            continue;
        }

        if ((Impl::Clock::now() >= deadline) || !impl->waitForChange(change_version, deadline))
        {
            /// The index is far behind the tables: the next queries don't wait for it for a while.
            /// A time series without tags is another case: it concerns the queries of one metric.
            if (status == Impl::LookupStatus::NotApplied)
                impl->no_wait_before_ms = Impl::nowMilliseconds() + TIME_WITHOUT_WAIT_AFTER_MISS_MS;
            return {};
        }
    }
}

bool TimeSeriesSeriesIndex::contains(const ColumnPtr & id_column, PaddedPODArray<UInt8> & res) const
{
    res.clear();
    std::shared_ptr<SeriesIndexData> index_data;
    {
        std::shared_lock lock{impl->mutex};
        if (impl->state != Impl::State::Ready)
            return false;
        index_data = impl->data;
    }

    auto full_column = recursiveRemoveLowCardinality(id_column->convertToFullColumnIfConst());
    IDReader reader{*full_column, index_data->layout};
    const size_t id_size = index_data->layout.size();
    char id[MAX_ID_SIZE];
    res.resize(full_column->size());

    /// The identifiers of one metric come together as a rule, and they are in one shard.
    std::shared_lock<SharedMutex> lock;
    size_t locked_shard = NUM_SHARDS;
    for (size_t row = 0; row != res.size(); ++row)
    {
        const size_t shard_number = getShardNumber(reader.getFirstComponent(row));
        if (shard_number != locked_shard)
        {
            if (lock.owns_lock())
                lock.unlock();
            lock = std::shared_lock{index_data->shards[shard_number].mutex};
            locked_shard = shard_number;
        }
        const auto & series = *index_data->shards[shard_number].series;
        reader.get(row, id);
        const Ordinal ordinal = series.find(id, SeriesShard::hashID(id, id_size));
        res[row] = (ordinal != NO_ORDINAL) && !series.isDead(ordinal) && series.hasTags(ordinal);
    }
    return true;
}

UInt64 TimeSeriesSeriesIndex::getEpoch() const
{
    return impl->epoch;
}

}
