#include <Storages/TimeSeries/TimeSeriesInsertCache.h>

#include <Common/CurrentMetrics.h>

#include <algorithm>


namespace CurrentMetrics
{
extern const Metric TimeSeriesInsertCacheBytes;
extern const Metric TimeSeriesInsertCacheEntries;
}

namespace DB
{

namespace
{
    constexpr size_t METRIC_FAMILY_CACHE_DIVISOR = 256;
    constexpr size_t MIN_METRIC_FAMILY_CACHE_BYTES = 65536;
    constexpr size_t BITS_PER_WORD = sizeof(UInt64) * 8;

    UInt64 foldHash(UInt128 hash)
    {
        return static_cast<UInt64>(hash) ^ static_cast<UInt64>(hash >> 64);
    }

    size_t capacityForBudget(size_t budget, size_t entry_size)
    {
        const size_t full_word_size = BITS_PER_WORD * entry_size + sizeof(UInt64);
        const size_t full_words = budget / full_word_size;
        size_t capacity = full_words * BITS_PER_WORD;
        const size_t remaining_bytes = budget % full_word_size;
        if (remaining_bytes >= sizeof(UInt64) + entry_size)
            capacity += std::min(BITS_PER_WORD - 1, (remaining_bytes - sizeof(UInt64)) / entry_size);
        return capacity;
    }

    size_t occupancyWords(size_t capacity)
    {
        return (capacity + BITS_PER_WORD - 1) / BITS_PER_WORD;
    }
}

TimeSeriesInsertCache::TimeSeriesInsertCache(size_t max_size_in_bytes)
{
    size_t metric_family_bytes = 0;
    if (max_size_in_bytes >= MIN_METRIC_FAMILY_CACHE_BYTES)
        metric_family_bytes = max_size_in_bytes / METRIC_FAMILY_CACHE_DIVISOR;

    const size_t series_bytes = max_size_in_bytes - metric_family_bytes;
    const size_t series_capacity = capacityForBudget(series_bytes, sizeof(UInt128));
    const size_t metric_family_capacity = capacityForBudget(metric_family_bytes, 2 * sizeof(UInt128));

    series_hashes.resize(series_capacity);
    series_occupied.resize(occupancyWords(series_capacity));
    metric_family_name_hashes.resize(metric_family_capacity);
    metric_family_value_hashes.resize(metric_family_capacity);
    metric_family_occupied.resize(occupancyWords(metric_family_capacity));

    ::CurrentMetrics::add(::CurrentMetrics::TimeSeriesInsertCacheBytes, allocatedBytes());
}

TimeSeriesInsertCache::~TimeSeriesInsertCache()
{
    ::CurrentMetrics::sub(::CurrentMetrics::TimeSeriesInsertCacheEntries, occupied_entries.load());
    ::CurrentMetrics::sub(::CurrentMetrics::TimeSeriesInsertCacheBytes, allocatedBytes());
}

size_t TimeSeriesInsertCache::seriesIndex(UInt128 hash) const
{
    return foldHash(hash) % series_hashes.size();
}

size_t TimeSeriesInsertCache::metricFamilyIndex(UInt128 hash) const
{
    return foldHash(hash) % metric_family_name_hashes.size();
}

size_t TimeSeriesInsertCache::allocatedBytes() const
{
    return series_hashes.capacity() * sizeof(series_hashes.front())
        + series_occupied.capacity() * sizeof(series_occupied.front())
        + metric_family_name_hashes.capacity() * sizeof(metric_family_name_hashes.front())
        + metric_family_value_hashes.capacity() * sizeof(metric_family_value_hashes.front())
        + metric_family_occupied.capacity() * sizeof(metric_family_occupied.front());
}

bool TimeSeriesInsertCache::containsSeries(UInt128 hash) const
{
    if (series_hashes.empty())
        return false;

    const size_t index = seriesIndex(hash);
    const size_t word = index / BITS_PER_WORD;
    const UInt64 mask = UInt64{1} << (index % BITS_PER_WORD);
    std::lock_guard lock(mutexes[word % NUM_SHARDS]);
    return (series_occupied[word] & mask) && series_hashes[index] == hash;
}

bool TimeSeriesInsertCache::containsMetricFamily(UInt128 name_hash, UInt128 value_hash) const
{
    if (metric_family_name_hashes.empty())
        return false;

    const size_t index = metricFamilyIndex(name_hash);
    const size_t word = index / BITS_PER_WORD;
    const UInt64 mask = UInt64{1} << (index % BITS_PER_WORD);
    std::lock_guard lock(mutexes[word % NUM_SHARDS]);
    return (metric_family_occupied[word] & mask)
        && metric_family_name_hashes[index] == name_hash
        && metric_family_value_hashes[index] == value_hash;
}

void TimeSeriesInsertCache::insertSeries(const std::vector<UInt128> & hashes)
{
    for (const auto hash : hashes)
    {
        if (series_hashes.empty())
            return;

        const size_t index = seriesIndex(hash);
        const size_t word = index / BITS_PER_WORD;
        const UInt64 mask = UInt64{1} << (index % BITS_PER_WORD);
        std::lock_guard lock(mutexes[word % NUM_SHARDS]);
        if (!(series_occupied[word] & mask))
        {
            series_occupied[word] |= mask;
            ++occupied_entries;
            ::CurrentMetrics::add(::CurrentMetrics::TimeSeriesInsertCacheEntries);
        }
        series_hashes[index] = hash;
    }
}

void TimeSeriesInsertCache::insertMetricFamilies(const std::vector<MetricFamilyEntry> & entries)
{
    for (const auto & entry : entries)
    {
        if (metric_family_name_hashes.empty())
            return;

        const size_t index = metricFamilyIndex(entry.name_hash);
        const size_t word = index / BITS_PER_WORD;
        const UInt64 mask = UInt64{1} << (index % BITS_PER_WORD);
        std::lock_guard lock(mutexes[word % NUM_SHARDS]);
        if (!(metric_family_occupied[word] & mask))
        {
            metric_family_occupied[word] |= mask;
            ++occupied_entries;
            ::CurrentMetrics::add(::CurrentMetrics::TimeSeriesInsertCacheEntries);
        }
        metric_family_name_hashes[index] = entry.name_hash;
        metric_family_value_hashes[index] = entry.value_hash;
    }
}

void TimeSeriesInsertCache::clear()
{
    std::array<std::unique_lock<std::mutex>, NUM_SHARDS> locks;
    for (size_t i = 0; i != NUM_SHARDS; ++i)
        locks[i] = std::unique_lock(mutexes[i]);

    std::fill(series_occupied.begin(), series_occupied.end(), 0);
    std::fill(metric_family_occupied.begin(), metric_family_occupied.end(), 0);
    ::CurrentMetrics::sub(::CurrentMetrics::TimeSeriesInsertCacheEntries, occupied_entries.exchange(0));
}

}
