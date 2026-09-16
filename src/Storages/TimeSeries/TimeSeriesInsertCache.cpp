#include <Storages/TimeSeries/TimeSeriesInsertCache.h>

#include <Common/CacheBase.h>
#include <Common/CurrentMetrics.h>
#include <Common/HashTable/Hash.h>


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

    struct TimeSeriesSeen
    {
    };

    struct TimeSeriesInsertCacheWeightFunction
    {
        static constexpr size_t OVERHEAD = 96;

        template <typename T>
        size_t operator()(const T & mapped) const
        {
            return sizeof(mapped) + OVERHEAD;
        }
    };
}

class TimeSeriesInsertCache::SeriesCache
    : public CacheBase<UInt128, TimeSeriesSeen, UInt128TrivialHash, TimeSeriesInsertCacheWeightFunction>
{
public:
    using Base = CacheBase<UInt128, TimeSeriesSeen, UInt128TrivialHash, TimeSeriesInsertCacheWeightFunction>;

    explicit SeriesCache(size_t max_size_in_bytes)
        : Base(
            "LRU",
            CurrentMetrics::TimeSeriesInsertCacheBytes,
            CurrentMetrics::TimeSeriesInsertCacheEntries,
            max_size_in_bytes,
            /*max_count=*/ 0,
            /*size_ratio=*/ 0)
    {
    }
};

class TimeSeriesInsertCache::MetricFamilyCache
    : public CacheBase<UInt128, UInt128, UInt128TrivialHash, TimeSeriesInsertCacheWeightFunction>
{
public:
    using Base = CacheBase<UInt128, UInt128, UInt128TrivialHash, TimeSeriesInsertCacheWeightFunction>;

    explicit MetricFamilyCache(size_t max_size_in_bytes)
        : Base(
            "LRU",
            CurrentMetrics::TimeSeriesInsertCacheBytes,
            CurrentMetrics::TimeSeriesInsertCacheEntries,
            max_size_in_bytes,
            /*max_count=*/ 0,
            /*size_ratio=*/ 0)
    {
    }
};

TimeSeriesInsertCache::TimeSeriesInsertCache(size_t max_size_in_bytes)
{
    size_t metric_family_bytes = 0;
    if (max_size_in_bytes >= MIN_METRIC_FAMILY_CACHE_BYTES)
        metric_family_bytes = max_size_in_bytes / METRIC_FAMILY_CACHE_DIVISOR;

    series_cache = std::make_unique<SeriesCache>(max_size_in_bytes - metric_family_bytes);
    metric_family_cache = std::make_unique<MetricFamilyCache>(metric_family_bytes);
}

TimeSeriesInsertCache::~TimeSeriesInsertCache() = default;

bool TimeSeriesInsertCache::containsSeries(UInt128 hash)
{
    return static_cast<bool>(series_cache->get(hash));
}

bool TimeSeriesInsertCache::containsMetricFamily(UInt128 name_hash, UInt128 value_hash)
{
    auto cached = metric_family_cache->get(name_hash);
    return cached && *cached == value_hash;
}

void TimeSeriesInsertCache::insertSeries(const std::vector<UInt128> & hashes)
{
    for (const auto hash : hashes)
        series_cache->set(hash, std::make_shared<TimeSeriesSeen>());
}

void TimeSeriesInsertCache::insertMetricFamilies(const std::vector<MetricFamilyEntry> & entries)
{
    for (const auto & entry : entries)
        metric_family_cache->set(entry.name_hash, std::make_shared<UInt128>(entry.value_hash));
}

void TimeSeriesInsertCache::clear()
{
    series_cache->clear();
    metric_family_cache->clear();
}

}
