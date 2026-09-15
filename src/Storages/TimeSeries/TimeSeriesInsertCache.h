#pragma once

#include <base/extended_types.h>

#include <array>
#include <atomic>
#include <mutex>
#include <vector>


namespace DB
{

class TimeSeriesInsertCache
{
public:
    struct MetricFamilyEntry
    {
        UInt128 name_hash;
        UInt128 value_hash;
    };

    explicit TimeSeriesInsertCache(size_t max_size_in_bytes);
    ~TimeSeriesInsertCache();

    bool containsSeries(UInt128 hash) const;
    bool containsMetricFamily(UInt128 name_hash, UInt128 value_hash) const;

    void insertSeries(const std::vector<UInt128> & hashes);
    void insertMetricFamilies(const std::vector<MetricFamilyEntry> & entries);
    void clear();

private:
    static constexpr size_t NUM_SHARDS = 64;

    size_t seriesIndex(UInt128 hash) const;
    size_t metricFamilyIndex(UInt128 hash) const;
    size_t allocatedBytes() const;

    mutable std::array<std::mutex, NUM_SHARDS> mutexes;
    std::vector<UInt128> series_hashes;
    std::vector<UInt64> series_occupied;
    std::vector<UInt128> metric_family_name_hashes;
    std::vector<UInt128> metric_family_value_hashes;
    std::vector<UInt64> metric_family_occupied;
    std::atomic_size_t occupied_entries = 0;
};

}
