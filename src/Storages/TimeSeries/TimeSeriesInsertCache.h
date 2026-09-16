#pragma once

#include <base/extended_types.h>

#include <memory>
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

    bool containsSeries(UInt128 hash);
    bool containsMetricFamily(UInt128 name_hash, UInt128 value_hash);

    void insertSeries(const std::vector<UInt128> & hashes);
    void insertMetricFamilies(const std::vector<MetricFamilyEntry> & entries);
    void clear();

private:
    class SeriesCache;
    class MetricFamilyCache;

    std::unique_ptr<SeriesCache> series_cache;
    std::unique_ptr<MetricFamilyCache> metric_family_cache;
};

}
