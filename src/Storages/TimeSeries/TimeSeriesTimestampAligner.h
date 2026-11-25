#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/IColumn.h>
#include <Common/CacheBase.h>
#include <Common/CurrentMetrics.h>
#include <Common/HashTable/Hash.h>
#include <Common/PODArray.h>
#include <Common/ProfileEvents.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <DataTypes/IDataType.h>
#include <base/types.h>

#include <array>
#include <memory>
#include <optional>


namespace DB
{

/// Aligns ingested sample timestamps onto each time series' inferred regular scrape grid
/// when the deviation is within a bounded tolerance (clamped to 1% of the inferred interval).
///
/// Each series tracks up to two grid anchors so both single-scraper and HA-pair interleaved
/// streams can align without false locks. An anchor only starts snapping after two consecutive
/// matching intervals confirm the grid, and samples outside tolerance are stored unchanged.
class TimeSeriesTimestampAligner
{
public:
    using KeyHash = UInt128;

    struct Anchor
    {
        Int64 anchor_ts = 0;
        Int64 interval = 0;
        Int64 last_emitted_ts = 0;
        UInt8 miss_count = 0;
        bool confirmed = false;
        bool initialized = false;
    };

    struct SeriesState
    {
        std::array<Anchor, 2> anchors{};
    };

    using PendingStates = UnorderedMapWithMemoryTracking<KeyHash, SeriesState, UInt128TrivialHash>;

    TimeSeriesTimestampAligner(
        UInt64 tolerance_ms_,
        size_t max_cache_size_bytes_,
        CurrentMetrics::Metric entries_metric,
        CurrentMetrics::Metric bytes_metric,
        ProfileEvents::Event aligned_event_,
        ProfileEvents::Event unchanged_event_);

    /// Aligns sample timestamps for rows marked in `filter`.
    /// Returns a new timestamp column if at least one sample changed, or nullptr if all timestamps stayed unchanged.
    ColumnPtr alignTimestamps(
        const PaddedPODArray<UInt8> & filter,
        const IColumn & id_column,
        const IColumn & ts_timestamps,
        const DataTypePtr & timestamp_type,
        const ColumnArray::Offsets & ts_offsets,
        PendingStates & pending_states) const;

    /// Commits pending per-series anchor states to the shared cache after an insert succeeds.
    void commitStates(PendingStates && pending_states);

private:
    struct SeriesStateWeight
    {
        size_t operator()(const SeriesState &) const { return APPROXIMATE_ENTRY_SIZE; }
    };

    using Cache = CacheBase<KeyHash, SeriesState, UInt128TrivialHash, SeriesStateWeight>;

    static constexpr size_t APPROXIMATE_ENTRY_SIZE
        = (sizeof(KeyHash) + 6 * sizeof(void *)) + (sizeof(KeyHash) + 2 * sizeof(void *)) + (sizeof(SeriesState) + 3 * sizeof(void *));

    template <typename TimestampColumn>
    ColumnPtr alignTimestampsImpl(
        const PaddedPODArray<UInt8> & filter,
        const IColumn & id_column,
        const TimestampColumn & ts_timestamps,
        Int64 ticks_per_second,
        Int64 tolerance_ticks,
        const ColumnArray::Offsets & ts_offsets,
        PendingStates & pending_states) const;

    const UInt64 tolerance_ms;
    const size_t max_pending_states;
    const ProfileEvents::Event aligned_event;
    const ProfileEvents::Event unchanged_event;
    mutable std::optional<Cache> cache;
};

using TimeSeriesTimestampAlignerPtr = std::shared_ptr<TimeSeriesTimestampAligner>;

}
