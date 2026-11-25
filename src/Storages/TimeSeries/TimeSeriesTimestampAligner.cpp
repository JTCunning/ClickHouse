#include <Storages/TimeSeries/TimeSeriesTimestampAligner.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypesDecimal.h>

#include <algorithm>
#include <limits>
#include <vector>


namespace DB
{

namespace
{
    constexpr UInt8 MAX_CONSECUTIVE_MISSES = 4;
    constexpr Int64 MAX_SCRAPE_INTERVAL_SECONDS = 3600;

    constexpr Int128 absInt128(Int128 v)
    {
        return v < 0 ? -v : v;
    }

    Int64 inferInterval(Int64 delta, Int64 ticks_per_second, Int64 tolerance_ticks)
    {
        if (delta <= 0)
            return 0;

        Int64 unit = ticks_per_second;
        if (delta < ticks_per_second)
        {
            if (ticks_per_second < 10)
                return 0;
            unit = ticks_per_second / 10;
        }

        Int128 rounded = ((static_cast<Int128>(delta) + unit / 2) / unit) * unit;
        Int128 max_interval = static_cast<Int128>(MAX_SCRAPE_INTERVAL_SECONDS) * ticks_per_second;
        if (rounded <= 0 || rounded > max_interval)
            return 0;

        Int64 candidate = static_cast<Int64>(rounded);
        Int64 effective_tol = std::min(tolerance_ticks, candidate / 100);
        if (effective_tol <= 0)
            return 0;

        if (absInt128(static_cast<Int128>(delta) - rounded) <= static_cast<Int128>(2) * effective_tol)
            return candidate;

        return 0;
    }

    std::optional<Int64> trySnapToAnchor(
        TimeSeriesTimestampAligner::Anchor & anchor,
        Int64 raw_ts,
        Int64 tolerance_ticks,
        Int64 min_k,
        Int64 min_val,
        Int64 max_val)
    {
        if (!anchor.initialized || anchor.interval <= 0)
            return std::nullopt;

        Int128 diff = static_cast<Int128>(raw_ts) - anchor.anchor_ts;
        if (diff <= 0)
            return std::nullopt;

        Int64 effective_tol = std::min(tolerance_ticks, anchor.interval / 100);
        if (effective_tol <= 0)
            return std::nullopt;

        Int128 k = (diff + anchor.interval / 2) / anchor.interval;
        if (k < min_k)
            return std::nullopt;

        Int128 expected_128 = static_cast<Int128>(anchor.anchor_ts) + k * anchor.interval;
        if (expected_128 < min_val || expected_128 > max_val)
            return std::nullopt;

        if (absInt128(static_cast<Int128>(raw_ts) - expected_128) > effective_tol)
            return std::nullopt;

        Int64 expected = static_cast<Int64>(expected_128);
        if (expected <= anchor.last_emitted_ts)
            return std::nullopt;

        anchor.confirmed = true;
        anchor.miss_count = 0;
        anchor.last_emitted_ts = expected;
        return expected;
    }

    Int64 alignOneSample(
        TimeSeriesTimestampAligner::SeriesState & state,
        Int64 raw_ts,
        Int64 ticks_per_second,
        Int64 tolerance_ticks,
        Int64 min_val,
        Int64 max_val)
    {
        for (auto & anchor : state.anchors)
        {
            if (anchor.confirmed)
            {
                if (auto snapped = trySnapToAnchor(anchor, raw_ts, tolerance_ticks, /* min_k = */ 1, min_val, max_val))
                    return *snapped;
            }
        }

        for (auto & anchor : state.anchors)
        {
            if (anchor.initialized && !anchor.confirmed && anchor.interval > 0)
            {
                if (auto snapped = trySnapToAnchor(anchor, raw_ts, tolerance_ticks, /* min_k = */ 2, min_val, max_val))
                    return *snapped;
            }
        }

        for (auto & anchor : state.anchors)
        {
            if (anchor.initialized && !anchor.confirmed)
            {
                Int128 diff = static_cast<Int128>(raw_ts) - anchor.anchor_ts;
                if (diff > 0 && diff <= std::numeric_limits<Int64>::max())
                {
                    Int64 candidate = inferInterval(static_cast<Int64>(diff), ticks_per_second, tolerance_ticks);
                    if (candidate > 0)
                    {
                        anchor.interval = candidate;
                        anchor.last_emitted_ts = std::max(anchor.last_emitted_ts, raw_ts);
                        anchor.miss_count = 0;
                        return raw_ts;
                    }
                }
            }
        }

        for (auto & anchor : state.anchors)
        {
            if (!anchor.initialized)
            {
                anchor = TimeSeriesTimestampAligner::Anchor{
                    .anchor_ts = raw_ts,
                    .interval = 0,
                    .last_emitted_ts = raw_ts,
                    .miss_count = 0,
                    .confirmed = false,
                    .initialized = true,
                };
                return raw_ts;
            }
        }

        for (auto & anchor : state.anchors)
        {
            if (anchor.miss_count < MAX_CONSECUTIVE_MISSES)
                ++anchor.miss_count;
            anchor.last_emitted_ts = std::max(anchor.last_emitted_ts, raw_ts);
        }

        size_t victim = 0;
        for (size_t i = 1; i < state.anchors.size(); ++i)
        {
            const auto & cur = state.anchors[i];
            const auto & best = state.anchors[victim];
            if ((!cur.confirmed && best.confirmed) || (cur.confirmed == best.confirmed && cur.miss_count > best.miss_count))
                victim = i;
        }

        if (state.anchors[victim].miss_count >= MAX_CONSECUTIVE_MISSES)
        {
            state.anchors[victim] = TimeSeriesTimestampAligner::Anchor{
                .anchor_ts = raw_ts,
                .interval = 0,
                .last_emitted_ts = raw_ts,
                .miss_count = 0,
                .confirmed = false,
                .initialized = true,
            };
        }

        return raw_ts;
    }

    template <typename TimestampValue>
    Int64 timestampToInt64(TimestampValue v)
    {
        if constexpr (is_decimal<TimestampValue>)
            return v.value;
        else
            return static_cast<Int64>(v);
    }

    template <typename TimestampValue>
    TimestampValue int64ToTimestamp(Int64 v)
    {
        if constexpr (is_decimal<TimestampValue>)
            return TimestampValue(v);
        else
            return static_cast<TimestampValue>(v);
    }
}

TimeSeriesTimestampAligner::TimeSeriesTimestampAligner(
    UInt64 tolerance_ms_,
    size_t max_cache_size_bytes_,
    CurrentMetrics::Metric entries_metric,
    CurrentMetrics::Metric bytes_metric,
    ProfileEvents::Event aligned_event_,
    ProfileEvents::Event unchanged_event_)
    : tolerance_ms(tolerance_ms_)
    , max_pending_states(max_cache_size_bytes_ ? (max_cache_size_bytes_ / APPROXIMATE_ENTRY_SIZE) : 0)
    , aligned_event(aligned_event_)
    , unchanged_event(unchanged_event_)
{
    if (max_cache_size_bytes_ > 0)
        cache.emplace(bytes_metric, entries_metric, max_cache_size_bytes_);
}

ColumnPtr TimeSeriesTimestampAligner::alignTimestamps(
    const PaddedPODArray<UInt8> & filter,
    const IColumn & id_column,
    const IColumn & ts_timestamps,
    const DataTypePtr & timestamp_type,
    const ColumnArray::Offsets & ts_offsets,
    PendingStates & pending_states) const
{
    if (tolerance_ms == 0 || ts_timestamps.empty())
        return nullptr;

    UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_type).value_or(0);
    Int64 ticks_per_second = DecimalUtils::scaleMultiplier<Int64>(timestamp_scale);
    Int64 tolerance_ticks = 0;
    if (timestamp_scale >= 3)
        tolerance_ticks = static_cast<Int64>(tolerance_ms) * DecimalUtils::scaleMultiplier<Int64>(timestamp_scale - 3);
    else
        tolerance_ticks = static_cast<Int64>(tolerance_ms) / DecimalUtils::scaleMultiplier<Int64>(3 - timestamp_scale);

    if (tolerance_ticks <= 0)
        return nullptr;

    if (const auto * decimal_timestamps = typeid_cast<const ColumnDecimal<DateTime64> *>(&ts_timestamps))
        return alignTimestampsImpl(filter, id_column, *decimal_timestamps, ticks_per_second, tolerance_ticks, ts_offsets, pending_states);

    if (const auto * uint32_timestamps = typeid_cast<const ColumnUInt32 *>(&ts_timestamps))
        return alignTimestampsImpl(filter, id_column, *uint32_timestamps, ticks_per_second, tolerance_ticks, ts_offsets, pending_states);

    return nullptr;
}

template <typename TimestampColumn>
ColumnPtr TimeSeriesTimestampAligner::alignTimestampsImpl(
    const PaddedPODArray<UInt8> & filter,
    const IColumn & id_column,
    const TimestampColumn & ts_timestamps,
    Int64 ticks_per_second,
    Int64 tolerance_ticks,
    const ColumnArray::Offsets & ts_offsets,
    PendingStates & pending_states) const
{
    using TimestampValue = typename TimestampColumn::ValueType;

    constexpr Int64 min_val = is_decimal<TimestampValue> ? std::numeric_limits<Int64>::min() : 0;
    constexpr Int64 max_val = is_decimal<TimestampValue> ? std::numeric_limits<Int64>::max() : static_cast<Int64>(std::numeric_limits<UInt32>::max());

    size_t num_series = id_column.size();
    std::vector<KeyHash> series_keys;
    series_keys.reserve(num_series);

    std::vector<KeyHash> cache_lookup_keys;
    std::vector<size_t> cache_lookup_positions;
    if (cache)
    {
        cache_lookup_keys.reserve(num_series);
        cache_lookup_positions.reserve(num_series);
    }

    for (size_t id_idx = 0; id_idx < num_series; ++id_idx)
    {
        SipHash hash;
        id_column.updateHashWithValue(id_idx, hash);
        KeyHash key = hash.get128();
        series_keys.push_back(key);
        if (cache && !pending_states.contains(key))
        {
            cache_lookup_keys.push_back(key);
            cache_lookup_positions.push_back(id_idx);
        }
    }

    std::vector<Cache::MappedPtr> cached_states;
    std::vector<size_t> lookup_index_for_series;
    if (cache && !cache_lookup_keys.empty())
    {
        cached_states = cache->getMany(cache_lookup_keys);
        lookup_index_for_series.assign(num_series, std::numeric_limits<size_t>::max());
        for (size_t k = 0; k < cache_lookup_positions.size(); ++k)
            lookup_index_for_series[cache_lookup_positions[k]] = k;
    }

    const auto & src_data = ts_timestamps.getData();
    IColumn::MutablePtr out_column;
    TimestampValue * out_data = nullptr;

    size_t num_aligned = 0;
    size_t num_unchanged = 0;
    size_t id_index = 0;

    for (size_t i = 0; i < filter.size(); ++i)
    {
        if (!filter[i])
            continue;

        size_t ts_start = (i == 0) ? 0 : ts_offsets[i - 1];
        size_t ts_end = ts_offsets[i];
        if (ts_start == ts_end)
        {
            ++id_index;
            continue;
        }

        KeyHash key = series_keys[id_index];
        SeriesState local_state;
        SeriesState * state_ptr = nullptr;

        auto pending_it = pending_states.find(key);
        if (pending_it != pending_states.end())
        {
            state_ptr = &pending_it->second;
        }
        else
        {
            if (!lookup_index_for_series.empty())
            {
                size_t lookup_idx = lookup_index_for_series[id_index];
                if (lookup_idx != std::numeric_limits<size_t>::max() && cached_states[lookup_idx])
                    local_state = *cached_states[lookup_idx];
            }

            if (max_pending_states == 0 || pending_states.size() < max_pending_states)
                state_ptr = &pending_states.emplace(key, local_state).first->second;
            else
                state_ptr = &local_state;
        }

        std::optional<Int64> prev_emitted_in_row;
        for (size_t j = ts_start; j < ts_end; ++j)
        {
            Int64 raw_ts = timestampToInt64(src_data[j]);
            Int64 aligned_ts = alignOneSample(*state_ptr, raw_ts, ticks_per_second, tolerance_ticks, min_val, max_val);

            /// Guard against snapping two distinct samples in the same row onto the exact same timestamp.
            if (prev_emitted_in_row && aligned_ts == *prev_emitted_in_row && raw_ts != *prev_emitted_in_row)
                aligned_ts = raw_ts;
            prev_emitted_in_row = aligned_ts;

            if (aligned_ts != raw_ts)
            {
                if (!out_column)
                {
                    out_column = ts_timestamps.clone();
                    out_data = assert_cast<TimestampColumn &>(*out_column).getData().data();
                }
                out_data[j] = int64ToTimestamp<TimestampValue>(aligned_ts);
                ++num_aligned;
            }
            else
            {
                ++num_unchanged;
            }
        }

        ++id_index;
    }

    ProfileEvents::increment(aligned_event, num_aligned);
    ProfileEvents::increment(unchanged_event, num_unchanged);

    return out_column;
}

void TimeSeriesTimestampAligner::commitStates(PendingStates && pending_states_)
{
    PendingStates pending_states = std::move(pending_states_);
    if (!cache || pending_states.empty())
        return;

    std::vector<KeyHash> keys;
    std::vector<Cache::MappedPtr> values;
    keys.reserve(pending_states.size());
    values.reserve(pending_states.size());

    for (const auto & [key, state] : pending_states)
    {
        keys.push_back(key);
        values.push_back(std::make_shared<SeriesState>(state));
    }

    cache->setMany(keys, values);
}

}
