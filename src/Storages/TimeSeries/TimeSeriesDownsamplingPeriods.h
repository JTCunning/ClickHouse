#pragma once

#include <base/types.h>

#include <string_view>
#include <vector>


namespace DB
{

/// One VictoriaMetrics-style downsampling level: samples older than `offset_seconds` keep one sample per `interval_seconds`.
struct TimeSeriesDownsamplingLevel
{
    UInt64 offset_seconds = 0;
    UInt64 interval_seconds = 0;
};

/// Parsed `samples_downsampling_periods`. Levels are ordered by increasing offset.
struct TimeSeriesDownsamplingPeriods
{
    std::vector<TimeSeriesDownsamplingLevel> levels;

    static TimeSeriesDownsamplingPeriods parse(std::string_view text);

    /// The last level whose offset is at most `age_seconds`, or nullptr when every sample in the range stays raw.
    const TimeSeriesDownsamplingLevel * levelForAge(UInt64 age_seconds) const;

    /// Spans strictly larger than the samples bucket step, coarsest first.
    /// A level whose interval is at most the bucket step does not add a span: its rows stay one bucket wide.
    std::vector<UInt64> sortingSpans(UInt64 bucket_step_seconds) const;

    void validateForTable(UInt64 bucket_step_seconds, UInt64 min_offset_seconds) const;
};

/// Right-closed interval end used by VictoriaMetrics: `(k * interval, (k + 1) * interval]`.
/// A timestamp that falls on `k * interval` belongs to the interval that ends there.
inline Int64 timeSeriesDownsampleIntervalEnd(Int64 timestamp, Int64 interval)
{
    Int64 end = timestamp + interval - 1;
    Int64 mod = end % interval;
    if (mod < 0)
        mod += interval;
    return end - mod;
}

}
