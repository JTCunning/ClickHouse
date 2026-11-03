#include <Storages/TimeSeries/TimeSeriesDownsamplingPeriods.h>

#include <Common/Exception.h>
#include <Parsers/Prometheus/PrometheusQueryParsingUtil.h>

#include <algorithm>
#include <cctype>
#include <limits>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SETTING_VALUE;
}

namespace
{

std::string_view trim(std::string_view text)
{
    while (!text.empty() && std::isspace(static_cast<unsigned char>(text.front())))
        text.remove_prefix(1);
    while (!text.empty() && std::isspace(static_cast<unsigned char>(text.back())))
        text.remove_suffix(1);
    return text;
}

UInt64 parseDurationSeconds(std::string_view text, std::string_view setting_text)
{
    PrometheusQueryParsingUtil::DurationType duration;
    String error;
    if (!PrometheusQueryParsingUtil::tryParseDuration(text, /*duration_scale=*/ 0, duration, &error))
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "Cannot parse duration '{}' in `samples_downsampling_periods` value '{}': {}",
            text, setting_text, error);

    if (duration.value <= 0 || static_cast<UInt64>(duration.value) > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "Duration '{}' in `samples_downsampling_periods` value '{}' must be in the range [1s, {}s]",
            text, setting_text, std::numeric_limits<UInt32>::max());

    return static_cast<UInt64>(duration.value);
}

}

TimeSeriesDownsamplingPeriods TimeSeriesDownsamplingPeriods::parse(std::string_view text)
{
    TimeSeriesDownsamplingPeriods result;
    text = trim(text);
    if (text.empty())
        return result;

    if (text.find('{') != std::string_view::npos || text.find('}') != std::string_view::npos)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "`samples_downsampling_periods` does not accept a series filter, got '{}'", text);

    while (!text.empty())
    {
        const auto comma = text.find(',');
        const auto piece = trim(text.substr(0, comma));
        text = (comma == std::string_view::npos) ? std::string_view{} : text.substr(comma + 1);
        if (piece.empty())
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                "`samples_downsampling_periods` contains an empty level");

        const auto colon = piece.find(':');
        if (colon == std::string_view::npos || piece.find(':', colon + 1) != std::string_view::npos)
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                "Level '{}' in `samples_downsampling_periods` must have the form offset:interval", piece);

        TimeSeriesDownsamplingLevel level;
        level.offset_seconds = parseDurationSeconds(trim(piece.substr(0, colon)), piece);
        level.interval_seconds = parseDurationSeconds(trim(piece.substr(colon + 1)), piece);

        if (!result.levels.empty())
        {
            const auto & previous = result.levels.back();
            if (level.offset_seconds <= previous.offset_seconds)
                throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                    "Offsets in `samples_downsampling_periods` must increase, but {}s follows {}s",
                    level.offset_seconds, previous.offset_seconds);
            if (level.interval_seconds % previous.interval_seconds != 0)
                throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                    "Interval {}s in `samples_downsampling_periods` must be a multiple of the previous interval {}s",
                    level.interval_seconds, previous.interval_seconds);
        }

        if (level.offset_seconds % level.interval_seconds != 0)
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                "Offset {}s in `samples_downsampling_periods` must be a multiple of its interval {}s",
                level.offset_seconds, level.interval_seconds);

        result.levels.push_back(level);
    }

    return result;
}

const TimeSeriesDownsamplingLevel * TimeSeriesDownsamplingPeriods::levelForAge(UInt64 age_seconds) const
{
    const TimeSeriesDownsamplingLevel * found = nullptr;
    for (const auto & level : levels)
    {
        if (level.offset_seconds > age_seconds)
            break;
        found = &level;
    }
    return found;
}

std::vector<UInt64> TimeSeriesDownsamplingPeriods::sortingSpans(UInt64 bucket_step_seconds) const
{
    std::vector<UInt64> spans;
    for (const auto & level : levels)
    {
        const UInt64 span = std::max(level.interval_seconds, bucket_step_seconds);
        if (span > bucket_step_seconds && (spans.empty() || spans.back() != span))
            spans.push_back(span);
    }
    std::reverse(spans.begin(), spans.end());
    return spans;
}

void TimeSeriesDownsamplingPeriods::validateForTable(UInt64 bucket_step_seconds, UInt64 min_offset_seconds) const
{
    if (levels.empty())
        return;

    if (levels.front().offset_seconds < min_offset_seconds)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "The first offset in `samples_downsampling_periods` must be at least {}s, the recent samples retention, got {}s",
            min_offset_seconds, levels.front().offset_seconds);

    for (const auto & level : levels)
    {
        if ((level.interval_seconds % bucket_step_seconds != 0) && (bucket_step_seconds % level.interval_seconds != 0))
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                "Interval {}s in `samples_downsampling_periods` must divide samples_bucket_step_seconds ({}s) or be a multiple of it",
                level.interval_seconds, bucket_step_seconds);
    }
}

}
