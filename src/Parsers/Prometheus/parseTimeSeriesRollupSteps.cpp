#include <Parsers/Prometheus/parseTimeSeriesRollupSteps.h>

#include <Common/Exception.h>
#include <Parsers/Prometheus/PrometheusQueryParsingUtil.h>

#include <algorithm>
#include <cctype>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
    void trimInPlace(std::string_view & s)
    {
        while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front())))
            s.remove_prefix(1);
        while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back())))
            s.remove_suffix(1);
    }
}

std::vector<UInt64> parseTimeSeriesRollupSteps(std::string_view input)
{
    std::vector<UInt64> steps;
    size_t pos = 0;
    while (pos <= input.size())
    {
        const size_t comma = input.find(',', pos);
        std::string_view token = input.substr(pos, (comma == std::string_view::npos) ? std::string_view::npos : (comma - pos));
        pos = (comma == std::string_view::npos) ? (input.size() + 1) : (comma + 1);

        trimInPlace(token);
        if (token.empty())
        {
            if (comma == std::string_view::npos && steps.empty())
                break; /// empty setting value
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid samples_rollup_steps: empty duration in '{}'", input);
        }

        /// The duration is parsed in milliseconds, so that a duration which isn't a whole number of seconds (`1500ms`)
        /// is rejected instead of being truncated.
        PrometheusQueryParsingUtil::DurationType duration{};
        String error;
        if (!PrometheusQueryParsingUtil::tryParseDuration(token, /* duration_scale = */ 3, duration, &error)
            || (duration <= 0))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Invalid samples_rollup_steps: cannot parse duration '{}'{}"
                "{}",
                token,
                error.empty() ? "" : ": ",
                error);
        }

        if (duration.value % 1000 != 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid samples_rollup_steps: duration '{}' is not a whole number of seconds", token);
        const UInt64 step_seconds = static_cast<UInt64>(duration.value / 1000);
        if (std::find(steps.begin(), steps.end(), step_seconds) != steps.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid samples_rollup_steps: duplicate duration '{}'", token);
        steps.push_back(step_seconds);
    }

    std::sort(steps.begin(), steps.end());
    return steps;
}

String formatTimeSeriesRollupSteps(const std::vector<UInt64> & steps_seconds)
{
    String result;
    for (size_t i = 0; i < steps_seconds.size(); ++i)
    {
        if (i)
            result += ", ";
        result += std::to_string(steps_seconds[i]);
    }
    return result;
}

}
