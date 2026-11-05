#pragma once

#include <base/types.h>

#include <string_view>
#include <vector>


namespace DB
{

/// Parses a comma-separated list of PromQL durations (`60s`, `5m`, `1h`) into whole seconds.
/// Rejects empty tokens, fractional seconds, and non-positive durations.
std::vector<UInt64> parseTimeSeriesRollupSteps(std::string_view input);

/// Formats rollup steps as a canonical ascending comma-separated list of second values (`60, 300, 3600`).
String formatTimeSeriesRollupSteps(const std::vector<UInt64> & steps_seconds);

}
