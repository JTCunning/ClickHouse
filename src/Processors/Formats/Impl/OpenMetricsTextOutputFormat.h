#pragma once

#include <Processors/Formats/Impl/PrometheusTextOutputFormat.h>

namespace DB
{

/// OpenMetrics text exposition (https://openmetrics.io/).
/// Extends Prometheus text layout with optional `# UNIT` and a trailing `# EOF` line.
class OpenMetricsTextOutputFormat final : public PrometheusTextOutputFormat
{
public:
    OpenMetricsTextOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_);

    String getName() const override { return "OpenMetricsTextOutputFormat"; }

private:
    void writeAdditionalFamilyMetadata() override;
    void finalizeImpl() override;
    bool useOpenMetricsTimestampRules() const override { return true; }
};

}
