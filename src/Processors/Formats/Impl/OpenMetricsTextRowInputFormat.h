#pragma once

#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>

#include <unordered_map>

namespace DB
{

class ReadBuffer;

/// Parses OpenMetrics text (and common Prometheus text exposition).
class OpenMetricsTextRowInputFormat final : public IRowInputFormat
{
public:
    OpenMetricsTextRowInputFormat(SharedHeader header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_);

    String getName() const override { return "OpenMetricsTextRowInputFormat"; }
    void resetParser() override;

private:
    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;
    void readPrefix() override;

    const FormatSettings format_settings;

    struct FamilyMeta
    {
        String help;
        String type;
        String unit;
    };

    std::unordered_map<String, FamilyMeta> family_meta;
    bool saw_eof = false;
};

class OpenMetricsTextSchemaReader : public IExternalSchemaReader
{
public:
    NamesAndTypesList readSchema() override;
};

}
