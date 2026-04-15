#include <Processors/Formats/Impl/OpenMetricsTextRowInputFormat.h>

#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/assert_cast.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBuffer.h>
#include <IO/readFloatText.h>
#include <IO/readIntText.h>

#include <algorithm>
#include <limits>
#include <map>


namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int INCORRECT_QUERY;
}

namespace
{
constexpr auto FORMAT_NAME = "OpenMetrics";

void skipAsciiSpaces(std::string_view s, size_t & pos)
{
    while (pos < s.size() && (s[pos] == ' ' || s[pos] == '\t'))
        ++pos;
}

bool tryConsume(std::string_view s, size_t & pos, char c)
{
    skipAsciiSpaces(s, pos);
    if (pos < s.size() && s[pos] == c)
    {
        ++pos;
        return true;
    }
    return false;
}

/// Read a Prometheus/OpenMetrics double-quoted string; `pos` at opening `"`.
/// Escape sequences: `\\`, `\"`, `\n` (https://prometheus.io/docs/instrumenting/exposition_formats/).
bool readQuotedLabelValue(std::string_view s, size_t & pos, String & out)
{
    if (pos >= s.size() || s[pos] != '"')
        return false;
    ++pos;
    out.clear();
    while (pos < s.size())
    {
        if (s[pos] == '"')
        {
            ++pos;
            return true;
        }
        if (s[pos] == '\\')
        {
            ++pos;
            if (pos >= s.size())
                return false;
            switch (s[pos])
            {
                case '\\':
                    out.push_back('\\');
                    break;
                case '"':
                    out.push_back('"');
                    break;
                case 'n':
                    out.push_back('\n');
                    break;
                default:
                    return false;
            }
            ++pos;
            continue;
        }
        out.push_back(s[pos]);
        ++pos;
    }
    return false;
}

bool parseLabelSet(std::string_view s, size_t & pos, std::map<String, String> & labels)
{
    skipAsciiSpaces(s, pos);
    if (pos >= s.size() || s[pos] != '{')
        return true;
    ++pos;
    while (true)
    {
        skipAsciiSpaces(s, pos);
        if (pos < s.size() && s[pos] == '}')
        {
            ++pos;
            return true;
        }
        size_t key_start = pos;
        while (pos < s.size() && s[pos] != '=' && s[pos] != '}')
            ++pos;
        if (pos >= s.size())
            return false;
        String key{s.substr(key_start, pos - key_start)};
        if (!tryConsume(s, pos, '='))
            return false;
        String value;
        if (!readQuotedLabelValue(s, pos, value))
            return false;
        auto [it, inserted] = labels.emplace(std::move(key), std::move(value));
        if (!inserted)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Duplicate label name '{}' in OpenMetrics label set", it->first);
        skipAsciiSpaces(s, pos);
        if (pos < s.size() && s[pos] == ',')
        {
            ++pos;
            continue;
        }
        if (pos < s.size() && s[pos] == '}')
        {
            ++pos;
            return true;
        }
        return false;
    }
}

/// `pos` at first char of metric stem (before `{`, ASCII space, or tab — whitespace before value).
bool parseMetricDescriptor(std::string_view s, size_t & pos, String & stem, std::map<String, String> & labels)
{
    size_t stem_start = pos;
    while (pos < s.size() && s[pos] != '{' && s[pos] != ' ' && s[pos] != '\t')
        ++pos;
    stem = String{s.substr(stem_start, pos - stem_start)};
    return parseLabelSet(s, pos, labels);
}

bool parseValueToken(std::string_view s, size_t & pos, String & value_out)
{
    skipAsciiSpaces(s, pos);
    size_t start = pos;
    while (pos < s.size() && s[pos] != ' ' && s[pos] != '\t')
        ++pos;
    value_out = String{s.substr(start, pos - start)};
    return !value_out.empty();
}

bool parseOptionalIntToken(std::string_view s, size_t & pos, String & out)
{
    skipAsciiSpaces(s, pos);
    if (pos >= s.size())
        return false;
    size_t start = pos;
    if (s[pos] == '-')
        ++pos;
    while (pos < s.size() && isNumericASCII(s[pos]))
        ++pos;
    if (pos == start)
        return false;
    out = String{s.substr(start, pos - start)};
    return true;
}

void stripExemplarSuffix(std::string_view s, size_t & pos)
{
    /// OpenMetrics: optional exemplar / histogram metadata after `#` (rest of line is not a float sample).
    skipAsciiSpaces(s, pos);
    if (pos < s.size() && s[pos] == '#')
        pos = s.size();
}

void checkMetricLineFullyConsumed(std::string_view s, size_t & pos, const String & line)
{
    skipAsciiSpaces(s, pos);
    if (pos < s.size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected trailing data in OpenMetrics line: {}", line);
}

void insertFloatFromPrometheusText(IColumn & column, const String & token)
{
    Float64 v = 0;
    if (token == "NaN")
        v = std::numeric_limits<double>::quiet_NaN();
    else if (token == "+Inf" || token == "Inf")
        v = std::numeric_limits<double>::infinity();
    else if (token == "-Inf")
        v = -std::numeric_limits<double>::infinity();
    else
    {
        ReadBufferFromString buf(token);
        if (!tryReadFloatText(v, buf) || !buf.eof())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse float value '{}' in OpenMetrics format", token);
    }
    assert_cast<ColumnFloat64 &>(column).insert(v);
}

struct ColumnLoc
{
    std::optional<size_t> name;
    std::optional<size_t> value;
    std::optional<size_t> help;
    std::optional<size_t> type;
    std::optional<size_t> labels;
    std::optional<size_t> timestamp;
    std::optional<size_t> unit;
};

ColumnLoc buildColumnLoc(const Block & header)
{
    ColumnLoc loc;
    for (size_t i = 0; i < header.columns(); ++i)
    {
        const auto & col = header.getByPosition(i).name;
        if (col == "name")
            loc.name = i;
        else if (col == "value")
            loc.value = i;
        else if (col == "help")
            loc.help = i;
        else if (col == "type")
            loc.type = i;
        else if (col == "labels")
            loc.labels = i;
        else if (col == "timestamp")
            loc.timestamp = i;
        else if (col == "unit")
            loc.unit = i;
    }
    if (!loc.name || !loc.value)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Format '{}' requires 'name' and 'value' columns", FORMAT_NAME);
    return loc;
}

void insertMapLabels(IColumn & column, const std::map<String, String> & labels)
{
    Field map_field = Map();
    Map & m = map_field.safeGet<Map>();
    for (const auto & [k, v] : labels)
        m.push_back(Tuple{k, v});
    column.insert(map_field);
}

bool isBlankAsciiLine(const String & line)
{
    for (char c : line)
        if (c != ' ' && c != '\t')
            return false;
    return true;
}

/// After `# EOF`, the exposition stream is logically finished; reject trailing payload.
void throwIfNonBlankAfterEOF(ReadBuffer & buf)
{
    while (!buf.eof())
    {
        String tail_line;
        readStringUntilNewlineInto(tail_line, buf);
        if (!buf.eof())
            buf.ignore();
        if (!tail_line.empty() && tail_line.back() == '\r')
            tail_line.pop_back();
        if (!tail_line.empty() && !isBlankAsciiLine(tail_line))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected data after # EOF in OpenMetrics input");
    }
}

}

OpenMetricsTextRowInputFormat::OpenMetricsTextRowInputFormat(
    SharedHeader header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_)
    : IRowInputFormat(std::move(header_), in_, std::move(params_))
    , format_settings(format_settings_)
{
}

void OpenMetricsTextRowInputFormat::resetParser()
{
    IRowInputFormat::resetParser();
    family_meta.clear();
    saw_eof = false;
}

void OpenMetricsTextRowInputFormat::readPrefix()
{
    skipBOMIfExists(*in);
}

bool OpenMetricsTextRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    const auto & header = getPort().getHeader();
    ColumnLoc loc = buildColumnLoc(header);

    /// Like JSONEachRowRowInputFormat::checkEndOfData: if we return false with no row, leave read_columns
    /// empty so IRowInputFormat does not treat all-zero read_columns as "missing values" (Code 7).
    ext.read_columns.clear();

    while (!in->eof() && !saw_eof)
    {
        String line;
        readStringUntilNewlineInto(line, *in);
        if (!in->eof())
            in->ignore(); /// Skip '\n'

        if (!line.empty() && line.back() == '\r')
            line.pop_back();

        if (line.empty())
            continue;

        if (line.starts_with("#"))
        {
            if (line == "# EOF" || (line.starts_with("# EOF") && (line.size() == 5 || line[5] == ' ' || line[5] == '\t')))
            {
                throwIfNonBlankAfterEOF(*in);
                saw_eof = true;
                continue;
            }

            /// `# HELP name text`
            if (line.starts_with("# HELP "))
            {
                std::string_view sv{line};
                size_t p = 7;
                skipAsciiSpaces(sv, p);
                size_t name_start = p;
                while (p < sv.size() && sv[p] != ' ' && sv[p] != '\t')
                    ++p;
                String name{sv.substr(name_start, p - name_start)};
                skipAsciiSpaces(sv, p);
                String help{sv.substr(p)};
                family_meta[name].help = String(help);
                continue;
            }
            if (line.starts_with("# TYPE "))
            {
                std::string_view sv{line};
                size_t p = 7;
                skipAsciiSpaces(sv, p);
                size_t name_start = p;
                while (p < sv.size() && sv[p] != ' ' && sv[p] != '\t')
                    ++p;
                String name{sv.substr(name_start, p - name_start)};
                skipAsciiSpaces(sv, p);
                String typ{sv.substr(p)};
                family_meta[name].type = String(typ);
                continue;
            }
            if (line.starts_with("# UNIT "))
            {
                std::string_view sv{line};
                size_t p = 7;
                skipAsciiSpaces(sv, p);
                size_t name_start = p;
                while (p < sv.size() && sv[p] != ' ' && sv[p] != '\t')
                    ++p;
                String name{sv.substr(name_start, p - name_start)};
                skipAsciiSpaces(sv, p);
                String unit{sv.substr(p)};
                family_meta[name].unit = String(unit);
                continue;
            }
            /// Other metadata / comments: ignore
            continue;
        }

        /// Metric line
        std::string_view sv{line};
        size_t pos = 0;
        String stem;
        std::map<String, String> labels;
        if (!parseMetricDescriptor(sv, pos, stem, labels))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse labels in OpenMetrics line: {}", line);

        if (stem.empty())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Empty metric name in OpenMetrics line: {}", line);

        skipAsciiSpaces(sv, pos);
        String value_token;
        if (!parseValueToken(sv, pos, value_token))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse value in OpenMetrics line: {}", line);

        String ts_token;
        bool has_ts = parseOptionalIntToken(sv, pos, ts_token);
        stripExemplarSuffix(sv, pos);
        checkMetricLineFullyConsumed(sv, pos, line);

        /// Resolve logical metric name (histogram/summary suffixes).
        String logical_name = stem;

        auto applyStemSuffix = [&]()
        {
            if (stem.ends_with("_bucket") && stem.size() > 7)
            {
                String base{stem.substr(0, stem.size() - 7)};
                auto it = family_meta.find(base);
                if (it != family_meta.end() && (it->second.type == "histogram" || it->second.type == "summary"))
                    logical_name = base;
            }
            else if (stem.ends_with("_sum") && stem.size() > 4)
            {
                String base{stem.substr(0, stem.size() - 4)};
                if (family_meta.contains(base))
                {
                    logical_name = base;
                    labels["sum"] = "";
                }
            }
            else if (stem.ends_with("_count") && stem.size() > 6)
            {
                String base{stem.substr(0, stem.size() - 6)};
                if (family_meta.contains(base))
                {
                    logical_name = base;
                    labels["count"] = "";
                }
            }
        };
        applyStemSuffix();

        /// Do not use operator[] here: it would insert an empty entry for every unseen metric name
        /// (high-cardinality streams). Metadata comes only from # HELP / # TYPE / # UNIT lines.
        static const FamilyMeta empty_family_meta;
        auto meta_it = family_meta.find(logical_name);
        const FamilyMeta & fm = (meta_it != family_meta.end()) ? meta_it->second : empty_family_meta;

        ext.read_columns.assign(columns.size(), 0);

        if (loc.name)
        {
            assert_cast<ColumnString &>(*columns[*loc.name]).insertData(logical_name.data(), logical_name.size());
            ext.read_columns[*loc.name] = 1;
        }
        if (loc.value)
        {
            insertFloatFromPrometheusText(*columns[*loc.value], value_token);
            ext.read_columns[*loc.value] = 1;
        }
        if (loc.help)
        {
            assert_cast<ColumnString &>(*columns[*loc.help]).insertData(fm.help.data(), fm.help.size());
            ext.read_columns[*loc.help] = 1;
        }
        if (loc.type)
        {
            assert_cast<ColumnString &>(*columns[*loc.type]).insertData(fm.type.data(), fm.type.size());
            ext.read_columns[*loc.type] = 1;
        }
        if (loc.labels)
        {
            insertMapLabels(*columns[*loc.labels], labels);
            ext.read_columns[*loc.labels] = 1;
        }
        if (loc.timestamp)
        {
            auto & col = *columns[*loc.timestamp];
            if (col.isNullable())
            {
                auto & nullable_col = assert_cast<ColumnNullable &>(col);
                if (has_ts)
                {
                    Int64 t = 0;
                    ReadBufferFromString buf(ts_token);
                    readIntText(t, buf);
                    nullable_col.getNestedColumn().insert(t);
                    nullable_col.getNullMapColumn().insertValue(0);
                }
                else
                    nullable_col.insertDefault();
            }
            else
            {
                if (!has_ts)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Timestamp column is not Nullable but line has no timestamp: {}", line);
                Int64 t = 0;
                ReadBufferFromString buf(ts_token);
                readIntText(t, buf);
                assert_cast<ColumnInt64 &>(col).insert(t);
            }
            ext.read_columns[*loc.timestamp] = 1;
        }
        if (loc.unit)
        {
            assert_cast<ColumnString &>(*columns[*loc.unit]).insertData(fm.unit.data(), fm.unit.size());
            ext.read_columns[*loc.unit] = 1;
        }

        return true;
    }

    ext.read_columns.clear();
    return false;
}

NamesAndTypesList OpenMetricsTextSchemaReader::readSchema()
{
    return {
        {"name", std::make_shared<DataTypeString>()},
        {"value", std::make_shared<DataTypeFloat64>()},
        {"help", std::make_shared<DataTypeString>()},
        {"type", std::make_shared<DataTypeString>()},
        {"labels", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>())},
        {"timestamp", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())},
        {"unit", std::make_shared<DataTypeString>()},
    };
}

void registerInputFormatOpenMetrics(FormatFactory & factory)
{
    factory.registerInputFormat(
        FORMAT_NAME,
        [](ReadBuffer & buf, const Block & sample, IRowInputFormat::Params params, const FormatSettings & settings)
        { return std::make_shared<OpenMetricsTextRowInputFormat>(std::make_shared<const Block>(sample), buf, std::move(params), settings); });

    factory.registerExternalSchemaReader(
        FORMAT_NAME,
        [](const FormatSettings &)
        { return std::make_shared<OpenMetricsTextSchemaReader>(); });

    factory.markFormatSupportsSubsetOfColumns(FORMAT_NAME);
}

}
