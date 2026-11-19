#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Common/VectorWithMemoryTracking.h>

#include <optional>


namespace DB
{

/// A matcher of a time series selector, in the form the index of type `time_series` evaluates.
/// A label which a time series does not have has an empty value.
struct TimeSeriesIndexMatcher
{
    enum class Kind : uint8_t
    {
        Equal,
        NotEqual,
        Regex,
        NotRegex,
    };

    String label;
    Kind kind = Kind::Equal;

    /// The value for `Equal` and `NotEqual`; a regular expression matching whole values for `Regex` and `NotRegex`.
    String value;

    /// The terms of the regular expression if it is a list of literal alternatives.
    std::optional<Strings> alternatives;
};

using TimeSeriesIndexMatchers = VectorWithMemoryTracking<TimeSeriesIndexMatcher>;

/// The time series selected by a lookup: the identifiers and the labels sorted by name.
struct TimeSeriesIndexLookupResult
{
    using Labels = VectorWithMemoryTracking<std::pair<String, String>>;
    using LabelsPtr = std::shared_ptr<const Labels>;

    MutableColumnPtr ids;
    VectorWithMemoryTracking<LabelsPtr> labels;
};

/// An index of the time series of a part: `INDEX name (id, tags, metric_name) TYPE time_series(metric_name = '__name__')`.
///
/// The first column of the index is the identifier of a time series. A column of type `Map` gives one label per key,
/// and each other column gives one label, named by an argument of the index or by the column.
/// A column of type `DateTime`, `DateTime64`, or `UInt32` is not a label, it is the heartbeat of a time series: a lookup can skip
/// the rows with an old heartbeat.
/// All the labels are equal for the index: each of them has a sorted dictionary of values with a posting list per value.
///
/// The index has one granule per part and four files:
///   - the header: the list of labels, the list of sets of label names, and the positions of the blocks of series;
///   - the dictionary: the sorted values of each label in blocks, each value with its posting list or a reference to it,
///     and a sparse index of the blocks of each label;
///   - the postings: the posting lists which are too long to be kept in the dictionary, as Roaring bitmaps of row numbers;
///   - the series: for each row the identifier, the set of label names and the numbers of the values in the dictionaries.
///
/// Queries on the table do not use the index for filtering. `lookup` reads the index files of the parts directly
/// and returns the identifiers and the labels of the matched time series, so a selector does not read the table.
struct MergeTreeIndexTimeSeries final : public IMergeTreeIndex
{
    /// Where the labels of a time series come from.
    struct LabelSource
    {
        size_t column_position = 0;
        bool is_map = false;
        /// The name of the label if the source is not a map.
        String label_name;
    };

    using LabelSources = VectorWithMemoryTracking<LabelSource>;

    MergeTreeIndexTimeSeries(StorageMetadataPtr metadata_snapshot_, const IndexDescription & index_);

    /// `heartbeat_column_position` receives the position of the column with heartbeats in the index, if there is such a column.
    static LabelSources parseLabelSources(const IndexDescription & description, std::optional<size_t> * heartbeat_column_position = nullptr);

    /// Whether the index can be built for a column with identifiers and columns with labels of these types.
    static bool supportsColumnTypes(const DataTypePtr & id_type, const DataTypes & label_types);

    [[nodiscard]] MergeTreeIndexSubstreams getSubstreams() const override;
    [[nodiscard]] MergeTreeIndexFormat getPhysicalFormat(
        const MergeTreeDataPartChecksums & checksums, const IDataPartStorage & storage, const std::string & relative_path_prefix) const override;

    [[nodiscard]] MergeTreeIndexGranulePtr createIndexGranule() const override;
    [[nodiscard]] MergeTreeIndexAggregatorPtr createIndexAggregator() const override;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const override;

    /// Finds the time series matching all `matchers` in `parts`. A time series stored in several parts is returned once.
    /// With `min_heartbeat` the rows with a heartbeat not greater than it are skipped (if the index has a column with heartbeats).
    [[nodiscard]] TimeSeriesIndexLookupResult lookup(
        const MergeTreeData::DataPartsVector & parts,
        const TimeSeriesIndexMatchers & matchers,
        std::optional<Int64> min_heartbeat,
        const ContextPtr & context) const;

    DataTypePtr id_type;
    /// The size of the binary representation of an identifier.
    size_t id_width = 0;
    /// The position of the column with heartbeats in the index.
    std::optional<size_t> heartbeat_column_position;
    LabelSources label_sources;
};

}
