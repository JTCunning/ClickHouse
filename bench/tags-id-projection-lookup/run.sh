#!/usr/bin/env bash
# Emulates the proposed v12 tags table (the v11 inner tags table of master plus the `tags_by_id` projection)
# and times the lookup of 17,000 missed ids (8,500 stored, 8,500 new) against a scan of the parent table
# and against writing the same number of tags rows. Needs only a master binary (clickhouse local).
# Usage: CH=/path/to/clickhouse ./run.sh <data dir>
set -euo pipefail
CH=${CH:-clickhouse}; D=${1:?data dir}; S=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$D" && cd "$D"
q() { "$CH" local --path . --max_memory_usage=2500000000 "$@"; }
q --queries-file "$S/create.sql"                                 # 300k series, merges stopped
q --queries-file "$S/lookup.sql" | grep -E 'explain-|ReadFrom|Granules|Ranges'   # projection ranges, with and without the settings
echo '300k: 6x projection lookup, then 6x parent scan (s)'; q --time --queries-file "$S/time.sql" 2>&1 >/dev/null | tail -12 | tr '\n' ' '; echo
q --print-profile-events --queries-file "$S/prof.sql" 2>&1 | grep -E 'FilteringMarksWithPrimaryKeyMicroseconds|SelectedRanges|SelectedMarks:' | tail -3
q --queries-file "$S/grow.sql"                                   # to 3M series; sizes of the projection and the parent
echo '3M: 6x projection lookup, then 6x parent scan (s)'; q --time --queries-file "$S/time.sql" 2>&1 >/dev/null | tail -12 | tr '\n' ' '; echo
echo 'write 17k tags rows: 6x v11 shape, then 6x v12 shape (s)'; q --time --queries-file "$S/ins.sql" 2>&1 >/dev/null | tail -12 | tr '\n' ' '; echo
echo '3M, Tuple(UInt64, UUID) id: 5x projection lookup (s)'; q --time --queries-file "$S/plain.sql" 2>&1 >/dev/null | tail -5 | tr '\n' ' '; echo
