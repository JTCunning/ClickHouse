#!/usr/bin/env bash
# Handoff #19: the steady-state variants, one after the other on the same server (see README "Steady state (#19)").
# Usage: steady.sh <name:variant[:ENV=v,...]> ...   e.g. steady.sh s_base:base s_tr:trcache3600 s_tra:trcache3600:ALIGN=1 s_b5:batch5
# Each run: HISTORY dead series merged, then SECONDS (105) of inserts at RATE (6/s) x SLICE (3000) series, the last TAIL (15) seconds with
# the merges of the tags and time ranges tables stopped. Writes <name>.parts.json and <name>.log in the current directory.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
for spec in "$@"; do
  IFS=: read -r name variant envs <<<"$spec"
  env TAIL=${TAIL:-15} ${envs//,/ } python3 "$HERE/loop.py" "$variant" "$name" "${SECONDS_RUN:-105}" "${RATE:-6}" "${SLICE:-3000}" > "$name.log" 2>&1
  python3 -c "import json; d=json.load(open('$name.parts.json'))['summary']; print('$name', d['variant'], d['statements'], d['rate'], d['lat_ms_p50'], d['tr_rows_emulated'], d['final_parts'], d['final_rows_ids'])"
done
