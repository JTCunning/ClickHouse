#!/usr/bin/env bash
# A/B harness for rollup columns of TimeSeries samples tables (branch lilred-perf-rollup-samples).
# Design and pass rule: ab-plans/rollup-samples.md on lilred-research.
#
#   run.sh <branch clickhouse binary> <master clickhouse binary | -> <work dir>
#
# Environment (defaults in brackets):
#   SERIES [2000]        series in total: half gauges (ab_gauge), half counters (ab_counter_total)
#   DAYS [30]            days of data ending at the current hour, one sample every SCRAPE seconds
#   SCRAPE [15]
#   SHAPE [jittered]     jittered: per-series offset in [0, SCRAPE) plus up to 40 ms; aligned: exact multiples of SCRAPE
#   STEPS ['1m, 5m, 1h'] samples_rollup_steps of the rollup table
#   RUNS [20]            timed runs per query and arm (interleaved), after one warm-up run
#   MAX_THREADS [16]
#   SMALL_INSERTS [200]  remote-write style inserts of one sample per series (insert cost of small batches)
#   SMOKE [0]            1: run the flow with a master binary as both binaries (no rollup settings), to check the harness
#
# Nothing is deleted: the data directories, server logs and results stay in <work dir>, also after a failure.
# A second run with the same work dir reuses the loaded data (delete <work dir>/<arm>/.loaded to reload).
set -uo pipefail

BRANCH_BIN=${1:?branch clickhouse binary}
MASTER_BIN=${2:?master clickhouse binary or -}
WORK=$(mkdir -p "${3:?work dir}" && cd "$3" && pwd)

SERIES=${SERIES:-2000}; DAYS=${DAYS:-30}; SCRAPE=${SCRAPE:-15}; SHAPE=${SHAPE:-jittered}
STEPS=${STEPS:-1m, 5m, 1h}; RUNS=${RUNS:-20}; MAX_THREADS=${MAX_THREADS:-16}
SMALL_INSERTS=${SMALL_INSERTS:-200}; SMOKE=${SMOKE:-0}

RES="$WORK/results"; mkdir -p "$RES"
LOG="$RES/harness.log"
log() { echo "[$(date -u +%FT%TZ)] $*" | tee -a "$LOG"; }
die() { log "FAILED: $*"; log "data, server logs and results are kept in $WORK"; exit 1; }

PIDS=()
cleanup() {
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then kill "$pid"; fi
    done
    for pid in "${PIDS[@]}"; do
        for _ in $(seq 60); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
    done
}
trap cleanup EXIT

if [ "$SMOKE" = 1 ]; then ROLLUP_SETTING=""; PREFER_ON=""; PREFER_OFF=""; else
    ROLLUP_SETTING=", samples_rollup_steps = '$STEPS'"
    PREFER_ON=", time_series_prefer_rollup_tables = 1"; PREFER_OFF=", time_series_prefer_rollup_tables = 0"; fi

# ---------------------------------------------------------------- servers
# arm name, binary, tcp port
ARMS=("branch $BRANCH_BIN 19101")
[ "$MASTER_BIN" != "-" ] && ARMS+=("master $MASTER_BIN 19102")

write_config() { # dir port
    local dir=$1 port=$2
    mkdir -p "$dir"/{data,tmp,log,user_files}
    cat > "$dir/config.xml" <<EOF
<clickhouse>
    <logger><level>information</level><log>$dir/log/server.log</log><errorlog>$dir/log/server.err.log</errorlog></logger>
    <listen_host>127.0.0.1</listen_host>
    <tcp_port>$port</tcp_port>
    <path>$dir/data/</path><tmp_path>$dir/tmp/</tmp_path><user_files_path>$dir/user_files/</user_files_path>
    <user_directories><users_xml><path>$dir/users.xml</path></users_xml></user_directories>
    <query_log><database>system</database><table>query_log</table><flush_interval_milliseconds>1000</flush_interval_milliseconds></query_log>
    <part_log><database>system</database><table>part_log</table><flush_interval_milliseconds>1000</flush_interval_milliseconds></part_log>
    <mark_cache_size>5368709120</mark_cache_size>
</clickhouse>
EOF
    cat > "$dir/users.xml" <<EOF
<clickhouse>
    <profiles><default>
        <allow_experimental_time_series_table>1</allow_experimental_time_series_table>
        <enable_time_series_aggregate_functions>1</enable_time_series_aggregate_functions>
        <log_queries>1</log_queries>
        <session_timezone>UTC</session_timezone>
    </default></profiles>
    <users><default><password></password><networks><ip>::/0</ip></networks><profile>default</profile><quota>default</quota></default></users>
    <quotas><default></default></quotas>
</clickhouse>
EOF
}

start_server() { # arm bin port
    local arm=$1 bin=$2 port=$3 dir="$WORK/$1"
    write_config "$dir" "$port"
    "$bin" server --config-file="$dir/config.xml" > "$dir/log/stdout.log" 2>&1 &
    local pid=$!
    PIDS+=("$pid")
    echo "$pid" > "$dir/server.pid"
    for _ in $(seq 120); do
        if "$bin" client --host 127.0.0.1 --port "$port" -q "SELECT 1" > /dev/null 2>&1; then
            log "$arm server up on $port, pid $pid, $("$bin" client --host 127.0.0.1 --port "$port" -q 'SELECT version()')"
            return 0
        fi
        kill -0 "$pid" 2>/dev/null || break
        sleep 1
    done
    die "$arm server did not start, see $dir/log"
}

ch() { # arm, then client args
    local arm=$1; shift
    local bin="" port="" name b p
    for a in "${ARMS[@]}"; do read -r name b p <<< "$a"; if [ "$name" = "$arm" ]; then bin=$b; port=$p; fi; done
    "$bin" client --host 127.0.0.1 --port "$port" "$@"
}

for a in "${ARMS[@]}"; do read -r name b p <<< "$a"; start_server "$name" "$b" "$p"; done

# ---------------------------------------------------------------- data
# The end of the data is the current hour, kept in meta.env so that a rerun uses the same data and queries.
if [ -f "$WORK/meta.env" ]; then . "$WORK/meta.env"; else
    T_END=$(( $(date +%s) / 3600 * 3600 )); echo "T_END=$T_END" > "$WORK/meta.env"; fi
T0=$(( T_END - DAYS * 86400 ))
PER_HOUR=$(( 3600 / SCRAPE ))
HALF=$(( SERIES / 2 ))
if [ "$SHAPE" = aligned ]; then TS="toDateTime64({T} + {J} * $SCRAPE, 3)"; else
    TS="toDateTime64({T} + {J} * $SCRAPE + (sipHash64(s, 'off') % ($SCRAPE * 1000) + sipHash64(s, {J}, 'jit') % 40) / 1000, 3)"; fi
ts_expr() { local e=${TS//\{T\}/$1}; echo "${e//\{J\}/$2}"; }   # base, sample index expression
# Gauges: a sine per series with noise and rare spikes. Counters: a varying rate and a reset every 7-31 hours.
GAUGE="100 + 50 * sin(j / 40 + s) + (sipHash64(s, j, 'n') % 20) + if(sipHash64(s, j, 's') % 500 = 0, 300, 0)"
COUNTER="(1 + sipHash64(s, 'r') % 100) * ((j % P) + 45 * sin((j % P) / 50))"
# One insert per hour of data and metric: every series with its PER_HOUR samples of that hour.
# j counts samples from T0, P is the reset period of a counter in samples.
insert_hour() { # hour table
    local h=$1 ts
    ts=$(ts_expr "$T0" j)
    echo "INSERT INTO $2 (metric_name, tags, samples)
      SELECT 'ab_gauge', map('instance', toString(s), 'job', toString(s % 8), 'dc', toString(s % 4)),
             arrayMap(j -> ($ts, toFloat64($GAUGE)), range($h * $PER_HOUR, ($h + 1) * $PER_HOUR))
      FROM (SELECT number AS s FROM numbers($HALF));
    INSERT INTO $2 (metric_name, tags, samples)
      SELECT 'ab_counter_total', map('instance', toString(s), 'job', toString(s % 8), 'dc', toString(s % 4)),
             arrayMap(j -> ($ts, toFloat64($COUNTER)), range($h * $PER_HOUR, ($h + 1) * $PER_HOUR))
      FROM (SELECT number + $HALF AS s, (7 + sipHash64(number, 'p') % 24) * $PER_HOUR AS P FROM numbers($HALF));"
}

# tables: name, settings, arms that load it
TABLES=("ab_rollup|recent_samples_ttl_seconds = 345600$ROLLUP_SETTING|branch"
        "ab_plain|recent_samples_ttl_seconds = 345600|branch master")
[ "$SMOKE" != 1 ] && TABLES+=("ab_rollup_coarse|recent_samples_ttl_seconds = 345600, samples_rollup_steps = '5m, 1h'|branch")

load() { # arm table settings
    local arm=$1 t=$2 s=$3
    [ -f "$WORK/$arm/.loaded.$t" ] && { log "$arm.$t already loaded"; return 0; }
    log "$arm: loading $t ($SERIES series x $DAYS days, one insert per hour and metric)"
    ch "$arm" -q "DROP TABLE IF EXISTS $t SYNC" || die "drop $arm.$t"
    ch "$arm" -q "CREATE TABLE $t ENGINE = TimeSeries SETTINGS $s" || die "create $arm.$t"
    local start mid end
    start=$(date +%s.%N)
    for h in $(seq 0 $(( DAYS * 24 - 1 ))); do insert_hour "$h" "$t"; done \
        | ch "$arm" -n --log_comment "ab-insert:$t" || die "insert $arm.$t"
    mid=$(date +%s.%N)
    ch "$arm" -q "OPTIMIZE TABLE $t FINAL" --log_comment "ab-optimize:$t" || die "optimize $arm.$t"
    end=$(date +%s.%N)
    [ -f "$RES/load_wall.tsv" ] || echo -e "arm\ttable\tinsert_wall_s\toptimize_wall_s" > "$RES/load_wall.tsv"
    echo -e "$arm\t$t\t$(awk "BEGIN { print $mid - $start }")\t$(awk "BEGIN { print $end - $mid }")" >> "$RES/load_wall.tsv"
    # Small remote-write style batches: one sample per series per insert, after the end of the data.
    ch "$arm" -q "DROP TABLE IF EXISTS ${t}_small SYNC" || die "drop $arm.${t}_small"
    ch "$arm" -q "CREATE TABLE ${t}_small ENGINE = TimeSeries SETTINGS $s" || die "create $arm.${t}_small"
    for i in $(seq 0 $(( SMALL_INSERTS - 1 ))); do
        echo "INSERT INTO ${t}_small (metric_name, tags, samples)
          SELECT 'ab_gauge', map('instance', toString(s), 'job', toString(s % 8), 'dc', toString(s % 4)),
                 [($(ts_expr "$T_END" "$i"), toFloat64(s))] FROM (SELECT number AS s FROM numbers($SERIES));"
    done | ch "$arm" -n --log_comment "ab-small:$t" || die "small inserts $arm.$t"
    touch "$WORK/$arm/.loaded.$t"
}

for spec in "${TABLES[@]}"; do
    IFS='|' read -r t s arms <<< "$spec"
    for arm in $arms; do
        for a in "${ARMS[@]}"; do read -r name _ _ <<< "$a"; [ "$name" = "$arm" ] && load "$arm" "$t" "$s"; done
    done
done

# ---------------------------------------------------------------- insert cost and storage
for a in "${ARMS[@]}"; do read -r arm _ _ <<< "$a"
    ch "$arm" -q "SYSTEM FLUSH LOGS"
    ch "$arm" -q "SELECT '$arm' AS arm, replaceRegexpOne(log_comment, '^ab-[a-z]+:', '') AS t, extract(log_comment, '^ab-([a-z]+)') AS kind,
                         count() AS queries, round(sum(query_duration_ms) / 1000, 2) AS total_s, round(avg(query_duration_ms), 1) AS avg_ms,
                         round(sum(written_rows)) AS written_rows, formatReadableSize(sum(written_bytes)) AS written
                  FROM system.query_log WHERE type = 'QueryFinish' AND log_comment LIKE 'ab-%' AND log_comment NOT LIKE 'ab-q:%'
                  GROUP BY t, kind ORDER BY t, kind FORMAT TSVWithNames" > "$RES/insert_$arm.tsv"
    ch "$arm" -q "SELECT '$arm' AS arm, database, table, column, sum(column_data_compressed_bytes) AS compressed, sum(column_data_uncompressed_bytes) AS uncompressed
                  FROM system.parts_columns WHERE active AND database = currentDatabase() AND table LIKE '.inner_id.%'
                  GROUP BY database, table, column ORDER BY table, column FORMAT TSVWithNames" > "$RES/storage_$arm.tsv"
    ch "$arm" -q "SELECT '$arm' AS arm, t.name AS ts_table, p.table AS inner_table, p.event_type, count() AS events, sum(p.duration_ms) AS ms
                  FROM system.part_log AS p LEFT JOIN (SELECT name, toString(uuid) AS u FROM system.tables WHERE database = currentDatabase() AND engine = 'TimeSeries') AS t
                  ON p.table LIKE concat('%', t.u)
                  WHERE p.database = currentDatabase() AND p.event_type IN ('NewPart', 'MergeParts')
                  GROUP BY ts_table, inner_table, p.event_type ORDER BY ts_table, inner_table, p.event_type FORMAT TSVWithNames" > "$RES/partlog_$arm.tsv"
done
log "insert cost: $RES/insert_*.tsv, storage: $RES/storage_*.tsv, merges: $RES/partlog_*.tsv"

# ---------------------------------------------------------------- queries
# case id | PromQL | window seconds | step
FUNCS=("selector|ab_gauge" "rate|rate(ab_counter_total[5m])" "increase|increase(ab_counter_total[1h])"
       "avg_over_time|avg_over_time(ab_gauge[1h])" "max_over_time|max_over_time(ab_gauge[1h])")
CASES=()
for w in 86400 604800 2592000; do
    [ "$w" -gt $(( DAYS * 86400 )) ] && continue
    for st in 15s:15 1m:60 5m:300 1h:3600; do
        IFS=: read -r sname ssec <<< "$st"
        # Prometheus refuses more than 11,000 points per series; so do we.
        if [ $(( w / ssec )) -gt 11000 ]; then echo -e "$w\t$sname\tskipped: $(( w / ssec )) points" >> "$RES/skipped.tsv"; continue; fi
        for f in "${FUNCS[@]}"; do IFS='|' read -r fid q <<< "$f"; CASES+=("${fid}_$((w / 86400))d_$sname|$q|$w|$sname"); done
    done
done
log "${#CASES[@]} query cases, $RUNS runs each"

query_sql() { # promql window step table
    echo "SELECT * FROM prometheusQueryRange('$4', '$1', $(( T_END - $2 )), $T_END, '$3')"
}

# arms of a query: label | server | table | extra settings
QARMS=("rollup|branch|ab_rollup|$PREFER_ON" "raw|branch|ab_rollup|$PREFER_OFF")
[ "$MASTER_BIN" != "-" ] && QARMS+=("master|master|ab_plain|")

run_query() { # label server table extra case run promql window step
    local label=$1 server=$2 table=$3 extra=$4 cid=$5 run=$6
    ch "$server" -q "$(query_sql "$7" "$8" "$9" "$table") FORMAT Null SETTINGS max_threads = $MAX_THREADS, log_comment = 'ab-q:$cid:$label:$run'$extra" \
        || echo -e "$cid\t$label\t$run" >> "$RES/query_errors.tsv"
}

for run in $(seq 0 "$RUNS"); do         # run 0 is the warm-up
    for c in "${CASES[@]}"; do
        IFS='|' read -r cid q w st <<< "$c"
        for qa in "${QARMS[@]}"; do IFS='|' read -r label server table extra <<< "$qa"
            run_query "$label" "$server" "$table" "$extra" "$cid" "$run" "$q" "$w" "$st"
        done
    done
    log "run $run of $RUNS done"
done

# ---------------------------------------------------------------- results equality
# Every case: rollup vs raw on the branch, raw vs master. Values compared with a relative tolerance of 1e-9
# (the order of a float sum may differ between runs); the instant selector must be bit-identical (tolerance 0).
mkdir -p "$RES/out"
for c in "${CASES[@]}"; do
    IFS='|' read -r cid q w st <<< "$c"
    for qa in "${QARMS[@]}"; do IFS='|' read -r label server table extra <<< "$qa"
        ch "$server" -q "$(query_sql "$q" "$w" "$st" "$table") ORDER BY tags FORMAT TSV SETTINGS max_threads = $MAX_THREADS$extra" > "$RES/out/$cid.$label.tsv" \
            || echo -e "$cid\t$label\tresult" >> "$RES/query_errors.tsv"
    done
done
compare() { # case a b tolerance
    local f="$RES/out/$1"
    "$BRANCH_BIN" local -q "
      WITH a AS (SELECT * FROM file('$f.$2.tsv', 'TSV', 'tags Array(Tuple(String, String)), samples Array(Tuple(DateTime64(3), Float64))')),
           b AS (SELECT * FROM file('$f.$3.tsv', 'TSV', 'tags Array(Tuple(String, String)), samples Array(Tuple(DateTime64(3), Float64))'))
      SELECT '$1', '$2 vs $3', (SELECT count() FROM a), (SELECT count() FROM b),
             countIf(length(a.samples) != length(b.samples) OR NOT arrayAll((x, y) -> x.1 = y.1
                 AND (x.2 = y.2 OR (isNaN(x.2) AND isNaN(y.2)) OR abs(x.2 - y.2) <= $4 * greatest(abs(x.2), abs(y.2))), a.samples, arrayResize(b.samples, length(a.samples)))) AS differ
      FROM a FULL OUTER JOIN b USING (tags) FORMAT TSV"
}
echo -e "case\tpair\tseries_a\tseries_b\tseries_differ" > "$RES/equality.tsv"
for c in "${CASES[@]}"; do
    IFS='|' read -r cid _ _ _ <<< "$c"
    tol=1e-9; [[ $cid == selector_* ]] && tol=0
    compare "$cid" rollup raw "$tol" >> "$RES/equality.tsv"
    [ "$MASTER_BIN" != "-" ] && compare "$cid" raw master 1e-9 >> "$RES/equality.tsv"
done
log "equality: $RES/equality.tsv"

# ---------------------------------------------------------------- probe: what range functions would get from a rollup
# Not part of the pass rule: PromQL reads range selectors raw. This measures the error and speed of reading a rollup
# for them, through timeSeriesSelector() with a step argument and the grid functions, over the 7-day window at a 1h step.
if [ "$SMOKE" != 1 ] && [ "$DAYS" -ge 7 ]; then
    S=$(( T_END - 604800 )); E=$T_END
    PROBES=("rate|timeSeriesRateToGrid($S, $E, 3600, 3900)|ab_counter_total|3900"
            "increase|timeSeriesIncreaseToGrid($S, $E, 3600, 3600)|ab_counter_total|3600"
            "avg_over_time|timeSeriesAvgToGrid($S, $E, 3600, 3600)|ab_gauge|3600"
            "max_over_time|timeSeriesMaxToGrid($S, $E, 3600, 3600)|ab_gauge|3600")
    echo -e "function\trollup\tpoints\tlost\textra\tdiffer\trel_p50\trel_p99\traw_ms\trollup_ms" > "$RES/probe.tsv"
    for p in "${PROBES[@]}"; do IFS='|' read -r fid fn metric win <<< "$p"
        for r in 300 3600; do
            sel() { echo "SELECT id, $fn(samples) AS g FROM timeSeriesSelector(currentDatabase(), 'ab_rollup', '$metric', $(( S - win )), $E, 'samples'$1) GROUP BY id"; }
            ch branch -q "SELECT '$fid', $r, count(), countIf(isNotNull(a) AND isNull(b)), countIf(isNull(a) AND isNotNull(b)),
                                 countIf(isNotNull(a) AND isNotNull(b) AND abs(b - a) > 1e-9 * greatest(abs(a), 1)),
                                 round(quantileExactIf(0.5)(abs(b - a) / greatest(abs(a), 1e-9), isNotNull(a) AND isNotNull(b)), 4),
                                 round(quantileExactIf(0.99)(abs(b - a) / greatest(abs(a), 1e-9), isNotNull(a) AND isNotNull(b)), 4), 0, 0
                          FROM ($(sel "")) AS raw JOIN ($(sel ", $r")) AS rl USING (id) ARRAY JOIN raw.g AS a, rl.g AS b
                          SETTINGS max_threads = $MAX_THREADS" >> "$RES/probe.tsv" || log "probe $fid $r failed"
            for i in $(seq 5); do
                ch branch -q "$(sel "") FORMAT Null SETTINGS max_threads = $MAX_THREADS, log_comment = 'ab-p:$fid:$r:raw:$i'"
                ch branch -q "$(sel ", $r") FORMAT Null SETTINGS max_threads = $MAX_THREADS, log_comment = 'ab-p:$fid:$r:rollup:$i'"
            done
        done
    done
    log "probe: $RES/probe.tsv"
fi

# ---------------------------------------------------------------- latency and reads
for a in "${ARMS[@]}"; do read -r arm _ _ <<< "$a"; ch "$arm" -q "SYSTEM FLUSH LOGS"; done
metrics() { # server
    ch "$1" -q "SELECT splitByChar(':', log_comment)[2] AS case, splitByChar(':', log_comment)[3] AS arm, count() AS runs,
                       quantileExact(0.5)(query_duration_ms) AS p50_ms, quantileExact(0.95)(query_duration_ms) AS p95_ms,
                       quantileExact(0.5)(read_rows) AS read_rows, quantileExact(0.5)(read_bytes) AS read_bytes,
                       quantileExact(0.5)(memory_usage) AS memory, quantileExact(0.5)(ProfileEvents['OSCPUVirtualTimeMicroseconds']) AS cpu_us
                FROM system.query_log
                WHERE type = 'QueryFinish' AND log_comment LIKE 'ab-q:%' AND toUInt32OrZero(splitByChar(':', log_comment)[4]) > 0
                GROUP BY case, arm ORDER BY case, arm FORMAT TSV"
}
{ echo -e "case\tarm\truns\tp50_ms\tp95_ms\tread_rows\tread_bytes\tmemory\tcpu_us"
  metrics branch; [ "$MASTER_BIN" != "-" ] && metrics master; } > "$RES/latency.tsv"
ch branch -q "SELECT splitByChar(':', log_comment)[2] AS function, splitByChar(':', log_comment)[3] AS rollup, splitByChar(':', log_comment)[4] AS arm,
                     count() AS runs, quantileExact(0.5)(query_duration_ms) AS p50_ms, quantileExact(0.5)(read_bytes) AS read_bytes
              FROM system.query_log WHERE type = 'QueryFinish' AND log_comment LIKE 'ab-p:%'
              GROUP BY function, rollup, arm ORDER BY function, rollup, arm FORMAT TSVWithNames" > "$RES/probe_latency.tsv"

# ---------------------------------------------------------------- pass rule
"$BRANCH_BIN" local -q "
  WITH l AS (SELECT * FROM file('$RES/latency.tsv', 'TSVWithNames')),
       r AS (SELECT case, anyIf(p50_ms, arm = 'rollup') AS rollup_p50, anyIf(p50_ms, arm = 'raw') AS raw_p50, anyIf(p50_ms, arm = 'master') AS master_p50,
                    anyIf(read_bytes, arm = 'rollup') AS rollup_bytes, anyIf(read_bytes, arm = 'raw') AS raw_bytes
             FROM l GROUP BY case)
  SELECT case, raw_p50, rollup_p50, round(raw_p50 / greatest(rollup_p50, 1), 2) AS speedup, round(raw_bytes / greatest(rollup_bytes, 1), 1) AS bytes_ratio,
         master_p50, round(raw_p50 / greatest(master_p50, 1), 3) AS raw_vs_master,
         multiIf(case LIKE 'selector%_1h' OR case LIKE 'selector%_5m', if(case LIKE '%_1d_%', 'target-1d', 'target'),
                 case LIKE 'selector%_1m', 'target-1m', case LIKE 'selector%_15s', 'guard-15s', 'guard-range') AS role,
         multiIf(role IN ('target', 'target-1d'), if(speedup >= 2 AND bytes_ratio >= 4, 'pass', 'FAIL'),
                 role = 'target-1m', if(speedup >= 1.25 AND bytes_ratio >= 2, 'pass', 'FAIL'),
                 if(speedup >= 0.95, 'pass', 'CHECK')) AS verdict
  FROM r ORDER BY role, case FORMAT TSVWithNames" > "$RES/summary.tsv"
log "summary: $RES/summary.tsv (pass rule in ab-plans/rollup-samples.md)"
column -t -s $'\t' "$RES/summary.tsv" | tee -a "$LOG"
[ -s "$RES/query_errors.tsv" ] && die "some queries failed: $RES/query_errors.tsv"
log "done"
