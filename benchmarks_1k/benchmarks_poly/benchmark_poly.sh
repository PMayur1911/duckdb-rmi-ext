#!/usr/bin/env bash
set -euo pipefail

DUCKDB="../../build/release/duckdb"


# detect /usr/bin/time

if command -v /usr/bin/time >/dev/null 2>&1; then
    TIME_BIN="/usr/bin/time"
elif command -v time >/dev/null 2>&1; then
    TIME_BIN="time"
else
    echo "[FATAL] No 'time' command available. Install with: apt-get install -y time"
    exit 1
fi



# BENCHMARK FUNCTION (RUNS 100 QUERIES)

run_benchmark() {
    local NAME="$1"
    local INSERT_SQL="$2"
    local QUERY_VALUES="$3"
    local OUT_DIR="$4"

    mkdir -p "$OUT_DIR/query_outputs"

    local RESULTS_FILE="$OUT_DIR/results.txt"
    local STATS_FILE="$OUT_DIR/stats.txt"
    local ACC_FILE="$OUT_DIR/accuracy.txt"
    local MEM_FILE="$OUT_DIR/index_memory.txt"

    rm -f "$RESULTS_FILE" "$STATS_FILE" "$ACC_FILE" "$MEM_FILE"

    echo "[*] Running RMI (poly model) benchmark for: $NAME"

    INSERT_BLOCK=$(cat "$INSERT_SQL")


    
    # 1. INDEX MEMORY + INDEX BUILD TIME
    
    echo "[*] Measuring RMI-poly index memory + build time..."

    BASE_SQL=$(cat <<EOF
CREATE TABLE test_rmi_data(id DOUBLE, value DOUBLE);
$INSERT_BLOCK
EOF
)

    INDEX_SQL=$(cat <<EOF
CREATE TABLE test_rmi_data(id DOUBLE, value DOUBLE);
$INSERT_BLOCK
CREATE INDEX idx_rmi_value
ON test_rmi_data USING RMI (value)
WITH (model='poly');
EOF
)

    # ---- baseline memory ----
    $TIME_BIN -v $DUCKDB mem_base.db -c "$BASE_SQL" \
        > /dev/null 2> base_mem.txt || true

    MEM_BASE=$(grep -Ei "maximum resident set size|resident set size" base_mem.txt \
               | grep -Eo "[0-9]+" | head -n 1)
    [[ -z "${MEM_BASE:-}" ]] && MEM_BASE=0


    # ---- measure index build time ----
    INDEX_BUILD_START=$(date +%s%N)

    $TIME_BIN -v $DUCKDB mem_index.db -c "$INDEX_SQL" \
        > /dev/null 2> index_mem.txt || true

    INDEX_BUILD_END=$(date +%s%N)
    INDEX_BUILD_MS=$(( (INDEX_BUILD_END - INDEX_BUILD_START) / 1000000 ))


    MEM_FULL=$(grep -Ei "maximum resident set size|resident set size" index_mem.txt \
               | grep -Eo "[0-9]+" | head -n 1)
    [[ -z "${MEM_FULL:-}" ]] && MEM_FULL=0


    # ---- true index memory ----
    INDEX_MEM_KB=$(( MEM_FULL - MEM_BASE ))
    (( INDEX_MEM_KB < 0 )) && INDEX_MEM_KB=0

    INDEX_MEM_MB=$(awk -v kb="$INDEX_MEM_KB" 'BEGIN { printf "%.2f", kb/1024 }')

    echo "Index Build Time (ms): $INDEX_BUILD_MS"
    echo "Index Memory (KB): $INDEX_MEM_KB"
    echo "Index Memory (MB): $INDEX_MEM_MB"

    {
        echo "Index Build Time (ms): $INDEX_BUILD_MS"
        echo "Index Memory (KB): $INDEX_MEM_KB"
        echo "Index Memory (MB): $INDEX_MEM_MB"
    } > "$MEM_FILE"

    rm -f mem_base.db mem_index.db base_mem.txt index_mem.txt


    
    # 2. RUN 100 QUERIES
    
    idx=1
    hits=0
    misses=0

    while read -r target; do

        SQL_BLOCK=$(cat <<EOF
CREATE TEMP TABLE test_rmi_data(id DOUBLE, value DOUBLE);

$INSERT_BLOCK

CREATE INDEX idx_rmi_value
ON test_rmi_data USING RMI (value)
WITH (model='poly');

SELECT id, value
FROM test_rmi_data
WHERE value = $target
ORDER BY id;
EOF
)

        start=$(date +%s%N)
        output=$($DUCKDB :memory: -c "$SQL_BLOCK")
        end=$(date +%s%N)

        runtime_ms=$(( (end - start) / 1000000 ))

        echo "$output" > "$OUT_DIR/query_outputs/output_${idx}.txt"
        echo "$runtime_ms" >> "$RESULTS_FILE"

        if echo "$output" | grep -q "0 rows"; then
            misses=$((misses + 1))
        else
            hits=$((hits + 1))
        fi

        echo "Query $idx -> ${runtime_ms} ms"
        idx=$((idx+1))

    done < "$QUERY_VALUES"


    
    # 3. TIMING STATS
    
    echo "[*] Computing stats for $NAME..."

    avg=$(awk '{s+=$1} END{print s/NR}' "$RESULTS_FILE")
    min=$(sort -n "$RESULTS_FILE" | head -n 1)
    max=$(sort -n "$RESULTS_FILE" | tail -n 1)

    total=$(wc -l < "$RESULTS_FILE")
    p99_index=$(( (total * 99 + 99) / 100 ))
    p99=$(sort -n "$RESULTS_FILE" | sed -n "${p99_index}p")

    {
        echo "Average (ms): $avg"
        echo "Min (ms): $min"
        echo "Max (ms): $max"
        echo "P99 (ms): $p99"
        echo "Index Build Time (ms): $INDEX_BUILD_MS"
        echo "Index Memory (KB): $INDEX_MEM_KB"
        echo "Index Memory (MB): $INDEX_MEM_MB"
    } > "$STATS_FILE"


    
    # 4. ACCURACY
    
    hit_rate=$(awk -v h="$hits" 'BEGIN { printf "%.2f", (h/100)*100 }')
    miss_rate=$(awk -v m="$misses" 'BEGIN { printf "%.2f", (m/100)*100 }')

    {
        echo "Hits: $hits"
        echo "Misses: $misses"
        echo "Hit Rate (%): $hit_rate"
        echo "Miss Rate (%): $miss_rate"
    } > "$ACC_FILE"

    echo "[*] Completed RMI-poly benchmark for: $NAME"
}



# RUN BENCHMARKS FOR linear, poly, random

run_benchmark \
    "linear" \
    "../insert/insert_data_linear.sql" \
    "../query/query_values_linear.txt" \
    "../outputs/rmi_poly/linear"

run_benchmark \
    "poly" \
    "../insert/insert_data_poly.sql" \
    "../query/query_values_poly.txt" \
    "../outputs/rmi_poly/poly"

run_benchmark \
    "random" \
    "../insert/insert_data_random.sql" \
    "../query/query_values_random.txt" \
    "../outputs/rmi_poly/random"

echo "[*] ALL RMI-POLY BENCHMARKS COMPLETED."
