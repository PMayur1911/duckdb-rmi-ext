#!/usr/bin/env bash
set -euo pipefail

DUCKDB="../../duckdb-rmi-ext/build/release/duckdb"

# Detect time binary
if command -v /usr/bin/time >/dev/null 2>&1; then
    TIME_BIN="/usr/bin/time"
elif command -v time >/dev/null 2>&1; then
    TIME_BIN="time"
else
    echo "[FATAL] 'time' command not found. Install using: apt-get install -y time"
    exit 1
fi

# =====================================================================
# BENCHMARK FUNCTION — RMI (two_layer model)
# =====================================================================
run_benchmark() {
    local NAME="$1"             # linear | poly | random
    local INSERT_SQL="$2"
    local QUERY_VALUES="$3"
    local OUT_DIR="$4"

    mkdir -p "$OUT_DIR/query_outputs"

    local RESULTS_FILE="$OUT_DIR/results.txt"
    local STATS_FILE="$OUT_DIR/stats.txt"
    local ACC_FILE="$OUT_DIR/accuracy.txt"
    local MEM_FILE="$OUT_DIR/index_memory.txt"

    rm -f "$RESULTS_FILE" "$STATS_FILE" "$ACC_FILE" "$MEM_FILE"

    echo ""
    echo "======================================================="
    echo "[*] Running RMI-two_layer benchmark for dataset: $NAME"
    echo "======================================================="

    INSERT_BLOCK=$(cat "$INSERT_SQL")

    # =====================================================================
    # STEP 1 — MEASURE INDEX MEMORY (base vs indexed)
    # =====================================================================
    echo "[*] Measuring RMI-two_layer index memory..."

    BASE_SQL=$(cat <<EOF
CREATE TABLE test_rmi_data(id DOUBLE, value DOUBLE);
$INSERT_BLOCK
-- NO INDEX
EOF
)

    INDEX_SQL=$(cat <<EOF
CREATE TABLE test_rmi_data(id DOUBLE, value DOUBLE);
$INSERT_BLOCK
CREATE INDEX idx_rmi_value
ON test_rmi_data USING RMI (value)
WITH (model='two_layer');
EOF
)

    # ---- BASE MEMORY ----
    $TIME_BIN -v $DUCKDB mem_base.db <<< "$BASE_SQL" \
        > /dev/null 2> base_mem.txt || true

    MEM_BASE=$(grep -Ei "maximum resident set size|resident set size" base_mem.txt \
                | grep -Eo "[0-9]+" | head -n 1)
    [[ -z "${MEM_BASE:-}" ]] && MEM_BASE=0

    # ---- INDEXED MEMORY ----
    $TIME_BIN -v $DUCKDB mem_index.db <<< "$INDEX_SQL" \
        > /dev/null 2> index_mem.txt || true

    MEM_FULL=$(grep -Ei "maximum resident set size|resident set size" index_mem.txt \
                | grep -Eo "[0-9]+" | head -n 1)
    [[ -z "${MEM_FULL:-}" ]] && MEM_FULL=0

    INDEX_MEM_KB=$(( MEM_FULL - MEM_BASE ))
    (( INDEX_MEM_KB < 0 )) && INDEX_MEM_KB=0

    INDEX_MEM_MB=$(awk -v kb="$INDEX_MEM_KB" 'BEGIN { printf "%.2f", kb/1024 }')

    echo "Baseline Memory (KB): $MEM_BASE"
    echo "With Index Memory (KB): $MEM_FULL"
    echo "Index Memory (KB): $INDEX_MEM_KB"
    echo "Index Memory (MB): $INDEX_MEM_MB"

    echo "Index Memory (KB): $INDEX_MEM_KB" > "$MEM_FILE"
    echo "Index Memory (MB): $INDEX_MEM_MB" >> "$MEM_FILE"

    rm -f mem_base.db mem_index.db base_mem.txt index_mem.txt

    # =====================================================================
    # STEP 2 — RUN 100 QUERIES
    # =====================================================================
    echo "[*] Running 100 lookup queries..."

    idx=1
    hits=0
    misses=0

    while read -r target; do

        cat > tmp_query.sql <<EOF
CREATE TEMP TABLE test_rmi_data(id DOUBLE, value DOUBLE);
$INSERT_BLOCK

CREATE INDEX idx_rmi_value
ON test_rmi_data USING RMI (value)
WITH (model='two_layer');

SELECT id, value
FROM test_rmi_data
WHERE value = $target
ORDER BY id;
EOF

        start=$(date +%s%N)

        if ! output=$($DUCKDB :memory: < tmp_query.sql 2>&1); then
            echo "[WARN] Query $idx failed — marking miss"
            misses=$((misses + 1))
            runtime_ms=0
            echo "$output" > "$OUT_DIR/query_outputs/error_${idx}.txt"
            echo "$runtime_ms" >> "$RESULTS_FILE"
            idx=$((idx+1))
            continue
        fi

        end=$(date +%s%N)
        runtime_ms=$(((end - start) / 1000000))

        echo "$output" > "$OUT_DIR/query_outputs/output_${idx}.txt"
        echo "$runtime_ms" >> "$RESULTS_FILE"

        if echo "$output" | grep -q "0 rows"; then
            misses=$((misses + 1))
        else
            hits=$((hits + 1))
        fi

        echo "Query $idx -> $runtime_ms ms"
        idx=$((idx + 1))

    done < "$QUERY_VALUES"

    rm -f tmp_query.sql

    # =====================================================================
    # STEP 3 — STATS
    # =====================================================================
    echo "[*] Computing statistics..."

    awk '
    {
        sum += $1
        count += 1
        if (NR == 1 || $1 < min) min = $1
        if (NR == 1 || $1 > max) max = $1
    }
    END {
        avg = sum / count
        printf("Average (ms): %f\nMin (ms): %f\nMax (ms): %f\n", avg, min, max)
    }' "$RESULTS_FILE" > "$STATS_FILE"

    total=$(wc -l < "$RESULTS_FILE")
    p99_index=$(( (total * 99 + 99) / 100 ))
    p99=$(sort -n "$RESULTS_FILE" | sed -n "${p99_index}p")
    echo "P99 (ms): $p99" >> "$STATS_FILE"

    echo "Index Memory (KB): $INDEX_MEM_KB" >> "$STATS_FILE"
    echo "Index Memory (MB): $INDEX_MEM_MB" >> "$STATS_FILE"

    # =====================================================================
    # STEP 4 — ACCURACY
    # =====================================================================
    hit_rate=$(awk -v h="$hits" 'BEGIN { printf "%.2f", (h/100)*100 }')
    miss_rate=$(awk -v m="$misses" 'BEGIN { printf "%.2f", (m/100)*100 }')

    echo "Hits: $hits" > "$ACC_FILE"
    echo "Misses: $misses" >> "$ACC_FILE"
    echo "Hit Rate (%): $hit_rate" >> "$ACC_FILE"
    echo "Miss Rate (%): $miss_rate" >> "$ACC_FILE"

    echo "[*] Completed RMI-two_layer benchmark for $NAME"
}

# =====================================================================
# RUN ALL THREE DISTRIBUTIONS
# =====================================================================
run_benchmark \
    "linear" \
    "../insert/insert_data_linear.sql" \
    "../query/query_values_linear.txt" \
    "../outputs/rmi_two_layer/linear"

run_benchmark \
    "poly" \
    "../insert/insert_data_poly.sql" \
    "../query/query_values_poly.txt" \
    "../outputs/rmi_two_layer/poly"

run_benchmark \
    "random" \
    "../insert/insert_data_random.sql" \
    "../query/query_values_random.txt" \
    "../outputs/rmi_two_layer/random"

echo "[*] ALL RMI-two_layer benchmarks completed."
