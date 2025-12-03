#!/usr/bin/env bash
set -euo pipefail

DUCKDB="../../build/release/duckdb"

# Detect time binary
if command -v /usr/bin/time >/dev/null 2>&1; then
    TIME_BIN="/usr/bin/time"
elif command -v time >/dev/null 2>&1; then
    TIME_BIN="time"
else
    echo "[FATAL] 'time' command not found. Install: apt-get install -y time"
    exit 1
fi


# BENCHMARK FUNCTION — RMI (two_layer model)

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

    echo ""
    echo "======================================================="
    echo "[*] Running RMI-two_layer benchmark for: $NAME"
    echo "======================================================="

    INSERT_BLOCK=$(cat "$INSERT_SQL")

    
    # STEP 1 — MEASURE INDEX MEMORY
    
    echo "[*] Measuring RMI-two_layer index memory..."

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
WITH (model='two_layer');
EOF
)

    # ---- BASE MEMORY ----
    $TIME_BIN -v $DUCKDB mem_base_100k.db <<< "$BASE_SQL" \
        > /dev/null 2> base_mem.txt || true

    MEM_BASE=$(grep -Ei "resident set size" base_mem.txt | grep -Eo "[0-9]+" | head -n 1)
    [[ -z "${MEM_BASE:-}" ]] && MEM_BASE=0

    # ---- INDEX MEMORY ----
    $TIME_BIN -v $DUCKDB mem_index_100k.db <<< "$INDEX_SQL" \
        > /dev/null 2> index_mem.txt || true

    MEM_FULL=$(grep -Ei "resident set size" index_mem.txt | grep -Eo "[0-9]+" | head -n 1)
    [[ -z "${MEM_FULL:-}" ]] && MEM_FULL=0

    INDEX_MEM_KB=$((MEM_FULL - MEM_BASE))
    (( INDEX_MEM_KB < 0 )) && INDEX_MEM_KB=0

    INDEX_MEM_MB=$(awk -v kb="$INDEX_MEM_KB" 'BEGIN{ printf "%.2f", kb/1024 }')

    echo "Baseline Memory (KB): $MEM_BASE"
    echo "With Index Memory (KB): $MEM_FULL"
    echo "Index Memory (KB): $INDEX_MEM_KB"
    echo "Index Memory (MB): $INDEX_MEM_MB"

    {
        echo "Index Memory (KB): $INDEX_MEM_KB"
        echo "Index Memory (MB): $INDEX_MEM_MB"
    } > "$MEM_FILE"

    rm -f mem_base_100k.db mem_index_100k.db base_mem.txt index_mem.txt


    
    # STEP 2 — RUN 100 QUERIES
    
    echo "[*] Running 100 timed lookup queries..."

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
            echo "[WARN] Query $idx failed"
            echo "0" >> "$RESULTS_FILE"
            echo "$output" > "$OUT_DIR/query_outputs/error_${idx}.txt"
            misses=$((misses + 1))
            idx=$((idx + 1))
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

        echo "Query $idx → $runtime_ms ms"
        idx=$((idx + 1))

    done < "$QUERY_VALUES"

    rm -f tmp_query.sql


    
    # STEP 3 — STATS
    
    echo "[*] Computing timing statistics..."

    avg=$(awk '{s+=$1} END{printf "%.4f", s/NR}' "$RESULTS_FILE")
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
        echo "Index Memory (KB): $INDEX_MEM_KB"
        echo "Index Memory (MB): $INDEX_MEM_MB"
    } > "$STATS_FILE"


    
    # STEP 4 — ACCURACY
    
    hit_rate=$(awk -v h="$hits" 'BEGIN { printf "%.2f", h }')
    miss_rate=$(awk -v m="$misses" 'BEGIN { printf "%.2f", m }')

    {
        echo "Hits: $hits"
        echo "Misses: $misses"
        echo "Hit Rate (% queries returning rows): $hit_rate"
        echo "Miss Rate (% queries returning 0 rows): $miss_rate"
    } > "$ACC_FILE"

    echo "[*] Finished two_layer benchmark for: $NAME"
}



# RUN ALL DISTRIBUTIONS — 100K versions


run_benchmark \
    "linear_100k" \
    "../insert/insert_data_linear_100k.sql" \
    "../query/query_values_linear_100k.txt" \
    "../outputs_100k/rmi_two_layer/linear"

run_benchmark \
    "poly_100k" \
    "../insert/insert_data_poly_100k.sql" \
    "../query/query_values_poly_100k.txt" \
    "../outputs_100k/rmi_two_layer/poly"

run_benchmark \
    "random_100k" \
    "../insert/insert_data_random_100k.sql" \
    "../query/query_values_random_100k.txt" \
    "../outputs_100k/rmi_two_layer/random"

echo "[*] ALL RMI-two_layer (100k) benchmarks completed."
