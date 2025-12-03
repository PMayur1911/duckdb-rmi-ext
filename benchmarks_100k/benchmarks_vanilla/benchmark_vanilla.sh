#!/usr/bin/env bash
set -euo pipefail

DUCKDB="../../build/release/duckdb"

# =====================================================================
# BENCHMARK FUNCTION — VANILLA SCAN (NO INDEX)
# =====================================================================
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
    echo "[*] Running VANILLA benchmark for dataset: $NAME"
    echo "======================================================="

    INSERT_BLOCK=$(cat "$INSERT_SQL")

    # Vanilla: index memory is always 0
    INDEX_MEM_KB=0
    INDEX_MEM_MB=0.00

    {
        echo "Index Memory (KB): 0"
        echo "Index Memory (MB): 0.00"
    } > "$MEM_FILE"

    # =====================================================================
    # RUN 100 QUERIES
    # =====================================================================
    echo "[*] Running 100 lookup queries (no index)..."

    idx=1
    hits=0
    misses=0

    while read -r target; do

cat > tmp_query.sql <<EOF
CREATE TEMP TABLE test_rmi_data(id DOUBLE, value DOUBLE);
$INSERT_BLOCK

-- VANILLA: sequential scan
SELECT id, value
FROM test_rmi_data
WHERE value = $target
ORDER BY id;
EOF

        start=$(date +%s%N)

        if ! output=$($DUCKDB :memory: < tmp_query.sql 2>&1); then
            echo "[WARN] Query $idx failed — marking miss"
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


    # =====================================================================
    # STEP 2 — STATS
    # =====================================================================
    echo "[*] Computing statistics..."

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
        echo "Index Memory (KB): 0"
        echo "Index Memory (MB): 0.00"
    } > "$STATS_FILE"


    # =====================================================================
    # STEP 3 — ACCURACY
    # =====================================================================
    hit_rate=$(awk -v h="$hits" 'BEGIN { printf "%.2f", (h/100)*100 }')
    miss_rate=$(awk -v m="$misses" 'BEGIN { printf "%.2f", (m/100)*100 }')

    {
        echo "Hits: $hits"
        echo "Misses: $misses"
        echo "Hit Rate (%): $hit_rate"
        echo "Miss Rate (%): $miss_rate"
    } > "$ACC_FILE"

    echo "[*] Completed VANILLA benchmark for $NAME"
}

# =====================================================================
# RUN ALL THREE DISTRIBUTIONS (100k)
# =====================================================================
run_benchmark \
    "linear_100k" \
    "../insert/insert_data_linear_100k.sql" \
    "../query/query_values_linear_100k.txt" \
    "../outputs_100k/vanilla/linear"

run_benchmark \
    "poly_100k" \
    "../insert/insert_data_poly_100k.sql" \
    "../query/query_values_poly_100k.txt" \
    "../outputs_100k/vanilla/poly"

run_benchmark \
    "random_100k" \
    "../insert/insert_data_random_100k.sql" \
    "../query/query_values_random_100k.txt" \
    "../outputs_100k/vanilla/random"

echo "[*] ALL VANILLA (100k) benchmarks completed."
