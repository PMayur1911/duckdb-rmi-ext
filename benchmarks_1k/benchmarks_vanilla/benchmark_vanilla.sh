#!/usr/bin/env bash
set -euo pipefail

DUCKDB="../../build/release/duckdb"

# ---------------------------------------------------------
# detect /usr/bin/time
# ---------------------------------------------------------
if command -v /usr/bin/time >/dev/null 2>&1; then
    TIME_BIN="/usr/bin/time"
elif command -v time >/dev/null 2>&1; then
    TIME_BIN="time"
else
    echo "[FATAL] No 'time' command available. Install with: apt-get install -y time"
    exit 1
fi


# ---------------------------------------------------------
# BENCHMARK FUNCTION (RUNS 100 QUERIES)
# ---------------------------------------------------------
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
    echo "[*] Running VANILLA benchmark for: $NAME"

    INSERT_BLOCK=$(cat "$INSERT_SQL")


    # =====================================================================
    # 1. VANILLA MEMORY MEASUREMENT (always zero — no index)
    # =====================================================================
    echo "[*] Measuring memory for vanilla (no index)..."

    BASE_SQL=$(cat <<EOF
CREATE TABLE test_rmi_data(id DOUBLE, value DOUBLE);
$INSERT_BLOCK
EOF
)

    $TIME_BIN -v $DUCKDB mem_base.db -c "$BASE_SQL" \
        > /dev/null 2> base_mem.txt || true

    MEM_BASE=$(grep -Ei "maximum resident set size|resident set size" base_mem.txt \
               | grep -Eo "[0-9]+" | head -n 1)

    [[ -z "${MEM_BASE:-}" ]] && MEM_BASE=0

    # For consistency: vanilla index memory = 0
    INDEX_MEM_KB=0
    INDEX_MEM_MB=0.00

    echo "Index Memory (KB): 0" > "$MEM_FILE"
    echo "Index Memory (MB): 0.00" >> "$MEM_FILE"

    rm -f mem_base.db base_mem.txt


    # =====================================================================
    # 2. RUN 100 QUERIES
    # =====================================================================
    idx=1
    hits=0
    misses=0

    while read -r target; do

        SQL_BLOCK=$(cat <<EOF
CREATE TEMP TABLE test_rmi_data(id DOUBLE, value DOUBLE);

$INSERT_BLOCK

-- VANILLA full scan lookup
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


    # =====================================================================
    # 3. STATS
    # =====================================================================
    echo "[*] Computing stats for $NAME..."

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
    echo "Index Memory (KB): 0" >> "$STATS_FILE"
    echo "Index Memory (MB): 0.00" >> "$STATS_FILE"


    # =====================================================================
    # 4. ACCURACY
    # =====================================================================
    hit_rate=$(awk -v h="$hits" 'BEGIN { printf "%.2f", (h/100)*100 }')
    miss_rate=$(awk -v m="$misses" 'BEGIN { printf "%.2f", (m/100)*100 }')

    echo "Hits: $hits" > "$ACC_FILE"
    echo "Misses: $misses" >> "$ACC_FILE"
    echo "Hit Rate (%): $hit_rate" >> "$ACC_FILE"
    echo "Miss Rate (%): $miss_rate" >> "$ACC_FILE"

    echo "[*] Accuracy stats written to: $ACC_FILE"
}


# ---------------------------------------------------------
# RUN ALL VANILLA BENCHMARKS
# ---------------------------------------------------------
run_benchmark \
    "linear" \
    "../insert/insert_data_linear.sql" \
    "../query/query_values_linear.txt" \
    "../outputs/vanilla/linear"

run_benchmark \
    "poly" \
    "../insert/insert_data_poly.sql" \
    "../query/query_values_poly.txt" \
    "../outputs/vanilla/poly"

run_benchmark \
    "random" \
    "../insert/insert_data_random.sql" \
    "../query/query_values_random.txt" \
    "../outputs/vanilla/random"

echo "[*] ALL VANILLA BENCHMARKS COMPLETED."
