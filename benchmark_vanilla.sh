#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------
# CONFIG
# ---------------------------------------------
CSV_FILE="data.csv"
RESULTS_FILE="results.txt"
STATS_FILE="stats.txt"
QUERY_VALUES="query_values.txt"
QUERIES=100
DB_NAME="bench.db"
DUCKDB_CLI="./build/release/duckdb"   # IMPORTANT: your local DuckDB binary

# ---------------------------------------------
# 1. Load CSV into DuckDB
# ---------------------------------------------
load_data() {
    echo "[*] Loading data from CSV into DuckDB..."

    $DUCKDB_CLI $DB_NAME <<EOF
DROP TABLE IF EXISTS bench;
CREATE TABLE bench(id DOUBLE, value DOUBLE);
COPY bench FROM '$CSV_FILE' (HEADER, DELIMITER ',');
EOF

    echo "[*] Data load complete."
}

# ---------------------------------------------
# 2. Pick REAL EXISTING values for queries
# ---------------------------------------------
prepare_query_values() {
    echo "[*] Sampling $QUERIES existing values from CSV..."

    # Remove header, shuffle, pick exactly N distinct values
    tail -n +2 "$CSV_FILE" | shuf -n $QUERIES | cut -d',' -f2 > "$QUERY_VALUES"

    echo "[*] Query values saved to $QUERY_VALUES"
}

# ---------------------------------------------
# 3. Run benchmark queries with manual timing
# ---------------------------------------------
run_benchmarks() {
    echo "[*] Running benchmark with manual high-precision timing..."
    rm -f "$RESULTS_FILE"

    # Create folder for query outputs
    OUTPUT_DIR="query_outputs"
    mkdir -p "$OUTPUT_DIR"

    idx=1
    while read -r target; do
        
        # Start high-precision timer
        start=$(date +%s%N)

        # Run the query and capture output
        query_output=$(echo "SELECT * FROM bench WHERE value = $target;" | \
                       $DUCKDB_CLI $DB_NAME)

        # End time
        end=$(date +%s%N)

        # Compute elapsed time in ms
        runtime_ms=$(( (end - start) / 1000000 ))

        # Save output to per-query file
        echo "$query_output" > "$OUTPUT_DIR/output_${idx}.txt"

        # Record time
        echo "$runtime_ms" >> "$RESULTS_FILE"
        echo "Query $idx/$QUERIES -> ${runtime_ms} ms  (saved to output_${idx}.txt)"

        idx=$((idx+1))
    done < "$QUERY_VALUES"

    echo "[*] Benchmark complete. Results saved to $RESULTS_FILE"
    echo "[*] Query outputs saved in folder: $OUTPUT_DIR/"
}


# ---------------------------------------------
# 4. Compute statistics from results.txt
# ---------------------------------------------
compute_stats() {
    echo "[*] Computing statistics..."
    rm -f "$STATS_FILE"

    # Basic stats via awk
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

    # -------------------------
    # Compute p99 using external sort
    # -------------------------
    total=$(wc -l < "$RESULTS_FILE")
    p99_index=$(( (total * 99 + 99) / 100 ))  # round up

    p99=$(sort -n "$RESULTS_FILE" | sed -n "${p99_index}p")

    echo "P99 (ms): $p99" >> "$STATS_FILE"

    echo "[*] Stats saved to $STATS_FILE"
}


# ---------------------------------------------
# MAIN EXECUTION PIPELINE
# ---------------------------------------------
load_data
prepare_query_values
run_benchmarks
compute_stats

echo "[*] ALL DONE."
