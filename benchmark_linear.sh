#!/usr/bin/env bash
set -euo pipefail

DUCKDB="./build/release/duckdb"
CSV_FILE="data.csv"
QUERY_VALUES="query_values.txt"
RESULTS_FILE="results_rmi_linear.txt"
OUTPUT_DIR="query_outputs_rmi_linear"

rm -f "$RESULTS_FILE"
mkdir -p "$OUTPUT_DIR"

echo "[*] Running RMI benchmark (linear model, stable execution)..."

idx=1
while read -r target; do

    SQL_FILE="rmi_query_${idx}.sql"

    # Build the SQL for this iteration
    cat > "$SQL_FILE" <<EOF
-- Load CSV into temp table
CREATE TEMP TABLE staging(id DOUBLE, value DOUBLE);
COPY staging FROM '$CSV_FILE' (HEADER, DELIMITER ',');

-- Create a proper PK table
CREATE TEMP TABLE test_rmi_data AS
SELECT
    ROW_NUMBER() OVER ()::INTEGER AS id,
    value
FROM staging;

-- Create the RMI index using the linear model
CREATE INDEX idx_rmi_value
ON test_rmi_data USING RMI (value)
WITH (model='linear');

-- Point lookup query
SELECT id, value
FROM test_rmi_data
WHERE value = $target
ORDER BY id;
EOF

    # Time the DuckDB execution
    start=$(date +%s%N)
    output=$($DUCKDB :memory: -c "$(cat "$SQL_FILE")")
    end=$(date +%s%N)

    runtime_ms=$(( (end - start) / 1000000 ))

    # Save output
    echo "$output" > "$OUTPUT_DIR/output_${idx}.txt"
    echo "$runtime_ms" >> "$RESULTS_FILE"

    echo "Query $idx -> ${runtime_ms} ms"

    idx=$((idx + 1))

done < "$QUERY_VALUES"

echo "[*] Completed RMI linear-model benchmark."
echo "[*] Timing results: $RESULTS_FILE"
echo "[*] Query outputs: $OUTPUT_DIR/"
