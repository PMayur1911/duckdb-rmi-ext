#!/usr/bin/env bash
set -euo pipefail

DUCKDB="./build/release/duckdb"
CSV_FILE="data.csv"
QUERY_VALUES="query_values.txt"
RESULTS_FILE="results_rmi_two_layer.txt"
OUTPUT_DIR="query_outputs_rmi_two_layer"

rm -f "$RESULTS_FILE"
mkdir -p "$OUTPUT_DIR"

echo "[*] Running RMI benchmark (two_layer model)..."

idx=1
while read -r target; do

    SQL_FILE="rmi_two_layer_query_${idx}.sql"

    # Build the SQL for this iteration
    cat > "$SQL_FILE" <<EOF
-- Load the CSV into a TEMP table
CREATE TEMP TABLE staging(id DOUBLE, value DOUBLE);
COPY staging FROM '$CSV_FILE' (HEADER, DELIMITER ',');

-- Create a main table with an integer primary key
CREATE TEMP TABLE test_rmi_data AS
SELECT
    ROW_NUMBER() OVER ()::INTEGER AS id,
    value
FROM staging;

-- Create RMI learned index using the two-layer model
CREATE INDEX idx_rmi_value
ON test_rmi_data USING RMI (value)
WITH (model='two_layer');

-- Point lookup query
SELECT id, value
FROM test_rmi_data
WHERE value = $target
ORDER BY id;
EOF

    # Time the entire DuckDB call (index + query)
    start=$(date +%s%N)
    output=$($DUCKDB :memory: -c "$(cat "$SQL_FILE")")
    end=$(date +%s%N)

    runtime_ms=$(( (end - start) / 1000000 ))

    # Save output and timing
    echo "$output" > "$OUTPUT_DIR/output_${idx}.txt"
    echo "$runtime_ms" >> "$RESULTS_FILE"

    echo "Query $idx -> ${runtime_ms} ms"

    idx=$((idx + 1))

done < "$QUERY_VALUES"

echo "[*] Completed RMI benchmark (two_layer model)."
echo "[*] Timing results saved to $RESULTS_FILE"
echo "[*] Query outputs saved in $OUTPUT_DIR/"
