#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------
N=10000
BATCH=1000

CSV_FILE="../data/data_poly.csv"
OUTPUT_SQL="../insert/insert_data_poly.sql"
QUERY_VALUES="../query/query_values_poly.txt"
NUM_QUERIES=100

echo "[*] Generating POLYNOMIAL dataset ($N rows, y = x^2 + 2x + 1 with x increment 0.1) into $CSV_FILE ..."

# ---------------------------------------------------
# 1) Create polynomial CSV dataset
# ---------------------------------------------------
python3 - <<EOF
N = $N
step = 0.1
with open("$CSV_FILE", "w") as f:
    f.write("id,value\n")
    for i in range(1, N + 1):
        x = i * step   # x = 0.1, 0.2, 0.3, ..., 1000.0
        y = x*x + 2*x + 1
        f.write(f"{x:.2f},{y:.2f}\n")
EOF

echo "[*] CSV generation complete: $(wc -l < $CSV_FILE) lines"

# ---------------------------------------------------
# 2) Generate **batched** INSERT SQL
# ---------------------------------------------------
echo "[*] Generating batched INSERT SQL into $OUTPUT_SQL ..."

rm -f "$OUTPUT_SQL"
touch "$OUTPUT_SQL"

count=0
batch_i=0

echo "" > "$OUTPUT_SQL"

tail -n +2 "$CSV_FILE" | while IFS=',' read -r id value; do
    if (( count % BATCH == 0 )); then
        if (( count > 0 )); then
            echo ";" >> "$OUTPUT_SQL"
        fi
        batch_i=$((batch_i + 1))
        echo "INSERT INTO test_rmi_data(id, value) VALUES" >> "$OUTPUT_SQL"
    else
        echo "," >> "$OUTPUT_SQL"
    fi

    printf "(%s, %s)" "$id" "$value" >> "$OUTPUT_SQL"

    count=$((count + 1))
done

echo ";" >> "$OUTPUT_SQL"

echo "[*] SQL generation complete with $batch_i batches: $OUTPUT_SQL"

# ---------------------------------------------------
# 3) Select query values
# ---------------------------------------------------
echo "[*] Selecting $NUM_QUERIES query values into $QUERY_VALUES ..."

tail -n +2 "$CSV_FILE" \
    | shuf -n $NUM_QUERIES \
    | cut -d',' -f2 > "$QUERY_VALUES"

echo "[*] Query values saved: $QUERY_VALUES"

# ---------------------------------------------------
# DONE
# ---------------------------------------------------
echo "[*] ALL DONE — generated:"
echo "    - $CSV_FILE"
echo "    - $OUTPUT_SQL"
echo "    - $QUERY_VALUES"
