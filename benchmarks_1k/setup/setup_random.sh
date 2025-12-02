#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------
N=1000
CSV_FILE="../data/data_random.csv"
OUTPUT_SQL="../insert/insert_data_random.sql"
QUERY_VALUES="../query/query_values_random.txt"
NUM_QUERIES=100

echo "[*] Generating RANDOM dataset ($N rows, values in 0–100000) into $CSV_FILE ..."

# ---------------------------------------------------
# 1) Create random CSV dataset
# ---------------------------------------------------
python3 - <<EOF
import random
random.seed(42)

N = $N
MIN_VAL = 0
MAX_VAL = 100000

with open("$CSV_FILE", "w") as f:
    f.write("id,value\n")
    for _ in range(N):
        id_val = round(random.uniform(MIN_VAL, MAX_VAL), 2)
        val_val = round(random.uniform(MIN_VAL, MAX_VAL), 2)
        f.write(f"{id_val:.2f},{val_val:.2f}\n")
EOF

echo "[*] CSV generation complete: $(wc -l < $CSV_FILE) lines"


# ---------------------------------------------------
# 2) Generate bulk INSERT SQL file
# ---------------------------------------------------
echo "[*] Generating bulk INSERT SQL into $OUTPUT_SQL ..."

echo "INSERT INTO test_rmi_data(id, value) VALUES" > "$OUTPUT_SQL"

tail -n +2 "$CSV_FILE" | awk -F, -v total="$N" '
{
    printf("(%s, %s)", $1, $2)
    if (NR < total) printf(",\n"); else printf(";\n")
}' >> "$OUTPUT_SQL"

echo "[*] SQL generation complete: $OUTPUT_SQL"


# ---------------------------------------------------
# 3) Generate query values from dataset
# ---------------------------------------------------
echo "[*] Selecting $NUM_QUERIES random query lookup values into $QUERY_VALUES ..."

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
