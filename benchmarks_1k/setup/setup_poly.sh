#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------
N=1000
CSV_FILE="../data/data_poly.csv"
OUTPUT_SQL="../insert/insert_data_poly.sql"
QUERY_VALUES="../query/query_values_poly.txt"
NUM_QUERIES=100

echo "[*] Generating POLYNOMIAL dataset ($N rows, value = x^2 + 2x + 1) into $CSV_FILE ..."

# ---------------------------------------------------
# 1) Create polynomial CSV dataset
# ---------------------------------------------------
python3 - <<EOF
N = $N
with open("$CSV_FILE", "w") as f:
    f.write("id,value\n")
    for x in range(1, N + 1):
        y = x*x + 2*x + 1   # polynomial y = x^2 + 2x + 1 = (x+1)^2
        f.write(f"{x:.2f},{y:.2f}\n")
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
