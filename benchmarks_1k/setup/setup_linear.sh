#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------
N=1000
CSV_FILE="../data/data_linear.csv"
OUTPUT_SQL="../insert/insert_data_linear.sql"
QUERY_VALUES="../query/query_values_linear.txt"
NUM_QUERIES=100

echo "[*] Generating LINEAR dataset ($N rows, id = value = x) into $CSV_FILE ..."

# ---------------------------------------------------
# 1) Create the linear CSV dataset
# ---------------------------------------------------
python3 - <<EOF
N = $N
with open("$CSV_FILE", "w") as f:
    f.write("id,value\n")
    for x in range(1, N + 1):
        f.write(f"{x:.2f},{x:.2f}\n")
EOF

echo "[*] CSV generation complete: $(wc -l < $CSV_FILE) lines"


# ---------------------------------------------------
# 2) Generate the bulk INSERT SQL file
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
# 3) Generate query_values.txt — sample 100 real values
# ---------------------------------------------------
echo "[*] Selecting $NUM_QUERIES query values from dataset into $QUERY_VALUES ..."

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
