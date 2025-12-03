#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------
N=100000                          # 100k rows
BATCH_SIZE=1000                   # rows per INSERT
CSV_FILE="../data/data_random_100k.csv"
OUTPUT_SQL="../insert/insert_data_random_100k.sql"
QUERY_VALUES="../query/query_values_random_100k.txt"
NUM_QUERIES=100

mkdir -p ../data ../insert ../query

echo "[*] Generating RANDOM dataset ($N rows, values ∈ [0,100000]) into $CSV_FILE ..."

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
        id_val = random.uniform(MIN_VAL, MAX_VAL)
        val_val = random.uniform(MIN_VAL, MAX_VAL)
        f.write(f"{id_val},{val_val}\n")
EOF

echo "[*] CSV generation complete: \$(wc -l < "$CSV_FILE") lines"


# ---------------------------------------------------
# 2) Generate batched INSERT SQL file
# ---------------------------------------------------
echo "[*] Generating batched INSERT SQL into $OUTPUT_SQL ..."

rm -f "$OUTPUT_SQL"
touch "$OUTPUT_SQL"

count=0
batch_i=0

while IFS=',' read -r id value; do
    if (( count % BATCH_SIZE == 0 )); then
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

done < <(tail -n +2 "$CSV_FILE")

echo ";" >> "$OUTPUT_SQL"

echo "[*] SQL generation complete with $batch_i batches: $OUTPUT_SQL"


# ---------------------------------------------------
# 3) Generate query values (random sample from CSV)
# ---------------------------------------------------
echo "[*] Selecting $NUM_QUERIES random query lookup values into $QUERY_VALUES ..."

tail -n +2 "$CSV_FILE" \
    | shuf -n "$NUM_QUERIES" \
    | cut -d',' -f2 \
    > "$QUERY_VALUES"

echo "[*] Query values saved: $QUERY_VALUES"


# ---------------------------------------------------
# DONE
# ---------------------------------------------------
echo "[*] ALL DONE — generated:"
echo "    • $CSV_FILE"
echo "    • $OUTPUT_SQL"
echo "    • $QUERY_VALUES"
