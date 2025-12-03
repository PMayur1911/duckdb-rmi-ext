#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------
N=100000                   # total rows (100k)
BATCH_SIZE=500             # rows per INSERT batch
CSV_FILE="../data/data_linear_100k.csv"
OUTPUT_SQL="../insert/insert_data_linear_100k.sql"
QUERY_VALUES="../query/query_values_linear_100k.txt"
NUM_QUERIES=100            # point lookups

mkdir -p ../data ../insert ../query

echo "[*] Generating LINEAR dataset ($N rows, id = value = x) into $CSV_FILE ..."

# ---------------------------------------------------
# 1) Create the linear CSV dataset
# ---------------------------------------------------
python3 - <<EOF
N = $N
with open("$CSV_FILE", "w") as f:
    f.write("id,value\n")
    for x in range(1, N + 1):
        # No floats needed for linear — faster & smaller
        f.write(f"{x},{x}\n")
EOF

echo "[*] CSV generation complete: $(wc -l < "$CSV_FILE") lines"


# ---------------------------------------------------
# 2) Generate batched INSERT SQL file
# ---------------------------------------------------
echo "[*] Generating batched INSERT SQL into $OUTPUT_SQL ..."

python3 - <<EOF
csv_path = "$CSV_FILE"
sql_path = "$OUTPUT_SQL"
batch_size = $BATCH_SIZE

# read all rows (skip header)
rows = []
with open(csv_path, "r") as f:
    next(f)  # skip header
    for line in f:
        line = line.strip()
        if not line:
            continue
        id_str, val_str = line.split(",")
        rows.append((id_str, val_str))

with open(sql_path, "w") as out:
    n = len(rows)
    for i in range(0, n, batch_size):
        batch = rows[i:i + batch_size]
        out.write("INSERT INTO test_rmi_data(id, value) VALUES\n")
        for j, (id_str, val_str) in enumerate(batch):
            sep = ",\n" if j < len(batch) - 1 else ";\n"
            out.write(f"({id_str}, {val_str}){sep}")
EOF

echo "[*] SQL generation complete: $OUTPUT_SQL"


# ---------------------------------------------------
# 3) Generate query_values (100 real point lookups)
# ---------------------------------------------------
echo "[*] Selecting $NUM_QUERIES query values from dataset into $QUERY_VALUES ..."

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
