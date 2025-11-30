#!/usr/bin/env bash
set -euo pipefail

CSV_FILE="data.csv"

echo "[*] Generating consistent CSV dataset (100,000 rows, values in 0–100000, 2 decimals): $CSV_FILE"

python3 - <<EOF
import random
random.seed(42)

N = 100000
MIN_VAL = 0
MAX_VAL = 100000

with open("$CSV_FILE", "w") as f:
    f.write("id,value\n")
    for i in range(N):
        x = round(random.uniform(MIN_VAL, MAX_VAL), 2)
        y = round(random.uniform(MIN_VAL, MAX_VAL), 2)
        f.write(f"{x},{y}\n")
EOF

echo "[*] CSV generation complete: $(wc -l < $CSV_FILE) lines created."
