#!/usr/bin/env bash
set -euo pipefail

echo "===================================================="
echo "        MASTER BENCHMARK PIPELINE STARTING"
echo "===================================================="
echo ""

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="$ROOT_DIR/master_run.log"

echo "[*] Logging output to $LOG_FILE"
echo "Master Benchmark Run - $(date)" > "$LOG_FILE"
echo "" >> "$LOG_FILE"

# ------------------------------------------
# Helper function: run script in its folder
# ------------------------------------------
run_in_dir() {
    local DIR="$1"
    local SCRIPT="$2"

    echo "[*] Running $DIR/$SCRIPT ..."
    pushd "$DIR" > /dev/null
    bash "$SCRIPT" >> "$LOG_FILE" 2>&1
    popd > /dev/null
    echo "[✓] Completed: $DIR/$SCRIPT"
    echo ""
}

# ----------------------------------------------------
# STEP 1 — RUN SETUP SCRIPTS
# ----------------------------------------------------
echo "----------------------------------------------------"
echo "[1] Running setup scripts ..."
echo "----------------------------------------------------"

run_in_dir "setup" "setup_linear.sh"
run_in_dir "setup" "setup_poly.sh"
run_in_dir "setup" "setup_random.sh"

echo "[✓] Setup complete."
echo ""

# ----------------------------------------------------
# STEP 2 — RUN BENCHMARK SCRIPTS
# ----------------------------------------------------
echo "----------------------------------------------------"
echo "[2] Running benchmark scripts ..."
echo "----------------------------------------------------"

run_in_dir "benchmarks_art" "benchmark_art.sh"
run_in_dir "benchmarks_linear" "benchmark_linear.sh"
run_in_dir "benchmarks_poly" "benchmark_poly.sh"
run_in_dir "benchmarks_two_layer" "benchmark_two_layer.sh"
run_in_dir "benchmarks_vanilla" "benchmark_vanilla.sh"

echo "[✓] All benchmarks completed."
echo ""

# ----------------------------------------------------
# STEP 3 — RUN SUMMARY SCRIPT
# ----------------------------------------------------
echo "----------------------------------------------------"
echo "[3] Running summary program ..."
echo "----------------------------------------------------"

# Activate venv if exists
if [[ -d "$ROOT_DIR/venv" ]]; then
    source "$ROOT_DIR/venv/bin/activate"
fi

python3 "$ROOT_DIR/summarize_benchmarks.py" >> "$LOG_FILE" 2>&1

echo "[✓] Summary generated."
echo "Summary file: benchmark_summary.csv"
echo ""

echo "===================================================="
echo "        MASTER PIPELINE COMPLETED SUCCESSFULLY"
echo "===================================================="
