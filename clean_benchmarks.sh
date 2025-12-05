#!/usr/bin/env bash
set -euo pipefail

# 1k
rm -f benchmarks_1k/*.png benchmarks_1k/*.csv benchmarks_1k/*log
rm -rf benchmarks_1k/outputs benchmarks_1k/venv
rm -f benchmarks_1k/insert/* benchmarks_1k/query/* benchmarks_1k/data/*

# 10k
rm -f benchmarks_10k/*.png benchmarks_10k/*.csv benchmarks_10k/*log
rm -rf benchmarks_10k/outputs benchmarks_10k/venv
rm -f benchmarks_10k/insert/* benchmarks_10k/query/* benchmarks_10k/data/*

# 100k
rm -f benchmarks_100k/*.png benchmarks_100k/*.csv benchmarks_100k/*log
rm -rf benchmarks_100k/outputs_100k benchmarks_100k/venv
rm -f benchmarks_100k/insert/* benchmarks_100k/query/* benchmarks_100k/data/*

