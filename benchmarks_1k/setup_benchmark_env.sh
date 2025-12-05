#!/usr/bin/env bash
set -euo pipefail

# Install prerequisites (Debian/Ubuntu)
apt-get install -y python3-venv python3-full

# Create/activate venv
python3 -m venv venv
source venv/bin/activate

# Python deps
pip install pandas matplotlib seaborn

# Run the benchmark pipepine
bash run_all.sh