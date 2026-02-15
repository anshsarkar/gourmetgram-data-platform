#!/usr/bin/env bash
# Sets up a conda to run python scipts for simulation
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# ---- Install Miniconda if missing ----
if ! command -v conda &>/dev/null; then
    echo "Installing Miniconda..."
    wget -q --show-progress -O /tmp/miniconda.sh \
        "https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh"
    bash /tmp/miniconda.sh -b -p "$HOME/miniconda3"
    rm /tmp/miniconda.sh
    eval "$("$HOME/miniconda3/bin/conda" shell.bash hook)"
    "$HOME/miniconda3/bin/conda" init bash
else
    eval "$(conda shell.bash hook)"
fi