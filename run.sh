#!/usr/bin/env bash
set -euo pipefail

# Error message and exit helper
error() {
  echo "Error: $*" >&2
  exit 1
}

# Check that required commands are available
check_cmd() {
  command -v "$1" >/dev/null 2>&1 || error "$1 not found. Please install it."
}

# Pre-flight checks
check_cmd bash
check_cmd python3
check_cmd make

# Check Python version >= 3.8
python3 - << 'EOF' || error "Python3 version must be >= 3.8"
import sys
sys.exit(not (sys.version_info >= (3, 8)))
EOF

# Warn if no .env file (settings may rely on it)
if [ ! -f .env ]; then
  echo "Warning: .env file not found; using default settings. Create a .env file to override configuration."
fi

echo "Setting up Python virtual environment and installing dependencies..."
make .venv

export SCALE_FACTOR=${SCALE_FACTOR:-1.0}
export RUN_LOG_TIMINGS=1

echo "Running benchmarks with cached IO (no additional I/O overhead)..."
make run-all
make plot

echo "Running benchmarks with I/O overhead included..."
export RUN_INCLUDE_IO=1
make run-all
make plot
