#!/bin/bash
#
# BKK Collector Cron Wrapper Script
# This script handles environment setup, locking, and error handling for cron execution
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Configuration
LOCK_FILE="/tmp/bkk_collector.lock"
LOG_FILE="$PROJECT_DIR/logs/cron_bkk.log"
CONFIG_FILE="$PROJECT_DIR/config/config.yaml"

# Change to project directory
cd "$PROJECT_DIR" || exit 1

# Extract Python interpreter path from config.yaml
PYTHON_BIN=$(grep "interpreter_path:" "$CONFIG_FILE" | sed 's/.*interpreter_path:[ ]*"\(.*\)"/\1/')

# Fallback to system python if config not found
if [ -z "$PYTHON_BIN" ] || [ ! -x "$PYTHON_BIN" ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - WARNING: Python interpreter from config not found, using fallback" >> "$LOG_FILE"
    PYTHON_BIN="/usr/bin/python3"
fi

echo "$(date '+%Y-%m-%d %H:%M:%S') - Using Python: $PYTHON_BIN" >> "$LOG_FILE"

# Check for lock file to prevent concurrent runs
if [ -f "$LOCK_FILE" ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - BKK collector already running (lock file exists)" >> "$LOG_FILE"
    exit 0
fi

# Create lock file
echo $$ > "$LOCK_FILE"

# Ensure lock file is removed on exit
trap "rm -f $LOCK_FILE" EXIT

# Log start
echo "$(date '+%Y-%m-%d %H:%M:%S') - Starting BKK collection" >> "$LOG_FILE"

# Run the collector and capture all output
"$PYTHON_BIN" "$PROJECT_DIR/src/bkk_collector.py" >> "$LOG_FILE" 2>&1

# Capture exit code
EXIT_CODE=$?

# Log completion
if [ $EXIT_CODE -eq 0 ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - BKK collection completed successfully" >> "$LOG_FILE"
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') - BKK collection failed with exit code $EXIT_CODE" >> "$LOG_FILE"
fi

exit $EXIT_CODE
