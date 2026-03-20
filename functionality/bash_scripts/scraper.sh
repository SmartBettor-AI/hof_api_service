#!/bin/bash
set -euo pipefail

VENV_PYTHON="/home/ec2-user/hof_api_service/venv/bin/python"
SCRIPT_PATH="/home/ec2-user/hof_api_service/functionality/scraper.py"
LOG_FILE="/home/ec2-user/hof_api_service/functionality/bash_scripts/scraper.log"

run_python_process() {
    local script=$1
    local log_file=$2
    nohup "$VENV_PYTHON" "$script" >> "$log_file" 2>&1 &
}

mkdir -p "$(dirname "$LOG_FILE")"

echo "[$(date)] Starting scraper bootstrap..." | tee -a "$LOG_FILE"

# Sanity checks
if [ ! -x "$VENV_PYTHON" ]; then
    echo "[$(date)] ERROR: Python not found at $VENV_PYTHON" | tee -a "$LOG_FILE"
    exit 1
fi

if [ ! -f "$SCRIPT_PATH" ]; then
    echo "[$(date)] ERROR: Scraper not found at $SCRIPT_PATH" | tee -a "$LOG_FILE"
    exit 1
fi

# Ensure Playwright package is importable
echo "[$(date)] Checking Playwright Python package..." | tee -a "$LOG_FILE"
"$VENV_PYTHON" -c "import playwright" >> "$LOG_FILE" 2>&1 || {
    echo "[$(date)] ERROR: Playwright Python package is not installed in the venv." | tee -a "$LOG_FILE"
    exit 1
}

# Ensure browser binaries are installed
echo "[$(date)] Running playwright install..." | tee -a "$LOG_FILE"
"$VENV_PYTHON" -m playwright install chromium >> "$LOG_FILE" 2>&1 || {
    echo "[$(date)] ERROR: playwright install failed." | tee -a "$LOG_FILE"
    exit 1
}

# Optional: verify browser launch actually works
echo "[$(date)] Verifying Playwright browser launch..." | tee -a "$LOG_FILE"
"$VENV_PYTHON" - <<'PY' >> "$LOG_FILE" 2>&1 || {
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    browser.close()
PY
    echo "[$(date)] ERROR: Playwright installed, but browser launch test failed." | tee -a "$LOG_FILE"
    exit 1
}

echo "[$(date)] Playwright OK. Launching scraper..." | tee -a "$LOG_FILE"
run_python_process "$SCRIPT_PATH" "$LOG_FILE"

echo "[$(date)] Scraper process started." | tee -a "$LOG_FILE"