#!/usr/bin/env bash
# ============================================================
# Setup TAT Pipeline on VM (db-taluithai.oswinfalk.xyz)
# ============================================================
# Run this script ON the VM as root:
#   bash setup_vm.sh
# ============================================================
set -euo pipefail

APP_DIR="/opt/tat-pipeline"
REPO_URL=""  # set if using git clone, otherwise we'll scp files

echo "================================================"
echo " TAT Pipeline — VM Setup"
echo "================================================"

# 1. Install system deps
echo "[1/6] Installing system dependencies..."
apt-get update -qq
apt-get install -y -qq python3 python3-venv python3-pip libpq-dev > /dev/null

# 2. Create app directory
echo "[2/6] Setting up $APP_DIR..."
mkdir -p "$APP_DIR/prefect_flows"

# 3. Copy files (assumes files are already in current dir via scp)
echo "[3/6] Copying files..."
cp -v prefect_flows/tat_daily_sync.py "$APP_DIR/prefect_flows/"
cp -v prefect_flows/__init__.py "$APP_DIR/prefect_flows/"
cp -v deploy/env.production "$APP_DIR/.env"

# 4. Create venv & install deps
echo "[4/6] Creating Python venv & installing packages..."
python3 -m venv "$APP_DIR/venv"
"$APP_DIR/venv/bin/pip" install --quiet --upgrade pip
"$APP_DIR/venv/bin/pip" install --quiet prefect psycopg2-binary python-dotenv requests

# 5. Login to Prefect Cloud
echo "[5/6] Logging into Prefect Cloud..."
source "$APP_DIR/.env"
"$APP_DIR/venv/bin/prefect" cloud login -k "$PREFECT_API_KEY"

# 6. Install & start systemd service
echo "[6/6] Installing systemd service..."
cp -v deploy/tat-prefect.service /etc/systemd/system/tat-prefect.service
systemctl daemon-reload
systemctl enable tat-prefect
systemctl start tat-prefect

echo ""
echo "================================================"
echo " Done! Service is running."
echo "================================================"
echo ""
echo " Check status:  systemctl status tat-prefect"
echo " View logs:     journalctl -u tat-prefect -f"
echo " Restart:       systemctl restart tat-prefect"
echo " Stop:          systemctl stop tat-prefect"
echo ""
