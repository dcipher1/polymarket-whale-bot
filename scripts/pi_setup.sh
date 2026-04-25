#!/bin/bash
# Raspberry Pi 5 Setup Script for Polymarket Whale Bot
# Run as: sudo bash scripts/pi_setup.sh
#
# Prerequisites: Fresh Raspberry Pi OS (64-bit Bookworm)
# Transfer this entire project folder to the Pi first.

set -e

echo "=== Polymarket Whale Bot - Pi 5 Setup ==="

# 1. System packages
echo "[1/7] Installing system packages..."
apt-get update
apt-get install -y python3 python3-pip python3-venv postgresql redis-server \
    libpq-dev git

# 2. Start services
echo "[2/7] Starting PostgreSQL and Redis..."
systemctl enable postgresql
systemctl start postgresql
systemctl enable redis-server
systemctl start redis-server

# 3. Create database
echo "[3/7] Setting up database..."
sudo -u postgres psql -c "CREATE USER whale WITH PASSWORD 'whale';" 2>/dev/null || true
sudo -u postgres psql -c "CREATE DATABASE whale_bot OWNER whale;" 2>/dev/null || true
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE whale_bot TO whale;" 2>/dev/null || true

# 4. Import database dump (if exists)
if [ -f data/whale_bot_dump.sql ]; then
    echo "[4/7] Importing database dump..."
    PGPASSWORD=whale psql -U whale -h localhost whale_bot < data/whale_bot_dump.sql 2>/dev/null || {
        echo "  Dump import had issues, running migrations instead..."
        python3 -m alembic upgrade head
    }
else
    echo "[4/7] No dump found, running migrations..."
    python3 -m alembic upgrade head
fi

# 5. Python environment
echo "[5/7] Setting up Python environment..."
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# 6. Environment file
echo "[6/7] Setting up .env..."
if [ ! -f .env ]; then
    cp .env.example .env
    echo "  Created .env from example — edit it to add Telegram credentials"
fi

# 7. Systemd service
echo "[7/7] Installing systemd service..."
cat > /etc/systemd/system/whale-bot.service << 'UNIT'
[Unit]
Description=Polymarket Whale Bot
After=network.target postgresql.service redis.service
Wants=postgresql.service redis.service

[Service]
Type=simple
User=pi
WorkingDirectory=/home/pi/polymarket-whale-bot
ExecStart=/home/pi/polymarket-whale-bot/.venv/bin/python -m src.main
Restart=always
RestartSec=10
StandardOutput=append:/home/pi/polymarket-whale-bot/bot.log
StandardError=append:/home/pi/polymarket-whale-bot/bot.log
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable whale-bot

echo ""
echo "=== Setup Complete ==="
echo ""
echo "To start the bot:"
echo "  sudo systemctl start whale-bot"
echo ""
echo "To check status:"
echo "  sudo systemctl status whale-bot"
echo "  tail -f /home/pi/polymarket-whale-bot/bot.log"
echo ""
echo "To stop:"
echo "  sudo systemctl stop whale-bot"
echo ""
echo "The bot will auto-start on boot."
echo ""
echo "IMPORTANT: Edit .env to add your Telegram bot token and chat ID"
