# Polymarket Whale Bot — Backup & Restore Guide

## What This Bot Does

Copies trades from profitable Polymarket whales. The bot continuously monitors whale wallets, detects new trades, scores them through risk/signal filters, and executes matching positions on your Polymarket account.

### Architecture

```
Indexer → Signals → Execution → Tracking
```

- **Indexer** (`src/indexer/`): Discovers and ingests whale wallets, builds their position history, classifies markets by category
- **Signals** (`src/signals/`): Monitors whale trades in real-time, generates candidate signals, checks price/convergence, manages signal lifecycle
- **Scorer** (`src/scorer/`): Scores wallets on performance, behavior, copyability — determines which whales are worth following
- **Execution** (`src/execution/`): Sizes positions, places orders via CLOB API, monitors fills, reconciles with on-chain state
- **Risk** (`src/risk/`): Drawdown limits, position limits, exposure checks
- **Tracking** (`src/tracking/`): PnL tracking, trade attribution, market resolution handling
- **Monitoring** (`src/monitoring/`): Web dashboard (port 8080), Telegram alerts

### Infrastructure

- **Runtime**: Python 3.11 on Raspberry Pi 5 (also works on any Linux box)
- **Database**: PostgreSQL (user: `whale`, db: `whale_bot`)
- **Cache**: Redis (localhost:6379)
- **VPN**: NordVPN (Polymarket is geoblocked in some regions)

---

## What's in This Backup

| File / Dir | Contents |
|---|---|
| `src/` | All bot source code |
| `scripts/` | Utility scripts (setup, wallet, trading helpers) |
| `.env` | **Private keys, API keys, Telegram tokens** — handle with care |
| `db_backup.sql` | Full PostgreSQL dump (trades, whales, positions, markets) |
| `crontab_backup.txt` | Cron entry for auto-start on boot |
| `data/` | Whale lists, discovered wallets, category mappings |
| `alembic/` | Database migration scripts |
| `start.sh` / `stop.sh` / `reset.sh` | Bot management scripts |
| `pyproject.toml` | Python dependencies |

**Not included** (recreated during setup):
- `.venv/` — Python virtual environment (rebuilt with `pip install`)
- `.git/` — Git history

---

## Deploy on a Fresh Raspberry Pi

### Option A: Automated Setup (recommended)

```bash
# 1. Copy and extract the backup
scp polymarket-whale-bot-backup.zip pi@<NEW_PI_IP>:/home/pi/
ssh pi@<NEW_PI_IP>
cd /home/pi && unzip polymarket-whale-bot-backup.zip

# 2. Run the setup script (installs PostgreSQL, Redis, Python, creates DB)
cd polymarket-whale-bot
sudo bash scripts/pi_setup.sh

# 3. Restore the database dump (pi_setup.sh imports data/whale_bot_dump.sql,
#    but this backup has a fresher dump at db_backup.sql)
source .venv/bin/activate
PGPASSWORD=whale psql -U whale -h localhost whale_bot < db_backup.sql

# 4. Restore crontab (auto-start on boot)
crontab crontab_backup.txt

# 5. Start the bot
bash start.sh
```

### Option B: Manual Setup

```bash
# 1. Extract
cd /home/pi && unzip polymarket-whale-bot-backup.zip && cd polymarket-whale-bot

# 2. Install system packages
sudo apt-get update
sudo apt-get install -y python3 python3-pip python3-venv postgresql redis-server libpq-dev

# 3. Start services
sudo systemctl enable --now postgresql redis-server

# 4. Create database
sudo -u postgres psql -c "CREATE USER whale WITH PASSWORD 'whale';"
sudo -u postgres psql -c "CREATE DATABASE whale_bot OWNER whale;"

# 5. Restore database
PGPASSWORD=whale psql -U whale -h localhost whale_bot < db_backup.sql

# 6. Python environment
python3 -m venv .venv
source .venv/bin/activate
pip install -e .

# 7. Restore crontab
crontab crontab_backup.txt

# 8. Start
bash start.sh
```

---

## Important Notes

- **`.env` contains your wallet private key** — never commit it to git or share it. Store this backup encrypted.
- **`LIVE_EXECUTION_ENABLED`** in `.env` controls real trading. Set to `false` for paper trading.
- **Dashboard** runs on port 8080 — accessible at `http://<PI_IP>:8080` when the bot is running.
- **Telegram alerts** require `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` in `.env`.
- **VPN**: If you're in a geoblocked region, set up NordVPN and configure iptables (see `start.sh` for the iptables rules).
- **`reset.sh`** wipes all database tables and logs — do NOT run it if you have open positions.

---

## Key Files Reference

| What | Where |
|---|---|
| Bot config | `src/config.py` |
| Main entry point | `src/main.py` |
| Environment variables | `.env` (secrets) / `.env.example` (template) |
| Database models | `src/models.py` |
| Bot logs | `bot.log` |
| Dashboard logs | `dashboard.log` |
| Start / stop / reset | `start.sh` / `stop.sh` / `reset.sh` |
| Pi setup script | `scripts/pi_setup.sh` |
| Database migrations | `alembic/` |
