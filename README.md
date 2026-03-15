# Polymarket Whale Copy-Trading Signal Engine

## What This Is

A Python system that monitors profitable Polymarket whale wallets, detects when multiple independent whales converge on the same market outcome, and generates copy-trade signals. Currently running Phase 1-2 (research + paper trading) — no real money is being traded.

## How It Works (The Pipeline)

```
Discover Whales → Ingest Trades → Score Wallets → Monitor for New Trades
                                                         ↓
                                              Detect CANDIDATE signal
                                                         ↓
                                              Check CONVERGENCE (2+ whales same market)
                                                         ↓
                                              Check price viability + fees
                                                         ↓
                                              Log PAPER TRADE in database
```

### Key Concepts

- **COPYABLE wallet**: A whale with proven P&L ($50K+), good win rate (55%+), and conviction-style trading (not a bot). We copy these.
- **WATCH wallet**: Promising but doesn't meet all thresholds yet. Monitored but not copied.
- **CANDIDATE signal**: One COPYABLE whale entered a target-category market. Waiting for convergence.
- **CONVERGENCE**: 2+ independent COPYABLE wallets bet the same side of the same market within a time window. This is the core signal.
- **LIVE/PAPER TRADE**: Convergence passed price and fee checks. Logged as a paper trade (no real execution yet).
- **Target categories**: politics, geopolitics, macro (Fed/CPI/etc), crypto_weekly, crypto_monthly. Sports and entertainment ("other") are filtered out.
- **Conviction score (0-100)**: Measures if a wallet trades like a researcher (high score) vs a bot/market-maker (low score). Based on trade frequency, hold duration, % held to resolution, position concentration.
- **Followability**: Can we actually follow this whale's trades? Measures if the price is still good 5min/30min/2h/24h after they enter.

### What Runs Automatically

| Job | Interval | What It Does |
|-----|----------|-------------|
| Trade monitor | 30 seconds | Polls COPYABLE wallets for new trades, generates signals |
| Market refresh | 15 minutes | Fetches new/updated markets from Gamma API |
| Resolution tracker | 15 minutes | Checks if markets resolved, computes P&L on paper trades |
| Signal expiry | 5 minutes | Expires stale CANDIDATE signals past convergence window |
| Wallet rescoring | 6 hours | Recomputes conviction scores and copyability |
| Whale discovery | 24 hours | Scans category leaderboards for new whales to track |
| Daily snapshot | 23:55 UTC | Records portfolio state for historical tracking |
| WebSocket | Continuous | Live price feed from Polymarket CLOB |

## Infrastructure

### Services (must be running)

| Service | How It Was Installed | Default Port |
|---------|---------------------|-------------|
| **PostgreSQL 16** | `winget install PostgreSQL.PostgreSQL.16` | 5432 |
| **Redis 3** | `winget install Redis.Redis` | 6379 |

Both are installed as Windows services and should auto-start on boot.

### Database

- **Host**: localhost:5432
- **Database**: whale_bot
- **User/Password**: whale / whale
- **Tables**: wallets, wallet_category_scores, markets, market_tokens, whale_trades, whale_positions, signals, my_trades, portfolio_snapshots

### Key Config

All config is in `src/config.py` via Pydantic Settings, overridable by `.env` file:

```
DATABASE_URL=postgresql+asyncpg://whale:whale@localhost:5432/whale_bot
REDIS_URL=redis://localhost:6379/0
LIVE_EXECUTION_ENABLED=false   # Phase 3+ only — DO NOT enable without review
STRICT_MODE=true               # Fail-closed: skip signal if any check fails
STARTING_CAPITAL=15000
FIXED_POSITION_SIZE_USDC=100   # $100 per paper trade
```

### Scoring Thresholds (tunable in config.py)

```
min_wallet_pnl = 50,000        # Min P&L to be COPYABLE
min_win_rate = 0.55             # 55% win rate minimum
min_profit_factor = 1.3         # Gross wins / gross losses
min_resolved_trades = 10        # Min settled trades for scoring
min_conviction_score = 40       # 0-100, filters out bots
max_trades_per_month = 200      # Filters high-frequency bots
min_followability = 0.50        # Can we follow their entries?
convergence_min_wallets = 2     # Min independent whales for signal
max_slippage_pct = 0.10         # Max 10% price move from whale entry
```

## Directory Structure

```
polymarket-whale-bot/
├── src/
│   ├── main.py                    # Entry point — starts everything
│   ├── config.py                  # All settings (Pydantic)
│   ├── db.py                      # SQLAlchemy async engine
│   ├── models.py                  # ORM models (all 9 tables)
│   ├── events.py                  # Redis pub/sub + caching
│   ├── polymarket/                # API clients
│   │   ├── data_api.py            # Wallet trades/positions (Data API)
│   │   ├── gamma_api.py           # Market metadata (Gamma API)
│   │   ├── clob_client.py         # Live prices (CLOB API)
│   │   ├── websocket.py           # Real-time price WebSocket
│   │   ├── geoblock.py            # Geo-restriction check
│   │   └── fees.py                # Per-market fee lookup
│   ├── indexer/                   # Data ingestion
│   │   ├── wallet_ingester.py     # Fetch + store wallet trades
│   │   ├── market_ingester.py     # Fetch + store markets
│   │   ├── market_classifier.py   # Classify into categories
│   │   ├── position_builder.py    # Derive positions from trades
│   │   └── whale_discovery.py     # Auto-find new whales from leaderboard
│   ├── scorer/                    # Wallet analysis
│   │   ├── wallet_scorer.py       # Orchestrates all scoring
│   │   ├── performance.py         # Win rate, profit factor, expectancy
│   │   ├── behavior.py            # Conviction score
│   │   ├── followability.py       # Can we follow their entries?
│   │   ├── cluster_detector.py    # Same-operator wallet detection
│   │   └── copyability.py         # REJECT/WATCH/COPYABLE classification
│   ├── signals/                   # Signal generation
│   │   ├── trade_monitor.py       # Polls wallets, triggers pipeline
│   │   ├── candidate_generator.py # Creates CANDIDATE signals
│   │   ├── convergence.py         # Detects multi-whale convergence
│   │   ├── price_checker.py       # Validates price + promotes to LIVE
│   │   └── signal_manager.py      # Lifecycle, expiry, diagnostics
│   ├── execution/
│   │   ├── paper_executor.py      # Logs paper trades (Phase 1-2)
│   │   ├── position_sizer.py      # Fixed $100 per trade (Kelly later)
│   │   └── order_manager.py       # Stub for Phase 3+ real execution
│   ├── risk/
│   │   ├── risk_manager.py        # Pre-trade exposure/drawdown checks
│   │   └── drawdown.py            # Daily/weekly/monthly drawdown
│   ├── tracking/
│   │   ├── resolution.py          # Monitors market outcomes
│   │   ├── pnl.py                 # Portfolio P&L calculations
│   │   ├── attribution.py         # Which whales generate alpha
│   │   └── snapshots.py           # Daily portfolio snapshots
│   └── monitoring/
│       ├── dashboard.py           # Rich CLI dashboard
│       └── telegram.py            # Telegram bot (alerts + commands)
├── scripts/
│   ├── discover_wallets.py        # Manual whale discovery from leaderboard
│   ├── seed_wallets.py            # Seed wallets from JSON file
│   ├── backfill_history.py        # Backfill + score all wallets
│   ├── pull_history.py            # Pull closed-position P&L data
│   ├── import_resolved.py         # Import resolved markets with outcomes
│   ├── backtest.py                # Approximate historical replay
│   └── export_report.py           # Export CSV reports
├── data/
│   ├── seed_wallets.json
│   ├── discovered_wallets.json
│   ├── category_whales.json
│   ├── economic_calendar.json
│   └── api_responses/             # Raw API payloads (gitignored)
├── tests/                         # 60 unit tests
├── alembic/                       # Database migrations
├── docker-compose.yml             # PostgreSQL + Redis (alternative to native)
├── pyproject.toml                 # Dependencies
├── .env                           # Secrets (not in git)
└── bot.log                        # Runtime logs
```

## Polymarket APIs Used

| API | Base URL | Auth | Purpose |
|-----|----------|------|---------|
| Data API | data-api.polymarket.com | None | Wallet trades, positions, activity |
| Gamma API | gamma-api.polymarket.com | None | Market metadata, resolution status |
| CLOB API | clob.polymarket.com | None (read) | Live prices via /midpoint |
| CLOB WebSocket | wss://ws-subscriptions-clob.polymarket.com | None | Real-time price updates |
| Leaderboard | data-api.polymarket.com/v1/leaderboard | None | Whale discovery by category |
| Geoblock | polymarket.com/api/geoblock | None | Check if region is blocked |

### API Quirks Discovered

- `/v1/leaderboard` supports `category` param: `politics`, `crypto`, `economics`
- `/trades` endpoint uses `user=` parameter (discovered at runtime, cached in Redis)
- Gamma API fields are camelCase (`conditionId`, `clobTokenIds`, `endDate`)
- `clobTokenIds` and `outcomes` are JSON strings, not arrays
- `/price` endpoint requires a `side` param — use `/midpoint` instead
- Activity/trades pagination returns 400 when offset exceeds available data (not empty array)
- CLOB token IDs are long numeric strings (not hex)

## How to Restart After Crash

### Quick Restart

```powershell
cd C:\Users\hotdo\polymarket-whale-bot
python -m src.main > bot.log 2>&1
```

Or to run in background (from Git Bash):
```bash
cd C:/Users/hotdo/polymarket-whale-bot && python -m src.main > bot.log 2>&1 &
```

### Verify Services Are Running

```powershell
# Check PostgreSQL
& 'C:\Program Files\PostgreSQL\16\bin\pg_isready.exe' -h localhost

# Check Redis
& 'C:\Program Files\Redis\redis-cli.exe' ping

# Check bot process
tasklist | findstr python
```

### If PostgreSQL Is Down

It's a Windows service — should auto-start. If not:
```powershell
net start postgresql-x64-16
```

### If Redis Is Down

```powershell
net start Redis
```

### If Database Is Empty (fresh start)

```powershell
cd C:\Users\hotdo\polymarket-whale-bot

# Create tables
python -m alembic upgrade head

# Ingest markets
python -c "import asyncio; from src.indexer.market_ingester import ingest_markets; asyncio.run(ingest_markets())"

# Import resolved markets
python scripts/import_resolved.py

# Discover and ingest whales
python scripts/discover_wallets.py
python scripts/seed_wallets.py data/discovered_wallets.json

# Pull P&L history
python scripts/pull_history.py

# Start bot
python -m src.main > bot.log 2>&1
```

### Check Status

```powershell
cd C:\Users\hotdo\polymarket-whale-bot

# Dashboard
python -c "import asyncio; from src.monitoring.dashboard import render_dashboard; asyncio.run(render_dashboard())"

# Quick DB check (from Git Bash)
PGPASSWORD=whale psql -U whale -h localhost -d whale_bot -c "
  SELECT count(*) as wallets FROM wallets;
  SELECT count(*) as trades FROM whale_trades;
  SELECT count(*) as signals FROM signals;
  SELECT count(*) as paper_trades FROM my_trades;
"

# Check paper trade P&L
python -c "import asyncio; from src.tracking.pnl import get_portfolio_pnl; p=asyncio.run(get_portfolio_pnl()); print(p)"

# Tail logs
Get-Content bot.log -Tail 30
```

## Current State (as of March 15, 2026)

- **67 wallets** tracked (33 COPYABLE, 25 WATCH, 9 REJECT)
- **~35,000 markets** ingested (~10,000 resolved)
- **~6,000 whale trades** stored
- **136 candidate signals** pending convergence
- **10 active paper trades** (geopolitics + politics)
- **$1,000 total paper exposure** ($100 per trade)
- Phase: **Paper trading** (no real money)
- Geoblock: **Passed** (Quebec, Canada — allowed)

## Phase Roadmap

| Phase | Status | What |
|-------|--------|------|
| Phase 1 | DONE | Research system — ingestion, scoring, classification |
| Phase 2 | ACTIVE | Paper trading — signals, convergence, paper execution |
| Phase 3 | NOT STARTED | Live execution with flat $100/trade via CLOB API |
| Phase 4 | NOT STARTED | Scale up position sizes |
| Phase 5 | NOT STARTED | Kelly criterion sizing after 50+ live trades |

### Phase 3 Gate Criteria (before going live)

- [ ] 30+ paper trades with profit factor > 1.3
- [ ] Expectancy > $0 per trade after fees
- [ ] Followability > 50% with live price data
- [ ] 3+ independent whales contributing positive P&L
- [ ] 2-5 signals per week minimum
- [ ] No single whale > 40% of total signals
- [ ] 2+ weeks of paper trading

## Telegram Bot (optional)

Add to `.env`:
```
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
```

Commands: `/status`, `/wallets`, `/markets`, `/health`, `/signals`, `/pnl`, `/diagnostics`

Alerts: new signals, paper trades, market resolutions, whale exits, daily summary.
