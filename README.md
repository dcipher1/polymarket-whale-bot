# Polymarket 3-Wallet Copy Bot

This bot copies three hardcoded Polymarket weather wallets. It does not discover new wallets or use copyability scoring to decide what to trade.

## Runtime Behavior

Every 30 seconds the bot:

- Pulls open positions for the three wallets in `settings.watch_whales`.
- Filters to weather markets only.
- Computes target contracts as `whale_contracts * position_size_fraction`.
- Places at most `$50` notional per order while building toward the target.
- Cancels stale pending orders each cycle before requoting.
- Skips entries that fail penny-market, stale-price, slippage, min-order, position-cap, or resolution-backlog checks.
- Reconciles local fills against the Polymarket wallet and tracks resolution/PnL.

## Important Settings

Configuration lives in `src/config.py` and can be overridden from `.env`.

```env
DATABASE_URL=postgresql+asyncpg://whale:whale@localhost:5432/whale_bot
REDIS_URL=redis://localhost:6379/0
LIVE_EXECUTION_ENABLED=false
STARTING_CAPITAL=15000
```

Core strategy constants are currently in `src/signals/sync_positions.py`:

- `MAX_ORDER_USDC = 50.0`
- `CEILING_FRAC = 1.05`
- `FLOOR_FRAC = 0.80`
- `MIN_TRADE_PRICE = 0.10`
- `MAX_POSITION_FRAC_OF_WALLET = 0.10`

## Run

```bash
.venv/bin/whale-bot
```

or:

```bash
.venv/bin/python -m src.main
```

## Useful Scripts

- `scripts/audit_whale_bot.py`: read-only trade/accounting audit.
- `scripts/close_all.py`: dry-run or execute closing open bot positions.
- `scripts/redeem_all.py`: redeem resolved positions.
- `scripts/setup_wallet.py`: initialize live execution wallet credentials.

## Safety Notes

- Keep `LIVE_EXECUTION_ENABLED=false` unless wallet credentials, VPN/geoblock checks, and balance checks are verified.
- Runtime logs, DB dumps, generated data, local env backups, and MM research files are intentionally gitignored.
- The running process must be restarted after code/config changes.
