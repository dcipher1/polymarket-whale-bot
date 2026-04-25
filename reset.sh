#!/usr/bin/env bash
# Full reset: stop bot, wipe DB + logs, ready for clean start
set -e

# Check for open trades — warn if Polymarket positions will become orphaned
OPEN_TRADES=$(psql -tA "postgresql://whale:whale@localhost:5432/whale_bot" -c "SELECT count(*) FROM my_trades WHERE resolved = false;" 2>/dev/null || echo "0")
if [ "$OPEN_TRADES" != "0" ]; then
    echo "WARNING: $OPEN_TRADES open trades in DB."
    echo "  Resetting will orphan these positions on Polymarket!"
    echo "  You must manually sell them on Polymarket or they'll be untracked."
    if [ "$1" != "--force" ]; then
        echo ""
        echo "  Pass --force to proceed anyway."
        exit 1
    fi
    echo "  --force passed, proceeding..."
fi

echo "Stopping bot..."
bash "$(dirname "$0")/stop.sh"

echo "Wiping database..."
psql "postgresql://whale:whale@localhost:5432/whale_bot" -c "
TRUNCATE
  my_trades,
  signals,
  portfolio_snapshots,
  whale_trades,
  whale_positions,
  wallet_category_scores,
  wallets,
  markets,
  market_tokens
CASCADE;
"

# Verify clean
WALLETS=$(psql -tA "postgresql://whale:whale@localhost:5432/whale_bot" -c "SELECT count(*) FROM wallets;")
if [ "$WALLETS" != "0" ]; then
    echo "ERROR: wallets table still has $WALLETS rows!"
    exit 1
fi

echo "Clearing logs..."
: > bot.log

echo "Flushing Redis cache..."
redis-cli FLUSHDB > /dev/null 2>&1 || true

echo "Reset complete (0 wallets verified). Start with:"
echo "  bash start.sh"
