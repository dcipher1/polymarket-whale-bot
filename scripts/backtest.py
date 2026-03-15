"""Replay historical signals — APPROXIMATE backtest.

*** ALL RESULTS ARE APPROXIMATE ***
No synchronized orderbook snapshots exist. Timing is reconstructed from
trade timestamps. Real followability may differ. Paper trading with live
WebSocket data is the real validation.
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from sqlalchemy import select, and_

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


async def run_backtest(months: int = 6):
    from src.db import async_session
    from src.models import WhaleTrade, WhalePosition, Market, Wallet
    from src.config import settings

    cutoff = datetime.now(timezone.utc) - timedelta(days=months * 30)

    async with async_session() as session:
        # Get all resolved markets in the period
        result = await session.execute(
            select(Market).where(
                and_(
                    Market.resolved == True,
                    Market.resolution_time >= cutoff,
                    Market.category.in_(["macro", "crypto_weekly", "crypto_monthly", "politics", "geopolitics"]),
                )
            )
        )
        markets = {m.condition_id: m for m in result.scalars().all()}

        # Get COPYABLE wallets
        result = await session.execute(
            select(Wallet).where(Wallet.copyability_class == "COPYABLE")
        )
        copyable_wallets = {w.address for w in result.scalars().all()}

        if not copyable_wallets:
            logger.warning("No COPYABLE wallets — using WATCH wallets for backtest")
            result = await session.execute(
                select(Wallet).where(Wallet.copyability_class.in_(["COPYABLE", "WATCH"]))
            )
            copyable_wallets = {w.address for w in result.scalars().all()}

        logger.info(
            "Backtesting %d markets with %d tracked wallets over %d months",
            len(markets), len(copyable_wallets), months,
        )

        # Simulate signal generation
        wins = 0
        losses = 0
        total_pnl = 0.0
        signals_generated = 0

        for cid, market in markets.items():
            # Find whale trades in this market
            result = await session.execute(
                select(WhaleTrade).where(
                    and_(
                        WhaleTrade.condition_id == cid,
                        WhaleTrade.wallet_address.in_(copyable_wallets),
                        WhaleTrade.side == "BUY",
                        WhaleTrade.timestamp >= cutoff,
                    )
                )
                .order_by(WhaleTrade.timestamp)
            )
            trades = result.scalars().all()

            # Check for convergence (2+ independent wallets)
            wallet_entries: dict[str, float] = {}
            for trade in trades:
                if trade.wallet_address not in wallet_entries:
                    wallet_entries[trade.wallet_address] = float(trade.price)

            if len(wallet_entries) < 2:
                continue

            signals_generated += 1
            avg_entry = sum(wallet_entries.values()) / len(wallet_entries)

            # Simulate outcome
            winning_outcome = market.outcome.upper() if market.outcome else ""
            # Find which outcome the majority bet on
            outcome_counts: dict[str, int] = {}
            for trade in trades:
                o = trade.outcome.upper()
                outcome_counts[o] = outcome_counts.get(o, 0) + 1

            majority_outcome = max(outcome_counts, key=outcome_counts.get) if outcome_counts else "YES"

            contracts = int(settings.fixed_position_size_usdc / avg_entry) if avg_entry > 0 else 0

            if majority_outcome == winning_outcome:
                pnl = (1.0 - avg_entry) * contracts
                wins += 1
            else:
                pnl = -avg_entry * contracts
                losses += 1

            total_pnl += pnl

    total = wins + losses
    win_rate = wins / total if total > 0 else 0
    profit_factor = "N/A"  # would need individual trade amounts

    print("\n" + "=" * 60)
    print("*** APPROXIMATE BACKTEST RESULTS ***")
    print("*** NOT statistically validated — use paper trading ***")
    print("=" * 60)
    print(f"Period: {months} months")
    print(f"Signals generated: {signals_generated}")
    print(f"Trades: {total} ({wins}W / {losses}L)")
    print(f"Win Rate: {win_rate:.0%}")
    print(f"Total P&L: ${total_pnl:+,.2f}")
    print(f"Avg P&L/trade: ${total_pnl/total:+,.2f}" if total > 0 else "N/A")
    print(f"Position size: ${settings.fixed_position_size_usdc}")
    print("=" * 60)
    print("*** These results are APPROXIMATE — no real orderbook data ***")
    print()


if __name__ == "__main__":
    asyncio.run(run_backtest())
