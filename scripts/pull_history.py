"""Pull historical closed positions and P&L for all wallets.

Uses the /closed-positions endpoint to get resolved trade data with realized P&L,
then updates wallet scores based on actual historical performance.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


async def pull_history():
    from src.db import async_session
    from src.models import Wallet, Market, WhalePosition, WalletCategoryScore
    from src.polymarket.data_api import DataAPIClient
    from src.indexer.market_classifier import classify_market

    client = DataAPIClient()

    async with async_session() as session:
        result = await session.execute(select(Wallet.address))
        addresses = [r[0] for r in result.all()]

    logger.info("Pulling history for %d wallets", len(addresses))

    for i, addr in enumerate(addresses):
        logger.info("[%d/%d] %s", i + 1, len(addresses), addr[:12])
        try:
            # Get closed positions (resolved trades with P&L)
            closed = await client.get_closed_positions(addr)
            # Get open positions
            open_pos = await client.get_positions(addr)

            total_pnl = 0.0
            wins = 0
            losses = 0
            category_stats = {}

            for pos in closed:
                pnl = float(pos.realized_pnl or 0)
                total_pnl += pnl

                if pnl > 0:
                    wins += 1
                elif pnl < 0:
                    losses += 1

                # Classify market by title
                title = pos.title or ""
                cls = classify_market(title)
                cat = cls.category

                if cat not in category_stats:
                    category_stats[cat] = {"wins": 0, "losses": 0, "pnl": 0.0, "count": 0}
                category_stats[cat]["count"] += 1
                category_stats[cat]["pnl"] += pnl
                if pnl > 0:
                    category_stats[cat]["wins"] += 1
                elif pnl < 0:
                    category_stats[cat]["losses"] += 1

            # Compute bankroll from open positions
            bankroll = sum(float(p.initial_value or 0) for p in open_pos)
            bankroll = max(bankroll, abs(total_pnl))

            total_trades = wins + losses
            win_rate = wins / total_trades if total_trades > 0 else 0
            avg_pnl = total_pnl / total_trades if total_trades > 0 else 0

            # Compute profit factor
            total_wins_usdc = sum(float(p.realized_pnl or 0) for p in closed if float(p.realized_pnl or 0) > 0)
            total_losses_usdc = abs(sum(float(p.realized_pnl or 0) for p in closed if float(p.realized_pnl or 0) < 0))
            profit_factor = total_wins_usdc / total_losses_usdc if total_losses_usdc > 0 else (99.0 if total_wins_usdc > 0 else 0)

            logger.info("  %d closed positions, P&L=$%.0f, WR=%.0f%%, PF=%.2f",
                        len(closed), total_pnl, win_rate * 100, profit_factor)

            # Update wallet
            async with async_session() as session:
                wallet = await session.get(Wallet, addr)
                if wallet:
                    wallet.total_pnl_usdc = Decimal(str(round(total_pnl, 2)))
                    wallet.total_trades = len(closed) + len(open_pos)
                    wallet.estimated_bankroll = Decimal(str(round(bankroll, 2)))

                # Update category scores
                for cat, stats in category_stats.items():
                    if cat in ("other", "excluded", "ambiguous", None):
                        continue
                    cat_total = stats["wins"] + stats["losses"]
                    if cat_total == 0:
                        continue
                    cat_wr = stats["wins"] / cat_total
                    cat_wins_usdc = stats["pnl"] if stats["pnl"] > 0 else 0
                    cat_losses_usdc = abs(stats["pnl"]) if stats["pnl"] < 0 else 0
                    cat_pf = cat_wins_usdc / cat_losses_usdc if cat_losses_usdc > 0 else (99.0 if cat_wins_usdc > 0 else 0)
                    cat_exp = stats["pnl"] / cat_total

                    stmt = insert(WalletCategoryScore).values(
                        wallet_address=addr,
                        category=cat,
                        win_rate=Decimal(str(round(cat_wr, 4))),
                        profit_factor=Decimal(str(round(min(cat_pf, 99), 2))),
                        expectancy=Decimal(str(round(cat_exp, 4))),
                        trade_count=cat_total,
                        followability=Decimal("0.70"),  # provisional estimate
                        followability_provisional=True,
                        last_updated=datetime.now(timezone.utc),
                    ).on_conflict_do_update(
                        constraint="wallet_category_scores_pkey",
                        set_={
                            "win_rate": Decimal(str(round(cat_wr, 4))),
                            "profit_factor": Decimal(str(round(min(cat_pf, 99), 2))),
                            "expectancy": Decimal(str(round(cat_exp, 4))),
                            "trade_count": cat_total,
                            "last_updated": datetime.now(timezone.utc),
                        },
                    )
                    await session.execute(stmt)

                await session.commit()

            # Log category breakdown
            for cat, stats in sorted(category_stats.items(), key=lambda x: x[1]["count"], reverse=True):
                if stats["count"] >= 3:
                    ct = stats["wins"] + stats["losses"]
                    wr = stats["wins"] / ct if ct > 0 else 0
                    logger.info("    %s: %d trades, WR=%.0f%%, P&L=$%.0f",
                                cat, stats["count"], wr * 100, stats["pnl"])

        except Exception as e:
            logger.error("  Failed: %s", e)

    await client.close()

    # Re-score copyability
    logger.info("Re-scoring copyability...")
    from src.scorer.wallet_scorer import score_all_wallets
    results = await score_all_wallets()

    logger.info("\n=== FINAL WALLET RANKINGS ===")
    for r in sorted(results, key=lambda x: x.get("conviction_score", 0) if x else 0, reverse=True):
        if r:
            best = r.get("best_category") or "-"
            logger.info("  %s conv=%d %s best=%s",
                        r["address"][:12], r["conviction_score"], r["copyability"], best)


if __name__ == "__main__":
    asyncio.run(pull_history())
