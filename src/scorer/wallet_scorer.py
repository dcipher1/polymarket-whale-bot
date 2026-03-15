"""Master wallet scorer — orchestrates all sub-scorers."""

import logging
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.db import async_session
from src.models import Wallet, WalletCategoryScore, WhaleTrade, Market
from src.scorer.performance import compute_performance
from src.scorer.behavior import compute_behavior
from src.scorer.followability import compute_followability
from src.scorer.copyability import classify_copyability, VALID_CATEGORIES

logger = logging.getLogger(__name__)


async def score_wallet(wallet_address: str) -> dict:
    """Run all scorers for a wallet and update DB. Returns summary."""
    async with async_session() as session:
        wallet = await session.get(Wallet, wallet_address)
        if not wallet:
            logger.warning("Wallet %s not found", wallet_address[:10])
            return {}

        # Compute behavior metrics (global)
        behavior = await compute_behavior(session, wallet_address)

        # Find which categories this wallet has trades in
        result = await session.execute(
            select(Market.category)
            .join(WhaleTrade, WhaleTrade.condition_id == Market.condition_id)
            .where(WhaleTrade.wallet_address == wallet_address)
            .where(Market.category.in_(VALID_CATEGORIES))
            .distinct()
        )
        active_categories = [r[0] for r in result.all() if r[0]]

        # Compute per-category metrics
        category_metrics = {}
        for category in active_categories:
            perf = await compute_performance(session, wallet_address, category)
            follow = await compute_followability(session, wallet_address, category)

            # Store category scores
            now = datetime.now(timezone.utc)
            stmt = insert(WalletCategoryScore).values(
                wallet_address=wallet_address,
                category=category,
                win_rate=Decimal(str(perf.win_rate)),
                profit_factor=Decimal(str(perf.profit_factor)),
                gain_loss_ratio=Decimal(str(perf.gain_loss_ratio)),
                expectancy=Decimal(str(perf.expectancy)),
                trade_count=perf.trade_count,
                avg_hold_hours=Decimal(str(perf.avg_hold_hours)),
                followability_5m=Decimal(str(follow.followability_5m)),
                followability_30m=Decimal(str(follow.followability_30m)),
                followability_2h=Decimal(str(follow.followability_2h)),
                followability_24h=Decimal(str(follow.followability_24h)),
                followability=Decimal(str(follow.primary_followability)),
                followability_provisional=follow.provisional,
                last_updated=now,
            ).on_conflict_do_update(
                constraint="wallet_category_scores_pkey",
                set_={
                    "win_rate": Decimal(str(perf.win_rate)),
                    "profit_factor": Decimal(str(perf.profit_factor)),
                    "gain_loss_ratio": Decimal(str(perf.gain_loss_ratio)),
                    "expectancy": Decimal(str(perf.expectancy)),
                    "trade_count": perf.trade_count,
                    "avg_hold_hours": Decimal(str(perf.avg_hold_hours)),
                    "followability_5m": Decimal(str(follow.followability_5m)),
                    "followability_30m": Decimal(str(follow.followability_30m)),
                    "followability_2h": Decimal(str(follow.followability_2h)),
                    "followability_24h": Decimal(str(follow.followability_24h)),
                    "followability": Decimal(str(follow.primary_followability)),
                    "followability_provisional": follow.provisional,
                    "last_updated": now,
                },
            )
            await session.execute(stmt)

            category_metrics[category] = {
                "win_rate": perf.win_rate,
                "profit_factor": perf.profit_factor,
                "trade_count": perf.trade_count,
                "followability": follow.primary_followability,
                "expectancy": perf.expectancy,
            }

        # Classify copyability
        total_pnl = float(wallet.total_pnl_usdc or 0)
        copyability, best_cat = classify_copyability(
            conviction_score=behavior.conviction_score,
            total_pnl=total_pnl,
            trades_per_month=behavior.trades_per_month,
            category_metrics=category_metrics,
        )

        # Update wallet
        wallet.conviction_score = behavior.conviction_score
        wallet.copyability_class = copyability
        wallet.trades_per_month = Decimal(str(behavior.trades_per_month))
        wallet.median_hold_hours = Decimal(str(behavior.median_hold_hours))
        wallet.pct_held_to_resolution = Decimal(str(behavior.pct_held_to_resolution))
        wallet.estimated_bankroll = Decimal(str(behavior.estimated_bankroll))
        wallet.last_scored = datetime.now(timezone.utc)

        await session.commit()

        summary = {
            "address": wallet_address,
            "conviction_score": behavior.conviction_score,
            "copyability": copyability,
            "best_category": best_cat,
            "categories": category_metrics,
            "trades_per_month": behavior.trades_per_month,
            "median_hold_hours": behavior.median_hold_hours,
        }
        logger.info(
            "Scored wallet %s: conviction=%d copyability=%s best_cat=%s",
            wallet_address[:10],
            behavior.conviction_score,
            copyability,
            best_cat,
        )
        return summary


async def score_all_wallets() -> list[dict]:
    """Score all tracked wallets."""
    async with async_session() as session:
        result = await session.execute(select(Wallet.address))
        addresses = [r[0] for r in result.all()]

    logger.info("Scoring %d wallets", len(addresses))
    results = []
    for address in addresses:
        try:
            summary = await score_wallet(address)
            results.append(summary)
        except Exception as e:
            logger.error("Failed to score wallet %s: %s", address[:10], e)

    # Run cluster detection to identify same-operator wallets
    try:
        from src.scorer.cluster_detector import detect_clusters
        async with async_session() as session:
            clusters = await detect_clusters(session)
            if clusters:
                logger.info("Cluster detection found %d clustered wallets", len(clusters))
    except Exception as e:
        logger.warning("Cluster detection failed: %s", e)

    return results
