"""Master wallet scorer — orchestrates all sub-scorers."""

import logging
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select, update, and_, or_, func
from sqlalchemy.dialects.postgresql import insert
from src.db import async_session
from src.models import Wallet, WalletCategoryScore, WhaleTrade, Market
from src.scorer.performance import compute_performance
from src.scorer.behavior import compute_behavior
from src.scorer.followability import compute_followability
from src.config import settings
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

        # Get most recent BUY trade per category (any market, not just resolved)
        # Used for recency gate — resolved-only would miss active traders on open markets
        last_buy_by_cat = {}
        for category in active_categories:
            result = await session.execute(
                select(func.max(WhaleTrade.timestamp))
                .join(Market, WhaleTrade.condition_id == Market.condition_id)
                .where(
                    and_(
                        WhaleTrade.wallet_address == wallet_address,
                        WhaleTrade.side == "BUY",
                        WhaleTrade.price <= settings.max_entry_price_trade,
                        Market.category == category,
                    )
                )
            )
            last_buy_by_cat[category] = result.scalar()

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
                category_pnl=Decimal(str(round(perf.total_wins_usdc - perf.total_losses_usdc, 2))),
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
                    "category_pnl": Decimal(str(round(perf.total_wins_usdc - perf.total_losses_usdc, 2))),
                    "last_updated": now,
                },
            )
            await session.execute(stmt)

            if perf.trade_count == 0:
                continue

            category_metrics[category] = {
                "win_rate": perf.win_rate,
                "profit_factor": perf.profit_factor,
                "trade_count": perf.trade_count,
                "wins": perf.win_count,
                "losses": perf.loss_count,
                "followability": follow.primary_followability,
                "expectancy": perf.expectancy,
                "category_pnl": round(perf.total_wins_usdc - perf.total_losses_usdc, 2),
                "last_trade_ts": last_buy_by_cat.get(category),
            }

        # Classify copyability (purely category-based)
        total_pnl = float(wallet.total_pnl_usdc or 0)

        # Whitelist override — skip all qualification gates
        if (wallet.meta or {}).get("whitelisted"):
            copyability = "COPYABLE"
            best_cat = max(category_metrics, key=lambda c: category_metrics[c].get("category_pnl", 0)) if category_metrics else None
        else:
            copyability, best_cat = classify_copyability(
                conviction_score=behavior.conviction_score,
                category_metrics=category_metrics,
                current_class=wallet.copyability_class,
            )

        # Tag qualifying categories in DB
        from src.scorer.copyability import is_category_qualifying
        for category in active_categories:
            metrics = category_metrics.get(category, {})
            is_qualifying = is_category_qualifying(metrics)
            await session.execute(
                update(WalletCategoryScore)
                .where(and_(
                    WalletCategoryScore.wallet_address == wallet_address,
                    WalletCategoryScore.category == category,
                ))
                .values(qualifying=is_qualifying)
            )

        # Whitelist safety net: if whitelisted but no category qualified, force-qualify best category
        if (wallet.meta or {}).get("whitelisted"):
            has_qualifying = any(
                is_category_qualifying(category_metrics.get(c, {}))
                for c in active_categories
            )
            if not has_qualifying and category_metrics:
                forced_cat = max(
                    (c for c in category_metrics if category_metrics[c].get("trade_count", 0) > 0),
                    key=lambda c: category_metrics[c].get("trade_count", 0),
                    default=None,
                )
                if forced_cat:
                    await session.execute(
                        update(WalletCategoryScore)
                        .where(and_(
                            WalletCategoryScore.wallet_address == wallet_address,
                            WalletCategoryScore.category == forced_cat,
                        ))
                        .values(qualifying=True)
                    )
                    logger.info(
                        "Whitelist force-qualified %s in %s (%d trades)",
                        wallet.display_name or wallet_address[:10],
                        forced_cat,
                        category_metrics[forced_cat].get("trade_count", 0),
                    )

        # Compute specialization ratio: best_category_pnl / total_pnl
        # Specialists (concentrated in one category) get higher ratio → sizing multiplier
        best_cat_pnl = 0.0
        if category_metrics:
            best_cat_pnl = max(
                m.get("category_pnl", 0) for m in category_metrics.values()
            )
        spec_ratio = round(best_cat_pnl / total_pnl, 4) if total_pnl > 0 else 0.0
        spec_ratio = max(0.0, min(spec_ratio, 1.0))

        # Update wallet
        old_class = wallet.copyability_class
        wallet.conviction_score = behavior.conviction_score
        wallet.copyability_class = copyability
        if old_class != copyability:
            name = wallet.display_name or wallet_address[:10]
            logger.warning(
                "Wallet %s class changed: %s → %s (best_cat=%s, total_pnl=$%.0f, conviction=%d, trades/mo=%.0f)",
                name, old_class, copyability, best_cat, total_pnl,
                behavior.conviction_score, behavior.trades_per_month,
            )
            # Log per-category gate breakdown so we know exactly why
            from src.scorer.copyability import wilson_lower_bound
            for cat, m in category_metrics.items():
                wins = m.get("wins", 0)
                tc = m.get("trade_count", 0)
                wlb = wilson_lower_bound(wins, tc)
                pf = m.get("profit_factor", 0)
                cat_pnl = m.get("category_pnl", 0)
                last_ts = m.get("last_trade_ts")
                if last_ts:
                    age_days = (datetime.now(timezone.utc) - last_ts).total_seconds() / 86400
                else:
                    age_days = 999
                gates = {
                    f"wilson={wlb:.2f}": wlb >= settings.min_win_rate,
                    f"pf={pf:.1f}": pf >= settings.min_profit_factor,
                    f"trades={tc}": tc >= settings.min_resolved_trades,
                    f"recent={age_days:.0f}d": age_days <= settings.max_qualifying_trade_age_days,
                    f"conviction={behavior.conviction_score}": behavior.conviction_score >= settings.min_conviction_score,
                    f"cat_pnl=${cat_pnl:.0f}": cat_pnl >= settings.get_min_category_pnl(cat),
                }
                parts = [f"{k}{'✓' if v else '✗'}" for k, v in gates.items()]
                logger.warning("  %s %s: %s", name, cat, "  ".join(parts))
        wallet.trades_per_month = Decimal(str(behavior.trades_per_month))
        wallet.median_hold_hours = Decimal(str(behavior.median_hold_hours))
        wallet.pct_held_to_resolution = Decimal(str(behavior.pct_held_to_resolution))
        wallet.estimated_bankroll = Decimal(str(behavior.estimated_bankroll))
        wallet.specialization_ratio = Decimal(str(spec_ratio))
        wallet.last_scored = datetime.now(timezone.utc)

        await session.commit()

        summary = {
            "address": wallet_address,
            "conviction_score": behavior.conviction_score,
            "copyability": copyability,
            "best_category": best_cat,
            "specialization_ratio": spec_ratio,
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
        # Subquery: count distinct market-positions with BUY trades on resolved markets
        post_filter_trades = (
            select(func.count(func.distinct(
                func.concat(WhaleTrade.condition_id, ':', WhaleTrade.outcome)
            )))
            .join(Market, WhaleTrade.condition_id == Market.condition_id)
            .where(
                and_(
                    WhaleTrade.wallet_address == Wallet.address,
                    WhaleTrade.side == "BUY",
                    Market.resolved == True,
                )
            )
            .correlate(Wallet)
            .scalar_subquery()
        )

        result = await session.execute(
            select(Wallet.address).where(
                and_(
                    # Skip MM wallets
                    or_(
                        Wallet.meta["reject_reason"].astext.notlike("MM %"),
                        ~Wallet.meta.has_key("reject_reason"),
                        Wallet.meta.is_(None),
                    ),
                    or_(
                        Wallet.meta["mm_flagged"].astext != "true",
                        ~Wallet.meta.has_key("mm_flagged"),
                        Wallet.meta.is_(None),
                    ),
                    # Must be promoted OR have enough post-filter trades
                    or_(
                        Wallet.copyability_class.in_(["COPYABLE", "WATCH"]),
                        post_filter_trades >= settings.min_resolved_trades,
                    ),
                )
            )
        )
        addresses = [r[0] for r in result.all()]

    logger.info("Scoring %d wallets", len(addresses))

    # Phase 1: Score each wallet
    logger.info("Phase 1/2: Scoring wallets...")
    results = []
    for i, address in enumerate(addresses, 1):
        try:
            summary = await score_wallet(address)
            results.append(summary)
            logger.debug("Scored %d/%d: %s → %s", i, len(addresses), address[:10], summary.get("new_class", "?"))
        except Exception as e:
            logger.error("Failed to score wallet %s: %s", address[:10], e)
        if i % 50 == 0:
            logger.info("Scoring progress: %d/%d", i, len(addresses))

    logger.info("Phase 1 done: scored %d wallets", len(results))

    # Phase 2: MM + cluster detection
    logger.info("Phase 2/2: Running MM and cluster detection...")
    # Run MM detection before cluster detection
    try:
        from src.scorer.mm_detector import flag_market_makers
        async with async_session() as session:
            mm_count = await flag_market_makers(session)
            if mm_count > 0:
                logger.info("MM detection flagged %d wallets as REJECT", mm_count)
            await session.commit()
    except Exception as e:
        logger.warning("MM detection failed: %s", e)

    return results
