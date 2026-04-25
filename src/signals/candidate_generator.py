"""Generate CANDIDATE signals from whale trades."""

import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from sqlalchemy import select, and_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.db import async_session
from src.indexer.market_ingester import ensure_market
from src.models import (
    Signal, Wallet, WalletCategoryScore, Market,
    WhalePosition, WhaleTrade,
)

logger = logging.getLogger(__name__)


async def generate_candidate(
    wallet_address: str,
    condition_id: str,
    outcome: str,
    whale_price: float,
    event_type: str,
) -> int | None:
    """Create or update a CANDIDATE signal. Returns signal ID if created.

    For ADD events on existing signals: updates the price but returns None
    to suppress convergence re-evaluation (prevents duplicate paper trades).
    """

    # Only OPEN and ADD events feed into convergence
    if event_type not in ("OPEN", "ADD"):
        return None

    async with async_session() as session:
        # Check wallet is COPYABLE
        wallet = await session.get(Wallet, wallet_address)
        if not wallet or wallet.copyability_class != "COPYABLE":
            return None

        # Check market is in our universe (fetch on-demand if not in DB)
        market = await ensure_market(condition_id, session)
        if not market:
            return None

        category = market.category_override or market.category
        if category in ("other", "excluded", "ambiguous", None):
            return None

        # Compute time to resolution (informational, no longer a gate)
        if market.resolution_time:
            hours_left = (
                market.resolution_time - datetime.now(timezone.utc)
            ).total_seconds() / 3600
            if market.resolved:
                return None  # already resolved
        else:
            hours_left = None

        # Check wallet's category score
        result = await session.execute(
            select(WalletCategoryScore).where(
                and_(
                    WalletCategoryScore.wallet_address == wallet_address,
                    WalletCategoryScore.category == category,
                )
            )
        )
        cat_score = result.scalar_one_or_none()

        # Per-category quality gate: whale must have proven edge in this category
        if cat_score:
            tc = cat_score.trade_count or 0
            wr = float(cat_score.win_rate or 0)
            pf = float(cat_score.profit_factor or 0)
            if tc < settings.candidate_min_trade_count or wr < settings.candidate_min_win_rate or pf < settings.candidate_min_profit_factor:
                logger.info(
                    "  SKIP category_edge: %s has no edge in %s (trades=%d wr=%.2f pf=%.1f) for %s",
                    wallet_address[:10], category, tc, wr, pf, condition_id[:10],
                )
                return None
        else:
            logger.info(
                "  SKIP no_category_score: %s has no score for %s (%s)",
                wallet_address[:10], category, condition_id[:10],
            )
            return None

        # Check for existing candidate from this wallet on same market+outcome
        existing = await session.execute(
            select(Signal).where(
                and_(
                    Signal.source_wallets.contains([wallet_address]),
                    Signal.condition_id == condition_id,
                    Signal.outcome == outcome,
                    Signal.signal_type == "CANDIDATE",
                    Signal.status == "PENDING",
                )
            )
        )
        existing_signal = existing.scalar_one_or_none()

        convergence_window = settings.get_convergence_window(category)
        expires_at = datetime.now(timezone.utc) + timedelta(hours=convergence_window)

        if existing_signal:
            # ADD event on existing signal — update price but return None
            # to suppress convergence re-evaluation (prevents duplicate trades)
            existing_signal.whale_avg_price = Decimal(str(whale_price))
            existing_signal.created_at = datetime.now(timezone.utc)
            existing_signal.expires_at = expires_at
            await session.commit()
            logger.info(
                "ADD reinforcement on signal %d for %s on %s (price updated, no re-convergence)",
                existing_signal.id, wallet_address[:10], condition_id[:10],
            )
            return None  # Suppress re-convergence

        # Create new candidate
        signal = Signal(
            condition_id=condition_id,
            signal_type="CANDIDATE",
            outcome=outcome,
            source_wallets=[wallet_address],
            whale_avg_price=Decimal(str(whale_price)),
            category=category,
            hours_to_resolution=Decimal(str(hours_left)) if hours_left else None,
            convergence_count=1,
            created_at=datetime.now(timezone.utc),
            expires_at=expires_at,
            status="PENDING",
        )
        session.add(signal)
        await session.commit()

        logger.info(
            "Created CANDIDATE signal %d: wallet=%s market=%s outcome=%s price=%.4f",
            signal.id, wallet_address[:10], condition_id[:10], outcome, whale_price,
        )
        return signal.id
