"""Followability scoring: can we follow this whale's trades in time?"""

import logging
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.models import WhaleTrade, Market

logger = logging.getLogger(__name__)

# Time windows in minutes
WINDOWS = {
    "5m": 5,
    "30m": 30,
    "2h": 120,
    "24h": 1440,
}


@dataclass
class FollowabilityResult:
    followability_5m: float = 0.0
    followability_30m: float = 0.0
    followability_2h: float = 0.0
    followability_24h: float = 0.0
    primary_followability: float = 0.0
    provisional: bool = True
    sample_size: int = 0


async def compute_followability(
    session: AsyncSession,
    wallet_address: str,
    category: str,
) -> FollowabilityResult:
    """Compute followability at multiple time windows for a wallet in a category.

    Phase 1 approximation: uses the next recorded trade in the same market
    as a proxy for price at each time window.
    """

    # Get all BUY trades for this wallet in this category
    result = await session.execute(
        select(WhaleTrade, Market)
        .join(Market, WhaleTrade.condition_id == Market.condition_id)
        .where(
            and_(
                WhaleTrade.wallet_address == wallet_address,
                WhaleTrade.side == "BUY",
                Market.category == category,
            )
        )
        .order_by(WhaleTrade.timestamp.asc())
    )
    trades_with_markets = result.all()

    if not trades_with_markets:
        return FollowabilityResult()

    max_slippage = settings.max_slippage_pct
    window_counts = {w: {"followable": 0, "total": 0} for w in WINDOWS}

    for whale_trade, market in trades_with_markets:
        # Find the next trade in the same market by ANY wallet (price proxy)
        for window_name, window_minutes in WINDOWS.items():
            window_start = whale_trade.timestamp
            window_end = whale_trade.timestamp + timedelta(minutes=window_minutes)

            # Look for the first trade after our trade but within the window
            next_result = await session.execute(
                select(WhaleTrade.price)
                .where(
                    and_(
                        WhaleTrade.condition_id == whale_trade.condition_id,
                        WhaleTrade.token_id == whale_trade.token_id,
                        WhaleTrade.timestamp > window_start,
                        WhaleTrade.timestamp <= window_end,
                        WhaleTrade.id != whale_trade.id,
                    )
                )
                .order_by(WhaleTrade.timestamp.asc())
                .limit(1)
            )
            next_price_row = next_result.scalar_one_or_none()

            if next_price_row is not None:
                next_price = float(next_price_row)
                entry_price = float(whale_trade.price)

                if entry_price > 0:
                    slippage = abs(next_price - entry_price) / entry_price
                    window_counts[window_name]["total"] += 1
                    if slippage <= max_slippage:
                        window_counts[window_name]["followable"] += 1
            # If no next trade found in window, skip (don't count)

    results = {}
    for window_name, counts in window_counts.items():
        if counts["total"] > 0:
            results[window_name] = round(
                counts["followable"] / counts["total"], 4
            )
        else:
            results[window_name] = 0.0

    sample_size = max(c["total"] for c in window_counts.values()) if window_counts else 0

    # Warn if sparse
    if sample_size > 0 and sample_size < 10:
        logger.warning(
            "Low followability sample for %s in %s: only %d data points",
            wallet_address[:10], category, sample_size,
        )

    # Determine primary followability based on category config
    primary_window = settings.get_followability_window(category)
    primary = results.get(primary_window, results.get("24h", 0.0))

    return FollowabilityResult(
        followability_5m=results.get("5m", 0.0),
        followability_30m=results.get("30m", 0.0),
        followability_2h=results.get("2h", 0.0),
        followability_24h=results.get("24h", 0.0),
        primary_followability=primary,
        provisional=True,  # Phase 1 always provisional
        sample_size=sample_size,
    )
