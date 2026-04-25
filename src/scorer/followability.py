"""Followability scoring: can we follow this whale's trades in time?"""

import bisect
import logging
from collections import defaultdict
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

    window_counts = {w: {"followable": 0, "total": 0} for w in WINDOWS}

    # Prefetch all trades for relevant markets to avoid N+1 queries
    condition_ids = {wt.condition_id for wt, _ in trades_with_markets}
    all_trades_result = await session.execute(
        select(WhaleTrade.condition_id, WhaleTrade.token_id, WhaleTrade.timestamp, WhaleTrade.price, WhaleTrade.id)
        .where(WhaleTrade.condition_id.in_(condition_ids))
        .order_by(WhaleTrade.timestamp.asc())
    )
    all_trades_rows = all_trades_result.all()

    # Index by (condition_id, token_id) → sorted list of (timestamp, price, id)
    trades_by_market: dict[tuple[str, str], list[tuple]] = defaultdict(list)
    for cid, tid, ts, price, trade_id in all_trades_rows:
        trades_by_market[(cid, tid)].append((ts, price, trade_id))

    for whale_trade, market in trades_with_markets:
        entry_price = float(whale_trade.price)
        if entry_price <= 0:
            continue

        market_trades = trades_by_market.get((whale_trade.condition_id, whale_trade.token_id), [])
        # Find insertion point for this trade's timestamp
        timestamps = [t[0] for t in market_trades]
        idx = bisect.bisect_right(timestamps, whale_trade.timestamp)

        for window_name, window_minutes in WINDOWS.items():
            window_end = whale_trade.timestamp + timedelta(minutes=window_minutes)

            # Find first trade after ours within the window
            next_price_row = None
            for j in range(idx, len(market_trades)):
                ts_j, price_j, id_j = market_trades[j]
                if id_j == whale_trade.id:
                    continue
                if ts_j > window_end:
                    break
                next_price_row = price_j
                break

            if next_price_row is not None:
                next_price = float(next_price_row)
                slippage = abs(next_price - entry_price)
                window_counts[window_name]["total"] += 1
                if slippage <= settings.max_absolute_slippage:
                    window_counts[window_name]["followable"] += 1
            else:
                # No next trade in window → no price reference, skip
                pass

    results = {}
    sample_size = max(c["total"] for c in window_counts.values()) if window_counts else 0

    # Minimum sample threshold: insufficient data → assume followable (provisional default)
    if sample_size < 5:
        if sample_size > 0:
            logger.warning(
                "Insufficient followability sample for %s in %s: only %d trades (need 5)",
                wallet_address[:10], category, sample_size,
            )
        default = 0.70  # same as discovery provisional default
        return FollowabilityResult(
            followability_5m=default, followability_30m=default,
            followability_2h=default, followability_24h=default,
            primary_followability=default,
            provisional=True, sample_size=sample_size,
        )

    for window_name, counts in window_counts.items():
        if counts["total"] > 0:
            results[window_name] = round(
                counts["followable"] / counts["total"], 4
            )
        else:
            results[window_name] = 0.0

    # Warn if sparse
    if sample_size < 10:
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
        provisional=(sample_size < 10),
        sample_size=sample_size,
    )
