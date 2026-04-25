"""Behavior scoring: conviction score from trading patterns."""

import logging
from dataclasses import dataclass
from decimal import Decimal

import numpy as np
from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from src.models import WhaleTrade, WhalePosition, Market, Wallet

logger = logging.getLogger(__name__)


@dataclass
class BehaviorMetrics:
    trades_per_month: float = 0.0
    median_hold_hours: float = 0.0
    pct_held_to_resolution: float = 0.0
    estimated_bankroll: float = 0.0
    conviction_score: int = 0


async def compute_behavior(
    session: AsyncSession, wallet_address: str
) -> BehaviorMetrics:
    """Compute behavioral metrics and conviction score for a wallet."""

    # Trades per month
    result = await session.execute(
        select(
            func.count(WhaleTrade.id),
            func.min(WhaleTrade.timestamp),
            func.max(WhaleTrade.timestamp),
        ).where(WhaleTrade.wallet_address == wallet_address)
    )
    row = result.one()
    total_trades = row[0] or 0
    first_trade = row[1]
    last_trade = row[2]

    if total_trades == 0 or not first_trade or not last_trade:
        return BehaviorMetrics()

    months_active = max(
        (last_trade - first_trade).total_seconds() / (30 * 24 * 3600), 1
    )
    trades_per_month = total_trades / months_active

    # Hold duration and resolution-holding — from trades
    hold_result = await session.execute(
        select(
            WhaleTrade.condition_id,
            WhaleTrade.outcome,
            func.min(WhaleTrade.timestamp).label("first_trade"),
            func.max(WhaleTrade.timestamp).label("last_trade"),
            func.bool_or(WhaleTrade.side == "SELL").label("has_sell"),
            Market.resolved,
            Market.resolution_time,
        )
        .join(Market, WhaleTrade.condition_id == Market.condition_id)
        .where(WhaleTrade.wallet_address == wallet_address)
        .group_by(WhaleTrade.condition_id, WhaleTrade.outcome, Market.resolved, Market.resolution_time)
    )
    hold_rows = hold_result.all()

    hold_hours_list = []
    held_to_resolution = 0
    total_resolved = 0

    for row in hold_rows:
        if row.first_trade and row.last_trade:
            if not row.has_sell and row.resolved and row.resolution_time:
                # True hold-to-resolution: measure from entry to market close
                hours = (row.resolution_time - row.first_trade).total_seconds() / 3600
            else:
                # Sold before resolution, or unresolved: measure between trades
                hours = (row.last_trade - row.first_trade).total_seconds() / 3600
            hold_hours_list.append(hours)
        if row.resolved:
            total_resolved += 1
            if not row.has_sell:
                held_to_resolution += 1

    median_hold = float(np.median(hold_hours_list)) if hold_hours_list else 0
    pct_resolution = held_to_resolution / total_resolved if total_resolved > 0 else 0

    # Estimated bankroll
    result = await session.execute(
        select(func.sum(WhalePosition.total_size_usdc)).where(
            and_(
                WhalePosition.wallet_address == wallet_address,
                WhalePosition.is_open == True,
            )
        )
    )
    current_deployed = float(result.scalar() or 0)

    wallet = await session.get(Wallet, wallet_address)
    total_pnl = float(wallet.total_pnl_usdc) if wallet else 0
    estimated_bankroll = max(current_deployed + abs(total_pnl), current_deployed * 2)

    # Conviction score: hold duration (30%) + resolution holding (70%)
    hold_score = _hold_duration_score(median_hold)
    resolution_score = _resolution_holding_score(pct_resolution)
    conviction = hold_score * 0.30 + resolution_score * 0.70

    conviction_score = int(max(0, min(100, round(conviction))))

    return BehaviorMetrics(
        trades_per_month=round(trades_per_month, 2),
        median_hold_hours=round(median_hold, 2),
        pct_held_to_resolution=round(pct_resolution, 4),
        estimated_bankroll=round(estimated_bankroll, 2),
        conviction_score=conviction_score,
    )


def _hold_duration_score(median_hold_hours: float) -> float:
    if median_hold_hours >= 72:
        return 100
    elif median_hold_hours >= 24:
        return 50 + (median_hold_hours - 24) / 48 * 50  # 50 → 100
    elif median_hold_hours >= 4:
        return 20 + (median_hold_hours - 4) / 20 * 30  # 20 → 50
    else:
        return 20


def _resolution_holding_score(pct: float) -> float:
    if pct >= 0.80:
        return 100
    elif pct >= 0.50:
        return 40 + (pct - 0.50) / 0.30 * 60  # 40 → 100
    elif pct >= 0.20:
        return 10 + (pct - 0.20) / 0.30 * 30  # 10 → 40
    else:
        return 10
