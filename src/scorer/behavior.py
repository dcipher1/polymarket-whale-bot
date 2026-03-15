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

    # Hold duration — from positions
    result = await session.execute(
        select(WhalePosition).where(
            and_(
                WhalePosition.wallet_address == wallet_address,
                WhalePosition.first_entry.isnot(None),
                WhalePosition.last_updated.isnot(None),
            )
        )
    )
    positions = result.scalars().all()

    hold_hours_list = []
    held_to_resolution = 0
    total_resolved = 0

    for pos in positions:
        if pos.first_entry and pos.last_updated:
            hours = (pos.last_updated - pos.first_entry).total_seconds() / 3600
            hold_hours_list.append(hours)

        # Check if held to resolution
        if pos.last_event_type in ("OPEN", "ADD") and not pos.is_open:
            total_resolved += 1
            # If close event wasn't a SELL (i.e., market resolved while holding)
            if pos.last_event_type != "CLOSE":
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

    # Conviction score components
    freq_score = _frequency_score(trades_per_month)
    hold_score = _hold_duration_score(median_hold)
    resolution_score = _resolution_holding_score(pct_resolution)

    # Concentration score
    if estimated_bankroll > 0 and positions:
        avg_position = sum(
            float(p.total_size_usdc or 0) for p in positions
        ) / len(positions)
        ratio = avg_position / estimated_bankroll
        conc_score = _concentration_score(ratio)
    else:
        # Skip concentration, reweight others
        conc_score = None

    if conc_score is not None:
        conviction = (
            freq_score * 0.25
            + hold_score * 0.25
            + resolution_score * 0.30
            + conc_score * 0.20
        )
    else:
        # Reweight without concentration
        conviction = (
            freq_score * 0.3125
            + hold_score * 0.3125
            + resolution_score * 0.375
        )

    conviction_score = int(max(0, min(100, round(conviction))))

    return BehaviorMetrics(
        trades_per_month=round(trades_per_month, 2),
        median_hold_hours=round(median_hold, 2),
        pct_held_to_resolution=round(pct_resolution, 4),
        estimated_bankroll=round(estimated_bankroll, 2),
        conviction_score=conviction_score,
    )


def _frequency_score(trades_per_month: float) -> float:
    if trades_per_month <= 20:
        return 100
    elif trades_per_month <= 50:
        return 100 - (trades_per_month - 20) / 30 * 30  # 100 → 70
    elif trades_per_month <= 100:
        return 70 - (trades_per_month - 50) / 50 * 40  # 70 → 30
    else:
        return 0


def _hold_duration_score(median_hold_hours: float) -> float:
    if median_hold_hours >= 168:
        return 100
    elif median_hold_hours >= 48:
        return 40 + (median_hold_hours - 48) / (168 - 48) * 60  # 40 → 100
    elif median_hold_hours >= 24:
        return 40
    else:
        return 0


def _resolution_holding_score(pct: float) -> float:
    if pct >= 0.80:
        return 100
    elif pct >= 0.50:
        return 20 + (pct - 0.50) / 0.30 * 80  # 20 → 100
    else:
        return 20


def _concentration_score(ratio: float) -> float:
    if ratio >= 0.05:
        return 100
    elif ratio >= 0.02:
        return (ratio - 0.02) / 0.03 * 100  # 0 → 100
    elif ratio >= 0.01:
        return 20
    else:
        return 20
