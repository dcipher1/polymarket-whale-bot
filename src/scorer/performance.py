"""Performance metrics: win rate, profit factor, gain/loss ratio, expectancy."""

import logging
from dataclasses import dataclass
from decimal import Decimal

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from src.models import WhaleTrade, WhalePosition, Market

logger = logging.getLogger(__name__)


@dataclass
class PerformanceMetrics:
    win_rate: float = 0.0
    profit_factor: float = 0.0
    gain_loss_ratio: float = 0.0
    expectancy: float = 0.0
    trade_count: int = 0
    total_wins_usdc: float = 0.0
    total_losses_usdc: float = 0.0
    avg_hold_hours: float = 0.0


async def compute_performance(
    session: AsyncSession,
    wallet_address: str,
    category: str | None = None,
) -> PerformanceMetrics:
    """Compute performance metrics for a wallet, optionally filtered by category."""

    # Get all closed positions for this wallet
    query = (
        select(WhalePosition, Market)
        .join(Market, WhalePosition.condition_id == Market.condition_id)
        .where(
            and_(
                WhalePosition.wallet_address == wallet_address,
                WhalePosition.is_open == False,
            )
        )
    )
    if category:
        query = query.where(Market.category == category)

    result = await session.execute(query)
    rows = result.all()

    if not rows:
        return PerformanceMetrics()

    wins = []
    losses = []
    hold_hours = []

    for position, market in rows:
        if not market.resolved:
            continue  # only count settled trades

        # Determine P&L
        pnl = _compute_position_pnl(position, market)

        if pnl > 0:
            wins.append(pnl)
        elif pnl < 0:
            losses.append(abs(pnl))
        # pnl == 0 treated as neither

        # Hold duration
        if position.first_entry and position.last_updated:
            delta = position.last_updated - position.first_entry
            hold_hours.append(delta.total_seconds() / 3600)

    total_trades = len(wins) + len(losses)
    if total_trades == 0:
        return PerformanceMetrics()

    total_wins = sum(wins)
    total_losses = sum(losses)

    win_rate = len(wins) / total_trades if total_trades > 0 else 0
    profit_factor = total_wins / total_losses if total_losses > 0 else float("inf") if total_wins > 0 else 0
    avg_win = total_wins / len(wins) if wins else 0
    avg_loss = total_losses / len(losses) if losses else 0
    gain_loss_ratio = avg_win / avg_loss if avg_loss > 0 else float("inf") if avg_win > 0 else 0
    expectancy = (total_wins - total_losses) / total_trades if total_trades > 0 else 0

    avg_hold = sum(hold_hours) / len(hold_hours) if hold_hours else 0

    return PerformanceMetrics(
        win_rate=round(win_rate, 4),
        profit_factor=round(min(profit_factor, 9999), 2),
        gain_loss_ratio=round(min(gain_loss_ratio, 9999), 2),
        expectancy=round(expectancy, 4),
        trade_count=total_trades,
        total_wins_usdc=round(total_wins, 2),
        total_losses_usdc=round(total_losses, 2),
        avg_hold_hours=round(avg_hold, 2),
    )


def _compute_position_pnl(position: WhalePosition, market: Market) -> float:
    """Compute P&L for a closed position on a resolved market."""
    if not market.resolved or not market.outcome:
        return 0.0

    avg_entry = float(position.avg_entry_price or 0)
    contracts = float(position.num_contracts or 0)

    if contracts == 0:
        return 0.0

    # Binary market: payout is $1 per contract if correct, $0 if wrong
    position_outcome = position.outcome.upper()
    market_outcome = market.outcome.upper()

    if position_outcome == market_outcome:
        # Winner: received $1 per contract
        pnl = (1.0 - avg_entry) * contracts
    else:
        # Loser: received $0 per contract
        pnl = -avg_entry * contracts

    return round(pnl, 2)
