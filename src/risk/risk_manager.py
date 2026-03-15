"""Pre-trade risk checks and exposure limits."""

import logging
from decimal import Decimal

from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.db import async_session
from src.models import MyTrade, PortfolioSnapshot

logger = logging.getLogger(__name__)


async def check_risk(
    category: str,
    size_usdc: float,
    current_capital: float | None = None,
) -> tuple[bool, str | None]:
    """Run pre-trade risk checks. Returns (allowed, rejection_reason)."""

    capital = current_capital or settings.starting_capital

    async with async_session() as session:
        # Check max position size
        if size_usdc > capital * settings.max_position_pct:
            return False, f"position_too_large:{size_usdc:.2f}>{capital * settings.max_position_pct:.2f}"

        # Check total exposure
        result = await session.execute(
            select(func.sum(MyTrade.size_usdc)).where(
                and_(
                    MyTrade.resolved == False,
                    MyTrade.fill_status.in_(["PAPER", "FILLED", "PARTIAL"]),
                )
            )
        )
        total_exposure = float(result.scalar() or 0)

        if (total_exposure + size_usdc) > capital * settings.max_total_exposure_pct:
            return False, f"total_exposure_exceeded:{total_exposure + size_usdc:.2f}>{capital * settings.max_total_exposure_pct:.2f}"

        # Check same-category position limit
        result = await session.execute(
            select(func.count(MyTrade.id)).where(
                and_(
                    MyTrade.resolved == False,
                    MyTrade.fill_status.in_(["PAPER", "FILLED", "PARTIAL"]),
                )
            )
        )
        # Would need to join with signals for category — simplified check
        open_count = result.scalar() or 0
        if open_count >= settings.max_same_category_positions * 3:  # rough limit
            return False, f"too_many_open_positions:{open_count}"

        # Check drawdown halts
        halt_reason = await _check_drawdown_halt(session, capital)
        if halt_reason:
            return False, halt_reason

    return True, None


async def _check_drawdown_halt(session: AsyncSession, capital: float) -> str | None:
    """Check if trading should be halted due to drawdown."""
    from datetime import datetime, timezone, timedelta

    now = datetime.now(timezone.utc)

    # Daily loss check
    result = await session.execute(
        select(func.sum(MyTrade.pnl_usdc)).where(
            and_(
                MyTrade.resolved == True,
                MyTrade.exit_timestamp >= now - timedelta(days=1),
            )
        )
    )
    daily_pnl = float(result.scalar() or 0)
    if daily_pnl < 0 and abs(daily_pnl) > capital * settings.daily_loss_halt_pct:
        return f"daily_loss_halt:{daily_pnl:.2f}"

    # Weekly drawdown check
    result = await session.execute(
        select(func.sum(MyTrade.pnl_usdc)).where(
            and_(
                MyTrade.resolved == True,
                MyTrade.exit_timestamp >= now - timedelta(days=7),
            )
        )
    )
    weekly_pnl = float(result.scalar() or 0)
    if weekly_pnl < 0 and abs(weekly_pnl) > capital * settings.weekly_drawdown_halt_pct:
        return f"weekly_drawdown_halt:{weekly_pnl:.2f}"

    return None
