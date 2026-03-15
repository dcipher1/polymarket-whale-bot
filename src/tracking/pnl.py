"""P&L computation for paper and live trades."""

import logging
from decimal import Decimal

from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from src.db import async_session
from src.models import MyTrade

logger = logging.getLogger(__name__)


async def get_portfolio_pnl() -> dict:
    """Get aggregate portfolio P&L."""
    async with async_session() as session:
        # Resolved trades
        result = await session.execute(
            select(
                func.sum(MyTrade.pnl_usdc),
                func.count(MyTrade.id),
            ).where(MyTrade.resolved == True)
        )
        row = result.one()
        total_pnl = float(row[0] or 0)
        total_resolved = row[1] or 0

        # Wins and losses
        result = await session.execute(
            select(
                func.count(MyTrade.id),
                func.sum(MyTrade.pnl_usdc),
            ).where(
                and_(MyTrade.resolved == True, MyTrade.trade_outcome == "WIN")
            )
        )
        win_row = result.one()
        wins = win_row[0] or 0
        win_pnl = float(win_row[1] or 0)

        result = await session.execute(
            select(
                func.count(MyTrade.id),
                func.sum(MyTrade.pnl_usdc),
            ).where(
                and_(MyTrade.resolved == True, MyTrade.trade_outcome == "LOSS")
            )
        )
        loss_row = result.one()
        losses = loss_row[0] or 0
        loss_pnl = abs(float(loss_row[1] or 0))

        # Open positions
        result = await session.execute(
            select(func.count(MyTrade.id), func.sum(MyTrade.size_usdc)).where(
                MyTrade.resolved == False
            )
        )
        open_row = result.one()
        open_count = open_row[0] or 0
        open_exposure = float(open_row[1] or 0)

        win_rate = wins / total_resolved if total_resolved > 0 else 0
        profit_factor = win_pnl / loss_pnl if loss_pnl > 0 else float("inf") if win_pnl > 0 else 0
        expectancy = total_pnl / total_resolved if total_resolved > 0 else 0

        return {
            "total_pnl": round(total_pnl, 2),
            "total_resolved": total_resolved,
            "wins": wins,
            "losses": losses,
            "win_rate": round(win_rate, 4),
            "profit_factor": round(min(profit_factor, 9999), 2),
            "expectancy": round(expectancy, 2),
            "open_positions": open_count,
            "open_exposure": round(open_exposure, 2),
            "total_fees": 0.0,  # tracked per-trade
        }
