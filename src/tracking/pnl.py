"""P&L computation for paper and live trades."""

import logging
from decimal import Decimal

from sqlalchemy import select, func, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from src.db import async_session
from src.models import MyTrade

logger = logging.getLogger(__name__)

ECONOMIC_FILL_STATUSES = ["FILLED", "PARTIAL", "PAPER"]


async def get_portfolio_pnl() -> dict:
    """Get aggregate portfolio P&L."""
    async with async_session() as session:
        # Resolved trades
        result = await session.execute(
            select(
                func.sum(MyTrade.pnl_usdc),
                func.count(MyTrade.id),
            ).where(
                and_(
                    MyTrade.resolved == True,
                    MyTrade.fill_status.in_(ECONOMIC_FILL_STATUSES),
                )
            )
        )
        row = result.one()
        total_pnl = float(row[0] or 0)
        total_resolved = row[1] or 0

        # Wins and losses (include WHALE_EXIT trades based on PnL sign)
        result = await session.execute(
            select(
                func.count(MyTrade.id),
                func.sum(MyTrade.pnl_usdc),
            ).where(
                and_(
                    MyTrade.resolved == True,
                    MyTrade.fill_status.in_(ECONOMIC_FILL_STATUSES),
                    or_(
                        MyTrade.trade_outcome == "WIN",
                        and_(MyTrade.trade_outcome == "WHALE_EXIT", MyTrade.pnl_usdc > 0),
                    ),
                )
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
                and_(
                    MyTrade.resolved == True,
                    MyTrade.fill_status.in_(ECONOMIC_FILL_STATUSES),
                    or_(
                        MyTrade.trade_outcome == "LOSS",
                        and_(MyTrade.trade_outcome == "WHALE_EXIT", MyTrade.pnl_usdc <= 0),
                    ),
                )
            )
        )
        loss_row = result.one()
        losses = loss_row[0] or 0
        loss_pnl = abs(float(loss_row[1] or 0))

        # Open positions (only FILLED/PARTIAL — exclude FAILED/CANCELLED)
        result = await session.execute(
            select(func.count(MyTrade.id), func.sum(MyTrade.size_usdc)).where(
                and_(
                    MyTrade.resolved == False,
                    MyTrade.fill_status.in_(ECONOMIC_FILL_STATUSES),
                )
            )
        )
        open_row = result.one()
        open_count = open_row[0] or 0
        open_exposure = float(open_row[1] or 0)

        # Total fees
        result = await session.execute(
            select(func.sum(MyTrade.fees_paid)).where(MyTrade.resolved == True)
            .where(MyTrade.fill_status.in_(ECONOMIC_FILL_STATUSES))
        )
        total_fees = float(result.scalar() or 0)

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
            "total_fees": round(total_fees, 4),
        }
