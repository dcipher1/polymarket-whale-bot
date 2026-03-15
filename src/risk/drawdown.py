"""Track daily/weekly/monthly drawdown."""

import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from src.db import async_session
from src.models import MyTrade, PortfolioSnapshot

logger = logging.getLogger(__name__)


async def compute_drawdown() -> dict:
    """Compute current drawdown metrics."""
    now = datetime.now(timezone.utc)

    async with async_session() as session:
        metrics = {}

        for period_name, days in [("daily", 1), ("weekly", 7), ("monthly", 30)]:
            cutoff = now - timedelta(days=days)
            result = await session.execute(
                select(func.sum(MyTrade.pnl_usdc)).where(
                    and_(
                        MyTrade.resolved == True,
                        MyTrade.exit_timestamp >= cutoff,
                    )
                )
            )
            pnl = float(result.scalar() or 0)
            metrics[f"{period_name}_pnl"] = round(pnl, 2)

        # Max drawdown from portfolio snapshots
        result = await session.execute(
            select(PortfolioSnapshot)
            .where(PortfolioSnapshot.date >= (now - timedelta(days=30)).date())
            .order_by(PortfolioSnapshot.date)
        )
        snapshots = result.scalars().all()

        if snapshots:
            peak = float(snapshots[0].cumulative_pnl or 0)
            max_dd = 0.0
            for snap in snapshots:
                cum = float(snap.cumulative_pnl or 0)
                peak = max(peak, cum)
                dd = peak - cum
                max_dd = max(max_dd, dd)
            metrics["max_drawdown_30d"] = round(max_dd, 2)
        else:
            metrics["max_drawdown_30d"] = 0.0

        return metrics
