"""Daily portfolio snapshots."""

import logging
from datetime import datetime, timezone, date, timedelta
from decimal import Decimal

from sqlalchemy import select, func, and_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.db import async_session
from src.models import MyTrade, PortfolioSnapshot
from src.tracking.pnl import get_portfolio_pnl

logger = logging.getLogger(__name__)


async def take_daily_snapshot() -> None:
    """Record a daily portfolio snapshot."""
    today = date.today()
    pnl_data = await get_portfolio_pnl()

    # Use cached wallet balance for total_capital (falls back to config-based estimate)
    from src.events import get_deployable_capital
    deployable = await get_deployable_capital()
    total_capital = deployable + pnl_data["open_exposure"]

    async with async_session() as session:
        stmt = insert(PortfolioSnapshot).values(
            date=today,
            total_capital=Decimal(str(round(total_capital, 2))),
            open_positions=pnl_data["open_positions"],
            total_exposure=Decimal(str(pnl_data["open_exposure"])),
            daily_pnl=Decimal(str(await _compute_daily_pnl(session))),
            cumulative_pnl=Decimal(str(pnl_data["total_pnl"])),
            win_count_30d=pnl_data["wins"],
            loss_count_30d=pnl_data["losses"],
            profit_factor_30d=Decimal(str(pnl_data["profit_factor"])),
            expectancy_30d=Decimal(str(pnl_data["expectancy"])),
        ).on_conflict_do_update(
            index_elements=["date"],
            set_={
                "total_capital": Decimal(str(round(total_capital, 2))),
                "open_positions": pnl_data["open_positions"],
                "total_exposure": Decimal(str(pnl_data["open_exposure"])),
                "cumulative_pnl": Decimal(str(pnl_data["total_pnl"])),
            },
        )
        await session.execute(stmt)
        await session.commit()

    # Rotate old API response files (keep last 24h)
    _rotate_api_responses()

    logger.info("Portfolio snapshot taken for %s", today)


def _rotate_api_responses(keep_hours: int = 24) -> None:
    """Delete API response files older than keep_hours."""
    from pathlib import Path
    api_dir = Path("data/api_responses")
    if not api_dir.exists():
        return
    cutoff = datetime.now(timezone.utc) - timedelta(hours=keep_hours)
    deleted = 0
    for f in api_dir.glob("*.json"):
        try:
            mtime = datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc)
            if mtime < cutoff:
                f.unlink()
                deleted += 1
        except Exception as e:
            logger.debug("Failed to delete stale API response %s: %s", f.name, e)
    if deleted > 0:
        logger.info("Rotated %d old API response files", deleted)


async def _compute_daily_pnl(session: AsyncSession) -> float:
    now = datetime.now(timezone.utc)
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)

    result = await session.execute(
        select(func.sum(MyTrade.pnl_usdc)).where(
            and_(
                MyTrade.resolved == True,
                MyTrade.exit_timestamp >= start_of_day,
            )
        )
    )
    return float(result.scalar() or 0)
