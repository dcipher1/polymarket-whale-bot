"""Monitor market resolution and update outcomes."""

import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from sqlalchemy import select, and_, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.db import async_session
from src.models import Market, MyTrade, WhalePosition
from src.polymarket.gamma_api import GammaAPIClient

logger = logging.getLogger(__name__)


async def check_resolutions() -> int:
    """Check for newly resolved markets and update P&L. Returns count."""
    gamma = GammaAPIClient()
    resolved_count = 0

    try:
        async with async_session() as session:
            # Get unresolved markets approaching resolution
            now = datetime.now(timezone.utc)
            result = await session.execute(
                select(Market).where(
                    and_(
                        Market.resolved == False,
                        Market.resolution_time.isnot(None),
                        Market.resolution_time <= now + timedelta(hours=1),
                    )
                )
            )
            markets = result.scalars().all()

            for market in markets:
                try:
                    updated = await _check_market_resolution(session, gamma, market)
                    if updated:
                        resolved_count += 1
                except Exception as e:
                    logger.debug("Resolution check skipped for %s: %s", market.condition_id[:10], e)

            await session.commit()
    finally:
        await gamma.close()

    if resolved_count > 0:
        logger.info("Resolved %d markets", resolved_count)
    return resolved_count


async def _check_market_resolution(
    session: AsyncSession,
    gamma: GammaAPIClient,
    market: Market,
) -> bool:
    """Check if a specific market has resolved. Returns True if newly resolved."""
    gm = await gamma.get_market(market.condition_id)
    if not gm or not gm.closed:
        return False

    # Determine winning outcome
    winning_outcome = None
    for token in gm.tokens:
        if token.get("winner"):
            winning_outcome = token.get("outcome", "").upper()
            break

    if not winning_outcome:
        return False

    # Update market
    market.resolved = True
    market.outcome = winning_outcome

    # Update our paper trades
    result = await session.execute(
        select(MyTrade).where(
            and_(
                MyTrade.condition_id == market.condition_id,
                MyTrade.resolved == False,
            )
        )
    )
    trades = result.scalars().all()

    for trade in trades:
        entry_price = float(trade.entry_price or 0)
        contracts = trade.num_contracts or 0

        if trade.outcome.upper() == winning_outcome:
            pnl = (1.0 - entry_price) * contracts
            trade.trade_outcome = "WIN"
        else:
            pnl = -entry_price * contracts
            trade.trade_outcome = "LOSS"

        trade.pnl_usdc = Decimal(str(round(pnl, 2)))
        trade.exit_price = Decimal("1.0") if trade.outcome.upper() == winning_outcome else Decimal("0.0")
        trade.exit_timestamp = datetime.now(timezone.utc)
        trade.resolved = True

        logger.info(
            "Trade %d resolved: %s P&L=$%.2f",
            trade.id, trade.trade_outcome, pnl,
        )

        try:
            from src.monitoring.telegram import send_alert
            icon = "WIN" if trade.trade_outcome == "WIN" else "LOSS"
            await send_alert(
                f"{icon}: {market.question[:50]} -> ${pnl:+.2f}"
            )
        except Exception:
            pass

    # Update whale positions
    await session.execute(
        update(WhalePosition)
        .where(
            and_(
                WhalePosition.condition_id == market.condition_id,
                WhalePosition.is_open == True,
            )
        )
        .values(is_open=False, last_event_type="CLOSE", last_updated=datetime.now(timezone.utc))
    )

    return True
