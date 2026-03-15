"""Build and maintain whale positions from trade history."""

import logging
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select, and_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.db import async_session
from src.models import WhaleTrade, WhalePosition

logger = logging.getLogger(__name__)

# Threshold for ADD event: new buy must be >10% of current position
ADD_THRESHOLD_PCT = Decimal("0.10")


async def build_positions_for_wallet(wallet_address: str) -> int:
    """Rebuild all positions for a wallet from trade history. Returns position count."""
    async with async_session() as session:
        # Get all trades ordered by time
        result = await session.execute(
            select(WhaleTrade)
            .where(WhaleTrade.wallet_address == wallet_address)
            .order_by(WhaleTrade.timestamp.asc())
        )
        trades = result.scalars().all()

        if not trades:
            return 0

        # Group by (condition_id, outcome)
        position_groups: dict[tuple[str, str], list[WhaleTrade]] = {}
        for trade in trades:
            key = (trade.condition_id, trade.outcome)
            position_groups.setdefault(key, []).append(trade)

        count = 0
        for (condition_id, outcome), group_trades in position_groups.items():
            await _build_position(session, wallet_address, condition_id, outcome, group_trades)
            count += 1

        await session.commit()
        logger.info("Built %d positions for wallet %s", count, wallet_address[:10])
        return count


async def _build_position(
    session: AsyncSession,
    wallet_address: str,
    condition_id: str,
    outcome: str,
    trades: list[WhaleTrade],
) -> str | None:
    """Build a single position from trades. Returns the last event type."""
    total_contracts = Decimal("0")
    total_cost = Decimal("0")
    first_entry = None
    last_event_type = None

    for trade in trades:
        contracts = trade.num_contracts or Decimal("0")

        if trade.side == "BUY":
            if total_contracts == 0:
                last_event_type = "OPEN"
                first_entry = trade.timestamp
            elif contracts > total_contracts * ADD_THRESHOLD_PCT:
                last_event_type = "ADD"

            total_cost += contracts * trade.price
            total_contracts += contracts

        elif trade.side == "SELL":
            total_contracts -= contracts
            if total_contracts <= 0:
                total_contracts = Decimal("0")
                total_cost = Decimal("0")
                last_event_type = "CLOSE"
            else:
                last_event_type = "REDUCE"

    is_open = total_contracts > 0
    avg_entry = (total_cost / total_contracts) if total_contracts > 0 else Decimal("0")
    total_size_usdc = total_contracts * avg_entry

    stmt = insert(WhalePosition).values(
        wallet_address=wallet_address,
        condition_id=condition_id,
        outcome=outcome,
        avg_entry_price=avg_entry,
        total_size_usdc=total_size_usdc,
        num_contracts=total_contracts,
        first_entry=first_entry,
        last_updated=datetime.now(timezone.utc),
        is_open=is_open,
        last_event_type=last_event_type,
    ).on_conflict_do_update(
        constraint="whale_positions_pkey",
        set_={
            "avg_entry_price": avg_entry,
            "total_size_usdc": total_size_usdc,
            "num_contracts": total_contracts,
            "last_updated": datetime.now(timezone.utc),
            "is_open": is_open,
            "last_event_type": last_event_type,
        },
    )
    await session.execute(stmt)
    return last_event_type


def classify_trade_event(
    trade_side: str,
    existing_contracts: Decimal,
    new_contracts: Decimal,
) -> str:
    """Classify a trade into a position event type."""
    if trade_side == "BUY":
        if existing_contracts == 0:
            return "OPEN"
        if new_contracts > existing_contracts * ADD_THRESHOLD_PCT:
            return "ADD"
        return "ADD"  # small addition still counts as ADD
    else:  # SELL
        remaining = existing_contracts - new_contracts
        if remaining <= 0:
            return "CLOSE"
        return "REDUCE"
