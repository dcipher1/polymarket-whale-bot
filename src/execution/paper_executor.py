"""Paper trading executor — logs what we WOULD trade."""

import logging
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select, and_

from src.config import settings
from src.db import async_session
from src.models import Signal, MyTrade

logger = logging.getLogger(__name__)


async def execute_paper_trade(signal_id: int) -> int | None:
    """Create a paper trade from a LIVE signal. Returns trade ID."""

    async with async_session() as session:
        signal = await session.get(Signal, signal_id)
        if not signal or signal.signal_type != "LIVE":
            return None

        # Idempotency: check if paper trade already exists for this signal
        existing = await session.execute(
            select(MyTrade).where(MyTrade.signal_id == signal_id)
        )
        if existing.scalar_one_or_none():
            logger.warning("Paper trade already exists for signal %d", signal_id)
            return None

        # Idempotency: check if we already have an open trade on this market/outcome
        existing_market = await session.execute(
            select(MyTrade).where(
                and_(
                    MyTrade.condition_id == signal.condition_id,
                    MyTrade.outcome == signal.outcome,
                    MyTrade.resolved == False,
                )
            )
        )
        if existing_market.scalar_one_or_none():
            logger.info("Already have open trade on %s %s, skipping", signal.condition_id[:10], signal.outcome)
            return None

        # Compute position size
        size_usdc = Decimal(str(settings.fixed_position_size_usdc))
        entry_price = signal.market_price_at_signal or signal.whale_avg_price
        if not entry_price or float(entry_price) <= 0:
            logger.error("No valid price for signal %d", signal_id)
            return None

        num_contracts = int(float(size_usdc) / float(entry_price))

        trade = MyTrade(
            signal_id=signal_id,
            condition_id=signal.condition_id,
            outcome=signal.outcome,
            entry_price=entry_price,
            size_usdc=size_usdc,
            num_contracts=num_contracts,
            fill_status="PAPER",
            entry_timestamp=datetime.now(timezone.utc),
            source_wallets=signal.source_wallets,
            attribution={
                "signal_type": signal.signal_type,
                "convergence_count": signal.convergence_count,
                "whale_avg_price": str(signal.whale_avg_price),
                "category": signal.category,
            },
        )
        session.add(trade)

        # Mark signal as executed
        signal.status = "EXECUTED"
        await session.commit()

        logger.info(
            "PAPER TRADE %d: %s %s at $%.4f, %d contracts ($%.2f)",
            trade.id, signal.condition_id[:10], signal.outcome,
            float(entry_price), num_contracts, float(size_usdc),
        )
        return trade.id
