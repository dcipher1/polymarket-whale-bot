"""Market maker wallet detection.

MMs trade both sides of the same market — their signals are noise.
Flag wallets with 3+ markets where they traded both YES and NO → REJECT.
"""

import logging

from sqlalchemy import select, and_, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.models import Wallet, WhaleTrade

logger = logging.getLogger(__name__)


async def detect_market_makers(session: AsyncSession) -> list[str]:
    """Find wallets that trade both sides of multiple markets (likely MMs).

    Returns list of wallet addresses flagged as market makers.
    """
    threshold = settings.mm_both_sides_threshold

    # Find wallets that have traded both YES and NO on the same market
    # Group by wallet + market, count distinct outcomes
    # If a wallet has both YES and NO on 3+ markets, it's likely an MM
    both_sides_subq = (
        select(
            WhaleTrade.wallet_address,
            WhaleTrade.condition_id,
        )
        .group_by(WhaleTrade.wallet_address, WhaleTrade.condition_id)
        .having(func.count(WhaleTrade.outcome.distinct()) >= 2)
        .subquery()
    )

    # Count how many markets each wallet traded both sides on
    result = await session.execute(
        select(
            both_sides_subq.c.wallet_address,
            func.count().label("both_sides_count"),
        )
        .group_by(both_sides_subq.c.wallet_address)
        .having(func.count() >= threshold)
    )

    mm_wallets = []
    for row in result.all():
        addr = row[0]
        count = row[1]
        mm_wallets.append(addr)
        logger.info(
            "MM detected: %s traded both sides on %d markets (threshold=%d)",
            addr[:10], count, threshold,
        )

    return mm_wallets


async def flag_market_makers(session: AsyncSession) -> int:
    """Detect MMs and set their copyability to REJECT. Returns count flagged."""
    mm_addrs = await detect_market_makers(session)
    if not mm_addrs:
        return 0

    flagged = 0
    for addr in mm_addrs:
        wallet = await session.get(Wallet, addr)
        if wallet and wallet.copyability_class != "REJECT":
            old_class = wallet.copyability_class
            wallet.copyability_class = "REJECT"
            meta = dict(wallet.meta or {})
            meta["mm_flagged"] = True
            meta["mm_previous_class"] = old_class
            wallet.meta = meta
            flagged += 1
            logger.info(
                "Flagged MM wallet %s: %s → REJECT",
                addr[:10], old_class,
            )

    return flagged
