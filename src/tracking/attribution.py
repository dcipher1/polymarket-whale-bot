"""P&L attribution to source wallets."""

import logging
from collections import defaultdict

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from src.db import async_session
from src.models import MyTrade

logger = logging.getLogger(__name__)


async def compute_attribution() -> dict[str, dict]:
    """Compute P&L attribution per source wallet."""
    async with async_session() as session:
        result = await session.execute(
            select(MyTrade).where(MyTrade.resolved == True)
        )
        trades = result.scalars().all()

    attribution: dict[str, dict] = defaultdict(
        lambda: {"total_pnl": 0.0, "wins": 0, "losses": 0, "trade_count": 0}
    )

    for trade in trades:
        pnl = float(trade.pnl_usdc or 0)
        wallets = trade.source_wallets or []
        # Split attribution evenly among source wallets
        per_wallet_pnl = pnl / len(wallets) if wallets else 0

        for addr in wallets:
            attribution[addr]["total_pnl"] += per_wallet_pnl
            attribution[addr]["trade_count"] += 1
            if pnl > 0:
                attribution[addr]["wins"] += 1
            elif pnl < 0:
                attribution[addr]["losses"] += 1

    # Round values
    for addr in attribution:
        attribution[addr]["total_pnl"] = round(attribution[addr]["total_pnl"], 2)

    return dict(attribution)
