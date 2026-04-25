"""P&L attribution to source wallets.

Weighted attribution: the wallet that entered first (initiator) gets more
credit than wallets that converged later, because the early signal is the
highest-alpha contribution.

Weights: first wallet gets 50%, rest split remaining 50% evenly.
For single-wallet trades, that wallet gets 100%.
"""

import logging
from collections import defaultdict

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from src.db import async_session
from src.models import MyTrade, Signal

logger = logging.getLogger(__name__)


async def compute_attribution() -> dict[str, dict]:
    """Compute weighted P&L attribution per source wallet."""
    async with async_session() as session:
        result = await session.execute(
            select(MyTrade, Signal)
            .join(Signal, MyTrade.signal_id == Signal.id, isouter=True)
            .where(MyTrade.resolved == True)
        )
        rows = result.all()

    attribution: dict[str, dict] = defaultdict(
        lambda: {"total_pnl": 0.0, "wins": 0, "losses": 0, "trade_count": 0}
    )

    for trade, signal in rows:
        pnl = float(trade.pnl_usdc or 0)
        wallets = trade.source_wallets or []
        if not wallets:
            continue

        # Build weight map: first wallet (initiator) gets 50%, rest split 50%
        if len(wallets) == 1:
            weights = {wallets[0]: 1.0}
        else:
            initiator = wallets[0]
            follower_share = 0.5 / (len(wallets) - 1)
            weights = {addr: follower_share for addr in wallets[1:]}
            weights[initiator] = 0.5

        for addr, weight in weights.items():
            attribution[addr]["total_pnl"] += pnl * weight
            attribution[addr]["trade_count"] += 1
            if pnl > 0:
                attribution[addr]["wins"] += 1
            elif pnl < 0:
                attribution[addr]["losses"] += 1

    # Round and compute per-wallet metrics
    for addr in attribution:
        a = attribution[addr]
        a["total_pnl"] = round(a["total_pnl"], 2)
        total = a["wins"] + a["losses"]
        a["win_rate"] = round(a["wins"] / total, 4) if total > 0 else 0

    return dict(attribution)
