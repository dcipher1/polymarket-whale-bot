"""Pre-trade risk checks and exposure limits."""

import logging
from decimal import Decimal

from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.db import async_session
from src.models import MyTrade, PortfolioSnapshot, Wallet

logger = logging.getLogger(__name__)


async def check_risk(
    category: str,
    size_usdc: float,
    current_capital: float | None = None,
    source_wallets: list[str] | None = None,
) -> tuple[bool, str | None]:
    """Run pre-trade risk checks. Returns (allowed, rejection_reason)."""

    if current_capital is not None:
        capital = current_capital
    else:
        from src.events import get_deployable_capital
        capital = await get_deployable_capital()

    async with async_session() as session:
        # Check max position size
        if size_usdc > capital * settings.max_position_pct:
            return False, f"position_too_large:{size_usdc:.2f}>{capital * settings.max_position_pct:.2f}"

        # Check if this trade fits within deployable capital
        # (wallet balance already reflects money spent on existing positions)
        if size_usdc > capital:
            return False, f"insufficient_capital:{size_usdc:.2f}>{capital:.2f}"

        # Check per-whale position limit (skip for whitelisted whales)
        if source_wallets:
            for whale_addr in source_wallets:
                wallet = await session.get(Wallet, whale_addr)
                if wallet and (wallet.meta or {}).get("whitelisted"):
                    continue
                result = await session.execute(
                    select(func.count(MyTrade.id)).where(
                        and_(
                            MyTrade.resolved == False,
                            MyTrade.fill_status.in_(["PAPER", "FILLED", "PARTIAL"]),
                            MyTrade.source_wallets.any(whale_addr),
                        )
                    )
                )
                whale_count = result.scalar() or 0
                if whale_count >= settings.max_per_whale_positions:
                    return False, f"whale_limit:{whale_addr[:10]}={whale_count}"

        # Check drawdown halts
        halt_reason = await _check_drawdown_halt(session, capital)
        if halt_reason:
            return False, halt_reason

    return True, None


async def _check_drawdown_halt(session: AsyncSession, capital: float) -> str | None:
    """Check if trading should be halted due to drawdown. Currently disabled."""
    return None
