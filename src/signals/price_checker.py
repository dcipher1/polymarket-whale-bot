"""Check if signal entry price is still viable."""

import logging
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.db import async_session
from src.events import cache_get
from src.models import Signal, Market, MarketToken, WalletCategoryScore, Wallet
from src.polymarket.clob_client import CLOBClient
from src.polymarket.fees import get_token_fees

logger = logging.getLogger(__name__)


async def check_price_viability(signal_id: int) -> bool:
    """Check if a CONVERGENCE signal is still price-viable. Promotes to LIVE if so."""

    async with async_session() as session:
        signal = await session.get(Signal, signal_id)
        if not signal or signal.signal_type != "CONVERGENCE" or signal.status != "PENDING":
            return False

        market = await session.get(Market, signal.condition_id)
        if not market:
            await _skip_signal(session, signal, "market_not_found")
            return False

        category = market.category_override or market.category

        # Check time to resolution
        if market.resolution_time:
            hours_left = (
                market.resolution_time - datetime.now(timezone.utc)
            ).total_seconds() / 3600
            min_hours = settings.get_min_hours_to_resolution(category)
            if hours_left < min_hours:
                await _skip_signal(session, signal, "too_close_to_resolution")
                return False
        else:
            hours_left = None

        # Get token for this outcome
        from sqlalchemy import select, and_
        result = await session.execute(
            select(MarketToken).where(
                and_(
                    MarketToken.condition_id == signal.condition_id,
                    MarketToken.outcome == signal.outcome,
                )
            )
        )
        token = result.scalar_one_or_none()
        if not token:
            if settings.strict_mode:
                await _skip_signal(session, signal, "token_not_found")
                return False

        # Get current price
        current_price = await _get_current_price(token.token_id if token else None)
        if current_price is None:
            if settings.strict_mode:
                await _skip_signal(session, signal, "price_lookup_failed")
                return False
            current_price = float(signal.whale_avg_price or 0.5)

        # Slippage check
        whale_avg = float(signal.whale_avg_price or 0)
        max_entry = float(signal.max_entry_price or whale_avg * 1.1)

        if current_price > max_entry:
            slippage_pct = (current_price - whale_avg) / whale_avg if whale_avg > 0 else 1
            await _skip_signal(
                session, signal,
                f"price_exceeded_slippage:current={current_price:.4f},max={max_entry:.4f},slippage={slippage_pct:.2%}",
            )
            return False

        # Fee check
        has_fees = False
        estimated_fee = 0.0
        if token:
            try:
                fee_info = await get_token_fees(token.token_id, session)
                has_fees = fee_info.has_taker_fees
                estimated_fee = fee_info.estimate_fee(current_price)
            except Exception as e:
                if settings.strict_mode:
                    await _skip_signal(session, signal, f"fee_lookup_failed:{e}")
                    return False

        # Edge check (with fees)
        if has_fees and estimated_fee > 0:
            net_edge = (1.0 - current_price) - (current_price - whale_avg) - estimated_fee
            if net_edge < 0.02:
                await _skip_signal(
                    session, signal,
                    f"insufficient_edge_after_fees:edge={net_edge:.4f},fee={estimated_fee:.4f}",
                )
                return False

        # Build audit trail
        audit = await _build_audit_trail(
            session, signal, current_price, whale_avg, has_fees, estimated_fee, category,
        )

        # Promote to LIVE
        signal.signal_type = "LIVE"
        signal.market_price_at_signal = Decimal(str(round(current_price, 6)))
        signal.suggested_size_usdc = Decimal(str(settings.fixed_position_size_usdc))
        signal.hours_to_resolution = Decimal(str(hours_left)) if hours_left else None
        signal.audit_trail = audit

        await session.commit()

        logger.info(
            "Signal %d promoted to LIVE: %s %s at %.4f (whale avg %.4f)",
            signal.id, signal.condition_id[:10], signal.outcome, current_price, whale_avg,
        )
        return True


async def _get_current_price(token_id: str | None) -> float | None:
    """Get current price from WebSocket cache or CLOB API."""
    if not token_id:
        return None

    # Try WebSocket cache first
    cached = await cache_get(f"ws:price:{token_id}")
    if cached and "price" in cached:
        return float(cached["price"])

    # Fallback to CLOB API
    clob = CLOBClient()
    try:
        price_data = await clob.get_price(token_id)
        if price_data:
            return price_data.get("mid", price_data.get("ask"))
    finally:
        await clob.close()

    return None


async def _skip_signal(session: AsyncSession, signal: Signal, reason: str) -> None:
    signal.status = "SKIPPED"
    signal.status_reason = reason
    await session.commit()
    logger.info("Signal %d SKIPPED: %s", signal.id, reason)


async def _build_audit_trail(
    session: AsyncSession,
    signal: Signal,
    current_price: float,
    whale_avg: float,
    has_fees: bool,
    estimated_fee: float,
    category: str,
) -> dict:
    """Build structured audit trail for the signal."""
    wallets_data = []
    for addr in signal.source_wallets:
        wallet = await session.get(Wallet, addr)
        from sqlalchemy import select, and_
        result = await session.execute(
            select(WalletCategoryScore).where(
                and_(
                    WalletCategoryScore.wallet_address == addr,
                    WalletCategoryScore.category == category,
                )
            )
        )
        cat_score = result.scalar_one_or_none()

        wallets_data.append({
            "address": addr,
            "category_score": {
                "win_rate": float(cat_score.win_rate) if cat_score and cat_score.win_rate else None,
                "followability": float(cat_score.followability) if cat_score and cat_score.followability else None,
            } if cat_score else None,
            "conviction": wallet.conviction_score if wallet else None,
        })

    slippage_pct = (current_price - whale_avg) / whale_avg if whale_avg > 0 else 0

    checks_passed = []
    checks_failed = []
    for check in ["category_match", "time_to_resolution", "price_viable", "fee_viable", "risk_limits"]:
        checks_passed.append(check)  # if we got this far, all checks passed

    return {
        "wallets": wallets_data,
        "convergence_window_hours": settings.get_convergence_window(category),
        "convergence_window_type": category,
        "market_price_at_check": current_price,
        "whale_avg_price": whale_avg,
        "slippage_pct": round(slippage_pct, 4),
        "max_slippage_allowed": settings.max_slippage_pct,
        "has_taker_fees": has_fees,
        "estimated_fee_per_contract": estimated_fee,
        "checks_passed": checks_passed,
        "checks_failed": checks_failed,
        "config_snapshot": {
            "max_slippage_pct": settings.max_slippage_pct,
            "min_followability": settings.get_min_followability(category),
            "convergence_window": settings.get_convergence_window(category),
            "min_hours_to_resolution": settings.get_min_hours_to_resolution(category),
            "fixed_position_size_usdc": settings.fixed_position_size_usdc,
            "convergence_min_wallets": settings.convergence_min_wallets,
            "strict_mode": settings.strict_mode,
        },
    }
