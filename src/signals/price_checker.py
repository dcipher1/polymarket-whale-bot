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

        # Quick liquidity pre-filter
        if market.liquidity_usdc is not None and float(market.liquidity_usdc) < settings.min_market_liquidity:
            await _skip_signal(
                session, signal,
                f"market_illiquid:liquidity=${float(market.liquidity_usdc):.0f}<${settings.min_market_liquidity:.0f}",
            )
            return False

        # Check time to resolution
        if market.resolution_time:
            hours_left = (
                market.resolution_time - datetime.now(timezone.utc)
            ).total_seconds() / 3600
            # Explicit past-resolution guard
            if hours_left <= 0:
                await _skip_signal(session, signal, "market_past_resolution")
                return False
            # (too_soon filter removed — short-dated markets are allowed)
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

        # Price ceiling — reject effectively-resolved markets
        if current_price >= settings.max_entry_price:
            await _skip_signal(
                session, signal,
                f"price_too_high:current={current_price:.4f},ceiling={settings.max_entry_price}",
            )
            return False

        # Slippage check — max % above whale's entry
        whale_avg = float(signal.whale_avg_price or 0)
        max_slippage = 1 + settings.max_slippage_pct
        max_entry = min(
            whale_avg * max_slippage,
            settings.max_entry_price,
        )

        if whale_avg > 0 and current_price > max_entry:
            slippage_pct = (current_price - whale_avg) / whale_avg
            await _skip_signal(
                session, signal,
                f"price_exceeded_slippage:current={current_price:.4f},whale={whale_avg:.4f},slippage={slippage_pct:.1%}>{settings.max_slippage_pct:.0%}",
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

        # Expected value using whale's actual category win rate
        avg_win_rate = await _get_avg_wallet_win_rate(session, signal.source_wallets, category)
        ev = avg_win_rate - current_price - estimated_fee

        # Raw edge (theoretical 50/50 payout minus cost)
        raw_edge = (1.0 - current_price) - (current_price - whale_avg) - estimated_fee

        # Both EV and raw edge must be positive
        if ev < settings.min_edge_threshold or raw_edge < settings.min_edge_threshold:
            await _skip_signal(
                session, signal,
                f"insufficient_edge:ev={ev:.4f},raw_edge={raw_edge:.4f},win_rate={avg_win_rate:.4f},fee={estimated_fee:.4f}",
            )
            return False

        # Orderbook depth check (observe-only in Phase 2)
        depth_info = await _check_orderbook_depth(
            token.token_id if token else None, current_price,
        )
        depth_sufficient = True
        if depth_info:
            required_depth = settings.fixed_position_size_usdc * settings.min_depth_multiple
            if depth_info["ask_depth_usdc"] < required_depth:
                depth_sufficient = False
                if settings.enforce_depth_check:
                    await _skip_signal(
                        session, signal,
                        f"insufficient_depth:depth=${depth_info['ask_depth_usdc']:.0f}<${required_depth:.0f}",
                    )
                    return False
                else:
                    logger.info(
                        "Signal %d depth warning: $%.0f available, need $%.0f (observe-only)",
                        signal.id, depth_info["ask_depth_usdc"], required_depth,
                    )

        # Build audit trail
        audit = await _build_audit_trail(
            session, signal, current_price, whale_avg, has_fees, estimated_fee,
            category, depth_info, depth_sufficient, avg_win_rate,
        )

        # Promote to LIVE
        signal.signal_type = "LIVE"
        signal.market_price_at_signal = Decimal(str(round(current_price, 6)))
        signal.suggested_size_usdc = Decimal(str(settings.fixed_position_size_usdc))
        signal.hours_to_resolution = Decimal(str(hours_left)) if hours_left else None
        signal.audit_trail = audit

        await session.commit()

        logger.info(
            "Signal %d promoted to LIVE: %s %s at %.4f (whale avg %.4f, ev=%.4f, win_rate=%.2f)",
            signal.id, signal.condition_id[:10], signal.outcome, current_price, whale_avg, ev, avg_win_rate,
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


async def _check_orderbook_depth(
    token_id: str | None,
    current_price: float,
    slippage_tolerance: float = 0.05,
) -> dict | None:
    """Check orderbook ask-side depth within slippage tolerance.

    Returns dict with depth info, or None if orderbook unavailable.
    """
    if not token_id:
        return None

    clob = CLOBClient()
    try:
        book = await clob.get_orderbook(token_id)
        if not book:
            return None

        asks = book.get("asks", [])
        if not asks:
            return None

        max_price = current_price * (1 + slippage_tolerance)
        depth_contracts = 0.0
        depth_usdc = 0.0

        for ask in asks:
            price = float(ask.get("price", 0))
            size = float(ask.get("size", 0))
            if price <= max_price and price > 0:
                depth_contracts += size
                depth_usdc += price * size

        return {
            "ask_depth_contracts": round(depth_contracts, 2),
            "ask_depth_usdc": round(depth_usdc, 2),
            "levels_checked": len(asks),
            "max_price_checked": round(max_price, 4),
        }
    except Exception as e:
        logger.debug("Orderbook depth check failed for %s: %s", token_id, e)
        return None
    finally:
        await clob.close()


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
    depth_info: dict | None,
    depth_sufficient: bool,
    avg_win_rate: float = 0.50,
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

    slippage_abs = current_price - whale_avg
    slippage_pct = slippage_abs / whale_avg if whale_avg > 0 else 0
    raw_edge = (1.0 - current_price) - slippage_abs - estimated_fee
    ev = avg_win_rate - current_price - estimated_fee

    checks_passed = ["category_match", "time_to_resolution", "price_viable", "edge_viable"]
    checks_failed = []
    if not depth_sufficient:
        checks_failed.append("depth_check")
    else:
        checks_passed.append("depth_check")
    checks_passed.append("risk_limits")

    trail = {
        "wallets": wallets_data,
        "convergence_window_hours": settings.get_convergence_window(category),
        "convergence_window_type": category,
        "market_price_at_check": current_price,
        "whale_avg_price": whale_avg,
        "slippage_abs": round(slippage_abs, 4),
        "slippage_pct": round(slippage_pct, 4),
        "max_absolute_slippage": settings.max_absolute_slippage,
        "has_taker_fees": has_fees,
        "estimated_fee_per_contract": estimated_fee,
        "raw_edge": round(raw_edge, 4),
        "expected_value": round(ev, 4),
        "avg_win_rate": round(avg_win_rate, 4),
        "checks_passed": checks_passed,
        "checks_failed": checks_failed,
        "config_snapshot": {
            "max_absolute_slippage": settings.max_absolute_slippage,
            "min_edge_threshold": settings.min_edge_threshold,
            "min_followability": settings.get_min_followability(category),
            "convergence_window": settings.get_convergence_window(category),
            "min_hours_to_resolution": settings.get_min_hours_to_resolution(category),
            "fixed_position_size_usdc": settings.fixed_position_size_usdc,
            "convergence_min_wallets": settings.convergence_min_wallets,
            "strict_mode": settings.strict_mode,
            "enforce_depth_check": settings.enforce_depth_check,
        },
    }

    if depth_info:
        trail["orderbook_depth"] = depth_info

    return trail


async def _get_avg_wallet_win_rate(
    session: AsyncSession,
    source_wallets: list[str],
    category: str,
) -> float:
    """Get average win rate of source wallets for this category.

    Provisional wallets (not enough resolved trades) are floored at 0.50.
    """
    from sqlalchemy import select, and_

    win_rates = []
    for addr in source_wallets:
        result = await session.execute(
            select(WalletCategoryScore).where(
                and_(
                    WalletCategoryScore.wallet_address == addr,
                    WalletCategoryScore.category == category,
                )
            )
        )
        wcs = result.scalar_one_or_none()
        if wcs and wcs.win_rate is not None:
            win_rates.append(float(wcs.win_rate))
        else:
            win_rates.append(0.50)  # no data → assume coinflip

    return sum(win_rates) / len(win_rates) if win_rates else 0.50
