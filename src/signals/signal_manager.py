"""Signal lifecycle management: create, promote, expire, skip."""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from collections import Counter

from sqlalchemy import select, and_, update, func
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.db import async_session
from src.models import Signal, Market, WalletCategoryScore, MyTrade
from src.indexer.market_ingester import ensure_market

logger = logging.getLogger(__name__)


@dataclass
class EvalResult:
    """Result of evaluating whether a position should be traded."""
    should_trade: bool
    skip_reason: str | None = None
    market: Market | None = None
    category: str | None = None
    hours_to_resolution: float | None = None


async def evaluate_position(
    session: AsyncSession,
    wallet_address: str,
    condition_id: str,
    outcome: str,
    entry_price: float,
    current_price: float | None = None,
    slug: str | None = None,
) -> EvalResult:
    """Shared evaluation logic for whether to trade a whale's position.

    Retained for legacy signal tools; the live bot uses sync_positions directly.

    Args:
        entry_price: Whale's avg entry price.
        current_price: Current market price (optional, used for slippage check).
        slug: Market slug for Gamma lookup.
    """
    # Price we'll use for range checks — current if available, else entry
    check_price = current_price if current_price is not None else entry_price

    # Price range filter
    if check_price <= 0:
        return EvalResult(False, "no_price")
    if check_price < settings.min_entry_price:
        return EvalResult(False, f"price_too_low:{check_price:.3f}<{settings.min_entry_price}")
    if check_price > settings.max_entry_price_trade:
        return EvalResult(False, f"price_too_high:{check_price:.3f}>{settings.max_entry_price_trade}")

    # Price rule: fill at or better than whale (up to 5% better — beyond that, stale).
    if current_price is not None and entry_price > 0:
        if current_price > entry_price:
            return EvalResult(False, f"slippage_above:{current_price:.3f}>{entry_price:.3f}")
        if current_price < entry_price * (1 - settings.max_slippage_pct):
            return EvalResult(False, f"stale_below:{current_price:.3f}<{entry_price:.3f}*{1 - settings.max_slippage_pct:.2f}")

    # Market lookup + category check
    market = await ensure_market(condition_id, session, slug=slug)
    if not market:
        return EvalResult(False, "no_market")

    category = market.category_override or market.category
    if category not in settings.valid_categories:
        return EvalResult(False, f"bad_category:{category}", market, category)

    # Resolution check
    if market.resolved:
        return EvalResult(False, "resolved", market, category)

    hours_left = None
    if market.resolution_time:
        hours_left = (market.resolution_time - datetime.now(timezone.utc)).total_seconds() / 3600

    return EvalResult(True, None, market, category, hours_left)


async def check_for_duplicate(session: AsyncSession, condition_id: str, outcome: str) -> str | None:
    """Check if we already have a LIVE signal or trade on this condition+outcome.

    Returns a skip reason string ("dup_signal", "dup_trade", "opposite_side"), or None if clear.
    """
    existing_sig = await session.execute(
        select(Signal.id).where(
            and_(
                Signal.condition_id == condition_id,
                Signal.outcome == outcome,
                Signal.signal_type == "LIVE",
                Signal.status.in_(["PENDING", "EXECUTED"]),
            )
        )
    )
    if existing_sig.scalars().first():
        return "dup_signal"

    existing_trade = await session.execute(
        select(MyTrade.id).where(
            and_(
                MyTrade.condition_id == condition_id,
                MyTrade.outcome == outcome,
                MyTrade.resolved == False,
            )
        )
    )
    if existing_trade.scalars().first():
        return "dup_trade"

    opposite = "NO" if outcome == "YES" else "YES"
    opp_trade = await session.execute(
        select(MyTrade.id).where(
            and_(
                MyTrade.condition_id == condition_id,
                MyTrade.outcome == opposite,
                MyTrade.resolved == False,
            )
        )
    )
    if opp_trade.scalars().first():
        return "opposite_side"

    return None


async def expire_stale_signals() -> int:
    """Expire PENDING signals past their expiry or older than 6h. Returns count."""
    from datetime import timedelta
    now = datetime.now(timezone.utc)
    total_expired = 0

    async with async_session() as session:
        # Expire signals with explicit expires_at
        result = await session.execute(
            update(Signal)
            .where(
                and_(
                    Signal.status == "PENDING",
                    Signal.expires_at.isnot(None),
                    Signal.expires_at < now,
                )
            )
            .values(status="EXPIRED", status_reason="convergence_window_expired")
            .returning(Signal.id)
        )
        expired_ids = result.scalars().all()
        total_expired += len(expired_ids)

        # Expire PENDING signals older than 6h with no expires_at
        # (prevents signals from sitting forever and blocking new ones)
        stale_cutoff = now - timedelta(hours=6)
        result2 = await session.execute(
            update(Signal)
            .where(
                and_(
                    Signal.status == "PENDING",
                    Signal.expires_at.is_(None),
                    Signal.created_at < stale_cutoff,
                )
            )
            .values(status="EXPIRED", status_reason="stale_pending_no_expiry")
            .returning(Signal.id)
        )
        stale_ids = result2.scalars().all()
        total_expired += len(stale_ids)

        await session.commit()

        if total_expired:
            logger.info("Expired %d signals (%d window, %d stale)", total_expired, len(expired_ids), len(stale_ids))
        return total_expired


async def get_diagnostics(days: int = 7) -> dict:
    """Get aggregated skip/reject reasons for diagnostics."""
    from datetime import timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    async with async_session() as session:
        # All signals in the period
        result = await session.execute(
            select(Signal).where(Signal.created_at >= cutoff)
        )
        signals = result.scalars().all()

        total = len(signals)
        by_type = Counter(s.signal_type for s in signals)
        by_status = Counter(s.status for s in signals)

        # Skip reasons breakdown
        skip_reasons = Counter()
        skip_by_category = {}
        for s in signals:
            if s.status in ("SKIPPED", "REJECTED", "EXPIRED") and s.status_reason:
                reason = s.status_reason.split(":")[0]  # get base reason
                skip_reasons[reason] += 1

                cat = s.category or "unknown"
                if cat not in skip_by_category:
                    skip_by_category[cat] = Counter()
                skip_by_category[cat][reason] += 1

        # Conversion rates
        candidates = by_type.get("CANDIDATE", 0)
        convergences = by_type.get("CONVERGENCE", 0)
        live = by_type.get("LIVE", 0)

        return {
            "period_days": days,
            "total_signals": total,
            "by_type": dict(by_type),
            "by_status": dict(by_status),
            "skip_reasons": dict(skip_reasons.most_common()),
            "skip_by_category": {
                cat: dict(reasons.most_common())
                for cat, reasons in skip_by_category.items()
            },
            "conversion_rates": {
                "candidate_to_convergence": f"{convergences/candidates:.1%}" if candidates > 0 else "N/A",
                "convergence_to_live": f"{live/convergences:.1%}" if convergences > 0 else "N/A",
                "candidate_to_live": f"{live/candidates:.1%}" if candidates > 0 else "N/A",
            },
        }


async def get_pending_signals() -> list[Signal]:
    """Get all pending signals."""
    async with async_session() as session:
        result = await session.execute(
            select(Signal)
            .where(Signal.status == "PENDING")
            .order_by(Signal.created_at.desc())
        )
        return result.scalars().all()
