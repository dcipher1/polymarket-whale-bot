"""Signal lifecycle management: create, promote, expire, skip."""

import logging
from datetime import datetime, timezone
from collections import Counter

from sqlalchemy import select, and_, update, func
from sqlalchemy.ext.asyncio import AsyncSession

from src.db import async_session
from src.models import Signal

logger = logging.getLogger(__name__)


async def expire_stale_signals() -> int:
    """Expire CANDIDATE and CONVERGENCE signals past their expiry. Returns count."""
    now = datetime.now(timezone.utc)

    async with async_session() as session:
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
        await session.commit()

        if expired_ids:
            logger.info("Expired %d stale signals", len(expired_ids))
        return len(expired_ids)


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
