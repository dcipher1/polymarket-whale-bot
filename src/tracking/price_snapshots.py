"""Capture price snapshots after whale trades for real followability computation.

When a whale trade is detected, we schedule price checks at 5m, 30m, 2h, and 24h.
These snapshots replace the Phase 1 "next trade proxy" with actual market prices,
allowing us to compute real followability scores.
"""

import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from src.db import async_session
from src.events import cache_set, cache_get
from src.models import WalletCategoryScore, Market, MarketToken
from src.config import settings
from src.polymarket.clob_client import CLOBClient

logger = logging.getLogger(__name__)

# Snapshot windows in seconds
WINDOWS = {
    "5m": 300,
    "30m": 1800,
    "2h": 7200,
    "24h": 86400,
}

# In-memory queue of pending snapshots
_pending_snapshots: list[dict] = []


async def schedule_snapshot(
    wallet_address: str,
    condition_id: str,
    token_id: str,
    outcome: str,
    category: str,
    entry_price: float,
    entry_time: datetime,
) -> None:
    """Schedule price snapshots at 5m, 30m, 2h, 24h after a whale trade."""
    for window_name, delay_seconds in WINDOWS.items():
        _pending_snapshots.append({
            "wallet_address": wallet_address,
            "condition_id": condition_id,
            "token_id": token_id,
            "outcome": outcome,
            "category": category,
            "entry_price": entry_price,
            "window": window_name,
            "check_at": entry_time.timestamp() + delay_seconds,
        })

    logger.debug(
        "Scheduled 4 price snapshots for %s on %s",
        wallet_address[:10], condition_id[:10],
    )


async def process_pending_snapshots() -> int:
    """Check and capture any snapshots that are due. Returns count captured."""
    if not _pending_snapshots:
        return 0

    now = datetime.now(timezone.utc).timestamp()
    due = [s for s in _pending_snapshots if s["check_at"] <= now]

    if not due:
        return 0

    clob = CLOBClient()
    captured = 0

    for snap in due:
        try:
            price_data = await clob.get_price(snap["token_id"])
            if not price_data:
                continue

            current_price = price_data["mid"]
            entry_price = snap["entry_price"]
            slippage = abs(current_price - entry_price) / entry_price if entry_price > 0 else 1.0
            followable = slippage <= settings.max_slippage_pct

            # Store snapshot in Redis for aggregation
            key = f"snapshot:{snap['wallet_address']}:{snap['category']}:{snap['window']}"
            existing = await cache_get(key) or {"followable": 0, "total": 0}
            existing["total"] += 1
            if followable:
                existing["followable"] += 1
            await cache_set(key, existing, ex=604800)  # 7 day TTL

            captured += 1

            logger.debug(
                "Snapshot %s: %s %s entry=%.4f now=%.4f slip=%.2f%% %s",
                snap["window"], snap["wallet_address"][:10],
                snap["condition_id"][:10], entry_price, current_price,
                slippage * 100, "FOLLOWABLE" if followable else "NOT_FOLLOWABLE",
            )

        except Exception as e:
            logger.debug("Snapshot capture failed: %s", e)

        # Remove from pending regardless
        _pending_snapshots.remove(snap)

    await clob.close()

    if captured > 0:
        logger.info("Captured %d price snapshots (%d pending)", captured, len(_pending_snapshots))

    return captured


async def recompute_followability_from_snapshots() -> int:
    """Recompute followability scores using captured snapshots.

    Compares real snapshot-based scores against provisional (Phase 1) scores.
    Returns count of wallets updated.
    """
    updated = 0

    async with async_session() as session:
        result = await session.execute(
            select(WalletCategoryScore).where(
                WalletCategoryScore.followability_provisional == True
            )
        )
        scores = result.scalars().all()

        for score in scores:
            any_updated = False
            for window in ["5m", "30m", "2h", "24h"]:
                key = f"snapshot:{score.wallet_address}:{score.category}:{window}"
                data = await cache_get(key)
                if not data or data["total"] < 5:
                    continue  # not enough data yet

                real_followability = round(data["followable"] / data["total"], 4)
                field = f"followability_{window}"
                old_val = float(getattr(score, field) or 0)

                setattr(score, field, Decimal(str(real_followability)))
                any_updated = True

                if abs(real_followability - old_val) > 0.10:
                    logger.warning(
                        "Followability divergence for %s %s %s: provisional=%.2f real=%.2f",
                        score.wallet_address[:10], score.category, window,
                        old_val, real_followability,
                    )

            if any_updated:
                # Update primary followability based on category window
                primary_window = settings.get_followability_window(score.category)
                primary_field = f"followability_{primary_window}"
                primary_val = getattr(score, primary_field)
                if primary_val is not None:
                    score.followability = primary_val
                    score.followability_provisional = False
                    updated += 1

        await session.commit()

    if updated > 0:
        logger.info("Updated %d followability scores from real snapshots", updated)

    return updated


async def run_snapshot_processor(interval_seconds: float = 30):
    """Background loop to process pending snapshots."""
    while True:
        try:
            await process_pending_snapshots()
        except Exception as e:
            logger.error("Snapshot processor error: %s", e)
        await asyncio.sleep(interval_seconds)
