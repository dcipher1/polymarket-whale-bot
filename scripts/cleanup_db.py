"""One-time DB cleanup: fix bad entry prices, prune dead markets, rotate API responses."""

import asyncio
import logging
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

from sqlalchemy import text

from src.db import async_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def fix_bad_entry_prices():
    """Clamp avg_entry_price to [0, 1] for existing bad records."""
    async with async_session() as session:
        result = await session.execute(text(
            "UPDATE whale_positions SET avg_entry_price = LEAST(GREATEST(avg_entry_price, 0), 1) "
            "WHERE avg_entry_price > 1 OR avg_entry_price < 0"
        ))
        count = result.rowcount
        await session.commit()
    logger.info("Fixed %d whale_positions with bad entry prices", count)


async def mark_expired_markets_resolved():
    """Mark obviously-expired unresolved markets as resolved (overdue by >7 days)."""
    async with async_session() as session:
        cutoff = datetime.now(timezone.utc) - timedelta(days=7)
        result = await session.execute(text(
            "UPDATE markets SET resolved = TRUE "
            "WHERE resolved = FALSE AND resolution_time IS NOT NULL AND resolution_time < :cutoff"
        ), {"cutoff": cutoff})
        count = result.rowcount
        await session.commit()
    logger.info("Marked %d stale overdue markets as resolved", count)


async def mark_gamma_gone_markets():
    """Mark overdue markets that have no whale trades as gamma_gone to stop polling."""
    async with async_session() as session:
        cutoff = datetime.now(timezone.utc) - timedelta(days=30)
        result = await session.execute(text("""
            UPDATE markets SET meta = jsonb_set(COALESCE(meta, '{}'), '{gamma_gone}', 'true')
            WHERE resolved = FALSE
              AND resolution_time IS NOT NULL
              AND resolution_time < :cutoff
              AND condition_id NOT IN (
                  SELECT DISTINCT condition_id FROM whale_trades
              )
              AND COALESCE(meta->>'gamma_gone', 'false') != 'true'
        """), {"cutoff": cutoff})
        count = result.rowcount
        await session.commit()
    logger.info("Marked %d stale markets as gamma_gone", count)


def rotate_api_responses(keep_hours: int = 24):
    """Delete API response files older than keep_hours."""
    api_dir = Path("data/api_responses")
    if not api_dir.exists():
        return
    cutoff = datetime.now(timezone.utc) - timedelta(hours=keep_hours)
    deleted = 0
    for f in api_dir.glob("*.json"):
        try:
            mtime = datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc)
            if mtime < cutoff:
                f.unlink()
                deleted += 1
        except Exception:
            pass
    logger.info("Deleted %d old API response files (kept last %dh)", deleted, keep_hours)


async def main():
    logger.info("=== DB Cleanup ===")
    await fix_bad_entry_prices()
    await mark_expired_markets_resolved()
    await mark_gamma_gone_markets()
    rotate_api_responses()
    logger.info("=== Done ===")


if __name__ == "__main__":
    asyncio.run(main())
