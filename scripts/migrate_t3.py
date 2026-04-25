"""One-off migration: re-evaluate NULL-class wallets through new activity-based T3/T4.

These wallets were discovered previously but never ingested (no trade data).
Runs _process_candidate() directly, bypassing T1 leaderboard scanning.

Usage:
    python3 -m scripts.migrate_t3
"""

import asyncio
import logging
import sys
from collections import defaultdict

from sqlalchemy import select, and_, text

from src.db import async_session
from src.models import Wallet
from src.indexer.whale_discovery import _process_candidate
from src.polymarket.data_api import DataAPIClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


async def main():
    # Get all NULL-class wallets without trade data
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT w.address, w.display_name
                FROM wallets w
                WHERE w.copyability_class IS NULL
                AND NOT EXISTS (
                    SELECT 1 FROM whale_trades wt WHERE wt.wallet_address = w.address
                )
                ORDER BY w.first_seen
            """)
        )
        wallets = [(r[0], r[1] or r[0][:10]) for r in result.all()]

    logger.info("Found %d NULL-class wallets to re-evaluate", len(wallets))

    if not wallets:
        return

    data_client = DataAPIClient()
    sem = asyncio.Semaphore(3)
    passed = 0
    failed = 0
    errors = 0

    async def process_one(addr: str, name: str, idx: int):
        nonlocal passed, failed, errors
        async with sem:
            try:
                result = await asyncio.wait_for(
                    _process_candidate(
                        {"address": addr, "name": name},
                        data_client,
                    ),
                    timeout=300,
                )
                if result:
                    passed += 1
                else:
                    failed += 1
            except asyncio.TimeoutError:
                logger.warning("  TIMEOUT: %s (%s) after 5min", name[:20], addr[:10])
                errors += 1
            except Exception as e:
                logger.error("  ERROR: %s (%s): %s", name[:20], addr[:10], str(e)[:80])
                errors += 1

        if (idx + 1) % 25 == 0:
            logger.info(
                "Progress: %d/%d (passed=%d, failed=%d, errors=%d)",
                idx + 1, len(wallets), passed, failed, errors,
            )

    tasks = [process_one(addr, name, i) for i, (addr, name) in enumerate(wallets)]
    await asyncio.gather(*tasks)

    await data_client.close()

    logger.info(
        "Migration complete: %d passed T3/T4, %d rejected, %d errors (of %d total)",
        passed, failed, errors, len(wallets),
    )


if __name__ == "__main__":
    asyncio.run(main())
