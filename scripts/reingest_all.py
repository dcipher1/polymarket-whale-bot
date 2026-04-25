"""Re-ingest all wallets with trade data using the activity API.

Calls ingest_wallet(full_backfill=True) for every wallet that has existing
whale_trades. Existing trades are deduped, new ones are inserted.

Usage:
    python3 -m scripts.reingest_all
"""

import asyncio
import logging

from sqlalchemy import text

from src.db import async_session
from src.indexer.wallet_ingester import ingest_wallet
from src.polymarket.data_api import DataAPIClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


async def main():
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT DISTINCT w.address, w.display_name
                FROM wallets w
                JOIN whale_trades wt ON wt.wallet_address = w.address
                ORDER BY w.display_name
            """)
        )
        wallets = [(r[0], r[1] or r[0][:10]) for r in result.all()]

    logger.info("Re-ingesting %d wallets with trade data", len(wallets))

    if not wallets:
        return

    client = DataAPIClient()
    sem = asyncio.Semaphore(3)
    total_new = 0
    errors = 0

    async def reingest_one(addr: str, name: str, idx: int):
        nonlocal total_new, errors
        async with sem:
            try:
                new = await asyncio.wait_for(
                    ingest_wallet(addr, full_backfill=True, client=client),
                    timeout=120,
                )
                if new > 0:
                    total_new += new
                    logger.info("  %s: +%d new trades", name[:20], new)
            except asyncio.TimeoutError:
                logger.warning("  TIMEOUT: %s", name[:20])
                errors += 1
            except Exception as e:
                logger.error("  ERROR: %s: %s", name[:20], str(e)[:80])
                errors += 1

        if (idx + 1) % 50 == 0:
            logger.info("Progress: %d/%d (new_trades=%d, errors=%d)", idx + 1, len(wallets), total_new, errors)

    tasks = [reingest_one(addr, name, i) for i, (addr, name) in enumerate(wallets)]
    await asyncio.gather(*tasks)

    await client.close()
    logger.info("Re-ingest complete: %d new trades across %d wallets (%d errors)", total_new, len(wallets), errors)


if __name__ == "__main__":
    asyncio.run(main())
