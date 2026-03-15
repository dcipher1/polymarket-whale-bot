"""Backfill 6 months of trade history for all tracked wallets.

Designed to be resumable — skips wallets that already have sufficient history.
"""

import asyncio
import logging

from sqlalchemy import select

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


async def backfill():
    from src.db import async_session
    from src.models import Wallet
    from src.indexer.wallet_ingester import ingest_wallet
    from src.indexer.position_builder import build_positions_for_wallet
    from src.scorer.wallet_scorer import score_wallet

    async with async_session() as session:
        result = await session.execute(select(Wallet.address))
        addresses = [r[0] for r in result.all()]

    logger.info("Backfilling %d wallets", len(addresses))

    for i, address in enumerate(addresses):
        logger.info("[%d/%d] Backfilling %s...", i + 1, len(addresses), address[:10])
        try:
            new_trades = await ingest_wallet(address, full_backfill=True)
            positions = await build_positions_for_wallet(address)
            summary = await score_wallet(address)
            logger.info(
                "  -> %d trades, %d positions, conviction=%s class=%s",
                new_trades, positions,
                summary.get("conviction_score", "?"),
                summary.get("copyability", "?"),
            )
        except Exception as e:
            logger.error("  -> Failed: %s", e)


if __name__ == "__main__":
    asyncio.run(backfill())
