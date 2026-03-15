"""Seed initial whale wallet addresses and backfill their history."""

import asyncio
import json
import logging
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


async def seed_from_file(filepath: str) -> None:
    from src.indexer.wallet_ingester import ingest_wallet
    from src.indexer.position_builder import build_positions_for_wallet

    path = Path(filepath)
    if not path.exists():
        logger.error("Seed file not found: %s", filepath)
        sys.exit(1)

    with open(path) as f:
        data = json.load(f)

    addresses = data if isinstance(data, list) else data.get("wallets", [])
    logger.info("Seeding %d wallets from %s", len(addresses), filepath)

    for i, entry in enumerate(addresses):
        address = entry if isinstance(entry, str) else entry.get("address", "")
        if not address:
            continue

        logger.info("[%d/%d] Ingesting wallet %s...", i + 1, len(addresses), address[:10])
        try:
            new_trades = await ingest_wallet(address, full_backfill=True)
            positions = await build_positions_for_wallet(address)
            logger.info(
                "  -> %d trades ingested, %d positions built", new_trades, positions
            )
        except Exception as e:
            logger.error("  -> Failed: %s", e)


async def main():
    filepath = sys.argv[1] if len(sys.argv) > 1 else "data/seed_wallets.json"
    await seed_from_file(filepath)


if __name__ == "__main__":
    asyncio.run(main())
