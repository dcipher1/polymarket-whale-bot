"""Sell positions from orphaned trades (whales no longer COPYABLE).

Fetches wallet positions from Polymarket, cross-references with DB trades
whose source whales are no longer COPYABLE, and sells only those.

Usage:
    # Dry run (default)
    .venv/bin/python scripts/sell_orphaned.py

    # Execute sales
    .venv/bin/python scripts/sell_orphaned.py --execute
"""

import argparse
import asyncio
import logging
import sys

import aiohttp

sys.path.insert(0, ".")
from src.config import settings
from src.db import async_session
from src.polymarket.clob_client import CLOBClient
from src.execution.order_manager import place_order, get_order_status
from sqlalchemy import text

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-5s %(message)s")
logger = logging.getLogger(__name__)


async def get_orphaned_positions() -> set[tuple[str, str]]:
    """Return (condition_id, outcome) pairs for trades from non-COPYABLE whales."""
    async with async_session() as s:
        result = await s.execute(text("""
            SELECT DISTINCT mt.condition_id, mt.outcome
            FROM my_trades mt
            WHERE (mt.resolved = false OR mt.resolved IS NULL)
              AND NOT EXISTS (
                SELECT 1 FROM wallets w
                WHERE w.address = ANY(mt.source_wallets)
                  AND w.copyability_class = 'COPYABLE'
              )
        """))
        return {(r[0], r[1]) for r in result.fetchall()}


async def fetch_all_positions(wallet: str) -> list[dict]:
    """Fetch ALL positions from Polymarket Data API."""
    url = "https://data-api.polymarket.com/positions"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params={"user": wallet, "sizeThreshold": "0"}) as resp:
            if resp.status != 200:
                logger.error("Data API returned %d", resp.status)
                return []
            return await resp.json()


async def sell_orphaned(execute: bool = False):
    wallet = settings.polymarket_wallet_address
    if not wallet:
        logger.error("No polymarket_wallet_address configured")
        return

    logger.info("Fetching orphaned trade positions...")
    orphaned_keys = await get_orphaned_positions()
    logger.info("Found %d orphaned (condition_id, outcome) pairs in DB", len(orphaned_keys))

    logger.info("Fetching wallet positions from Polymarket...")
    all_positions = await fetch_all_positions(wallet)

    # Match wallet positions to orphaned trades
    to_sell = []
    for p in all_positions:
        size = float(p.get("size", 0))
        if size <= 0:
            continue
        cid = p.get("conditionId", "")
        outcome = p.get("outcome", "").upper()
        if (cid, outcome) in orphaned_keys:
            to_sell.append(p)

    if not to_sell:
        logger.info("No orphaned positions found on-chain to sell.")
        return

    total_contracts = sum(float(p.get("size", 0)) for p in to_sell)
    logger.info("Matched %d positions to sell (%.0f contracts)", len(to_sell), total_contracts)

    clob = CLOBClient()
    sold = 0
    try:
        for p in to_sell:
            size = float(p.get("size", 0))
            avg_price = float(p.get("avgPrice", 0))
            cur_price = float(p.get("curPrice", 0))
            title = (p.get("title", "") or p.get("conditionId", ""))[:55]
            outcome = p.get("outcome", "?")
            token_id = p.get("asset", "")

            if not token_id:
                logger.error("  SKIP — no token_id for %s %s", title, outcome)
                continue

            # Get best bid
            book = await clob.get_orderbook(token_id)
            if book and book.get("bids"):
                best_bid = max(float(b["price"]) for b in book["bids"])
            else:
                mid = await clob.get_midpoint(token_id)
                if mid:
                    best_bid = mid * 0.98
                else:
                    logger.error("  SKIP — no orderbook for %s", title)
                    continue

            pnl = (best_bid - avg_price) * size
            logger.info(
                "  %s | %.1f ct @%.3f → sell @%.3f | pnl=$%.2f | %s",
                outcome, size, avg_price, best_bid, pnl, title,
            )

            if not execute:
                continue

            order_id = await place_order(
                token_id=token_id, side="SELL", price=best_bid, size=size,
            )
            if not order_id:
                logger.error("  FAILED — order placement returned None")
                continue

            # Poll for fill (up to 30s)
            filled = False
            for _ in range(15):
                await asyncio.sleep(2)
                status = await get_order_status(order_id)
                if status and status.get("status") == "MATCHED":
                    filled = True
                    break

            logger.info("  → %s  order=%s", "FILLED" if filled else "PENDING", order_id)
            sold += 1
    finally:
        await clob.close()

    if execute:
        logger.info("Sold %d/%d orphaned positions", sold, len(to_sell))
    else:
        logger.info("\nDry run complete. Run with --execute to sell.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sell orphaned positions (non-COPYABLE whales)")
    parser.add_argument("--execute", action="store_true", help="Actually place sell orders")
    args = parser.parse_args()
    asyncio.run(sell_orphaned(execute=args.execute))
