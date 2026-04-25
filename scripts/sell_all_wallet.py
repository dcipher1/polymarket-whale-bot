"""Sell ALL positions and redeem resolved winners in the Polymarket wallet.

Fetches positions from the raw Data API (not DB), sells active ones at best bid,
and redeems resolved winners.

Usage:
    # Dry run (default)
    .venv/bin/python scripts/sell_all_wallet.py

    # Execute sales + redemptions
    .venv/bin/python scripts/sell_all_wallet.py --execute
"""

import argparse
import asyncio
import logging
import sys

import aiohttp

sys.path.insert(0, ".")
from src.config import settings
from src.polymarket.clob_client import CLOBClient
from src.execution.order_manager import place_order, get_order_status

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-5s %(message)s")
logger = logging.getLogger(__name__)


async def fetch_all_positions(wallet: str) -> list[dict]:
    """Fetch ALL positions including zero-size via raw Data API."""
    url = "https://data-api.polymarket.com/positions"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params={"user": wallet, "sizeThreshold": "0"}) as resp:
            if resp.status != 200:
                logger.error("Data API returned %d", resp.status)
                return []
            return await resp.json()


async def sell_all(execute: bool = False):
    wallet = settings.polymarket_wallet_address
    if not wallet:
        logger.error("No polymarket_wallet_address configured")
        return

    logger.info("Fetching ALL positions for wallet %s...", wallet[:12])
    all_positions = await fetch_all_positions(wallet)

    if not all_positions:
        logger.info("No positions found.")
        return

    # Split into sellable and redeemable
    to_sell = []
    to_redeem = []
    for p in all_positions:
        size = float(p.get("size", 0))
        redeemable = p.get("redeemable", False)
        if size > 0 and redeemable:
            to_redeem.append(p)
        elif size > 0:
            to_sell.append(p)

    logger.info("Found %d to sell, %d to redeem, %d other (zero-size/empty)",
                len(to_sell), len(to_redeem), len(all_positions) - len(to_sell) - len(to_redeem))

    # --- SELL active positions ---
    if to_sell:
        logger.info("\n=== SELLING %d active positions ===", len(to_sell))
        clob = CLOBClient()
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
                    "  %s | %.1f contracts @%.3f → sell @%.3f | pnl=$%.2f | %s",
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
        finally:
            await clob.close()

    # --- REDEEM resolved positions ---
    if to_redeem:
        logger.info("\n=== REDEEMING %d resolved positions ===", len(to_redeem))
        for p in to_redeem:
            size = float(p.get("size", 0))
            title = (p.get("title", "") or p.get("conditionId", ""))[:55]
            outcome = p.get("outcome", "?")
            cid = p.get("conditionId", "")
            neg_risk = p.get("negativeRisk", False)

            logger.info("  %s | %.1f contracts | %s", outcome, size, title)

            if not execute:
                continue

            try:
                from src.tracking.resolution import _redeem_positions
                await _redeem_positions(cid, neg_risk=neg_risk)
                logger.info("  → REDEEMED")
            except Exception as e:
                logger.warning("  → REDEEM FAILED: %s", e)

    if not execute:
        logger.info("\nDry run complete. Run with --execute to sell + redeem.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sell all wallet positions + redeem winners")
    parser.add_argument("--execute", action="store_true", help="Actually place sell orders and redeem")
    args = parser.parse_args()
    asyncio.run(sell_all(execute=args.execute))
