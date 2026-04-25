"""Sell all sports positions on Polymarket.

Uses the market category from the DB (markets.category = 'sports') to identify
sports trades, then sells directly using the token_id from Polymarket's Data API.

Usage:
    # Dry run (default) — shows what would be sold
    .venv/bin/python scripts/sell_sports.py

    # Execute sales
    .venv/bin/python scripts/sell_sports.py --execute
"""

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select, and_

sys.path.insert(0, ".")
from src.config import settings
from src.db import async_session
from src.models import MyTrade, Market
from src.execution.order_manager import place_order, get_order_status
from src.polymarket.clob_client import CLOBClient
from src.polymarket.data_api import DataAPIClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-5s %(message)s")
logger = logging.getLogger(__name__)


async def sell_sports(execute: bool = False):
    wallet = settings.polymarket_wallet_address
    if not wallet:
        logger.error("No wallet address configured")
        return

    # Get sports trades from DB
    async with async_session() as session:
        result = await session.execute(
            select(MyTrade, Market)
            .join(Market, MyTrade.condition_id == Market.condition_id)
            .where(
                and_(
                    MyTrade.resolved == False,
                    MyTrade.fill_status == "FILLED",
                    Market.category == "sports",
                )
            )
            .order_by(MyTrade.id)
        )
        rows = list(result.all())

    if not rows:
        logger.info("No sports positions to sell.")
        return

    # Get live positions from Polymarket to get exact token_ids
    client = DataAPIClient()
    try:
        positions = await client.get_positions(wallet)
    finally:
        await client.close()

    # Build map: (condition_id, outcome_upper) -> token_id from Polymarket
    token_map: dict[tuple[str, str], str] = {}
    for p in positions:
        outcome = (p.outcome or "").upper()
        token_map[(p.condition_id, outcome)] = p.asset

    logger.info("Found %d sports positions to sell:", len(rows))

    clob = CLOBClient()
    sold = 0

    for trade, market in rows:
        question = (market.question or "")[:60]
        key = (trade.condition_id, trade.outcome.upper())
        token_id = token_map.get(key)

        if not token_id:
            logger.error("  #%d SKIP — no Polymarket position for %s %s | %s",
                         trade.id, trade.outcome, trade.condition_id[:12], question)
            continue

        # Get best bid price
        book = await clob.get_orderbook(token_id)
        if book and book.get("bids"):
            best_bid = max(float(b["price"]) for b in book["bids"])
        else:
            mid = await clob.get_midpoint(token_id)
            if mid:
                best_bid = mid * 0.98
            else:
                logger.error("  #%d SKIP — no orderbook for %s | %s", trade.id, trade.outcome, question)
                continue

        pnl = (best_bid - float(trade.entry_price)) * trade.num_contracts
        logger.info(
            "  #%d %s @%.3f → sell %d @%.3f  PnL≈$%.2f  %s",
            trade.id, trade.outcome, float(trade.entry_price),
            trade.num_contracts, best_bid, pnl, question,
        )

        if not execute:
            continue

        # Place SELL order
        order_id = await place_order(
            token_id=token_id,
            side="SELL",
            price=best_bid,
            size=float(trade.num_contracts),
        )

        if not order_id:
            logger.error("  #%d FAILED — order placement returned None", trade.id)
            continue

        # Poll for fill (up to 30s)
        filled = False
        for _ in range(15):
            await asyncio.sleep(2)
            status = await get_order_status(order_id)
            if status and status.get("status") == "MATCHED":
                filled = True
                break

        status_str = "FILLED" if filled else "PENDING"
        logger.info("  #%d → %s  order=%s", trade.id, status_str, order_id)
        sold += 1

        # Don't update DB here — let reconciliation handle it
        # The position will disappear from Polymarket and reconcile will close it

    await clob.close()

    if not execute:
        logger.info("\nDry run. Run with --execute to sell.")
    else:
        logger.info("Sold %d sports positions. Reconciliation will update the DB.", sold)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sell all sports positions")
    parser.add_argument("--execute", action="store_true", help="Actually place sell orders")
    args = parser.parse_args()
    asyncio.run(sell_sports(execute=args.execute))
