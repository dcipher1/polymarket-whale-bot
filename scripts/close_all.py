"""Close all open positions by selling on CLOB at best bid.

Usage:
    # Dry run (default) — shows what would be sold
    .venv/bin/python scripts/close_all.py

    # Execute sales
    .venv/bin/python scripts/close_all.py --execute
"""

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select, and_

sys.path.insert(0, ".")
from src.db import async_session
from src.models import MyTrade, MarketToken, Market
from src.execution.order_manager import place_order, get_order_status
from src.polymarket.clob_client import CLOBClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-5s %(message)s")
logger = logging.getLogger(__name__)


async def close_all(execute: bool = False):
    clob = CLOBClient()

    async with async_session() as session:
        result = await session.execute(
            select(MyTrade).where(
                and_(
                    MyTrade.resolved == False,
                    MyTrade.fill_status == "FILLED",
                )
            ).order_by(MyTrade.id)
        )
        trades = list(result.scalars().all())

    if not trades:
        logger.info("No open positions to close.")
        return

    logger.info("Found %d open positions to close:", len(trades))

    for trade in trades:
        # Get market name for display
        async with async_session() as session:
            market = await session.get(Market, trade.condition_id)
            question = market.question[:60] if market else trade.condition_id[:20]

            # Look up token_id
            token_result = await session.execute(
                select(MarketToken).where(
                    and_(
                        MarketToken.condition_id == trade.condition_id,
                        MarketToken.outcome == trade.outcome,
                    )
                )
            )
            token = token_result.scalar_one_or_none()

        if not token:
            logger.error("  #%d SKIP — no token found for %s %s", trade.id, trade.condition_id[:12], trade.outcome)
            continue

        # Get best bid price
        book = await clob.get_orderbook(token.token_id)
        if book and book.get("bids"):
            best_bid = max(float(b["price"]) for b in book["bids"])
        else:
            mid = await clob.get_midpoint(token.token_id)
            if mid:
                best_bid = mid * 0.98  # slightly below mid if no bids
            else:
                logger.error("  #%d SKIP — no orderbook for %s", trade.id, question)
                continue

        pnl = (best_bid - float(trade.entry_price)) * trade.num_contracts
        logger.info(
            "  #%d %s %s @%.3f → sell %d contracts @%.3f  PnL≈$%.2f  %s",
            trade.id, trade.outcome, token.token_id[:12],
            float(trade.entry_price), trade.num_contracts, best_bid, pnl, question,
        )

        if not execute:
            continue

        # Place SELL order
        order_id = await place_order(
            token_id=token.token_id,
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

        # Update DB
        async with async_session() as session:
            t = await session.get(MyTrade, trade.id)
            t.exit_price = Decimal(str(round(best_bid, 6)))
            t.exit_timestamp = datetime.now(timezone.utc)
            t.pnl_usdc = Decimal(str(round(pnl, 2)))
            t.resolved = True
            t.trade_outcome = "EARLY_EXIT"
            await session.commit()

        status_str = "FILLED" if filled else "PENDING (check manually)"
        logger.info("  #%d → %s  order=%s", trade.id, status_str, order_id)

    await clob.close()

    if not execute:
        logger.info("\nDry run complete. Run with --execute to sell.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Close all open positions")
    parser.add_argument("--execute", action="store_true", help="Actually place sell orders")
    args = parser.parse_args()
    asyncio.run(close_all(execute=args.execute))
