"""Liquidate all open positions on the whale-bot wallet.

Pulls open positions from Polymarket's /positions API (source of truth — not
my_trades, which can drift), looks up each token's best_bid via the existing
CLOB client, and places SELL FAK orders at 90% of the bid for guaranteed fill.

Skips:
  - size < 1 contract
  - best_bid <= 0.01 (let those resolve naturally; selling captures ~nothing)

Usage:
  python -m scripts.sell_all --dry-run     # preview only
  python -m scripts.sell_all --execute     # actually place sells
"""
from __future__ import annotations

import argparse
import asyncio
import logging

from src.config import settings
from src.execution.order_manager import place_order, quantize_order_price
from src.polymarket.clob_client import CLOBClient
from src.polymarket.data_api import DataAPIClient
from src.signals.sync_positions import _get_book_prices

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)


SLEEP_BETWEEN_ORDERS = 0.5
MIN_BID = 0.01
SELL_AT_FRAC_OF_BID = 0.90


async def run(execute: bool) -> int:
    wallet = settings.polymarket_wallet_address
    if not wallet:
        log.error("polymarket_wallet_address not configured")
        return 1

    data_api = DataAPIClient()
    clob = CLOBClient()
    try:
        positions = await data_api.get_positions(wallet)
        log.info("Wallet %s — %d open positions", wallet, len(positions))
        if not positions:
            return 0

        log.info("\n%-55s %-4s %8s %7s %7s %9s  %s",
                 "slug", "side", "size", "bid", "sell@", "est_$", "decision")
        log.info("-" * 110)

        sold = 0
        skipped_low_bid = 0
        skipped_no_book = 0
        skipped_small = 0
        failed = 0
        total_est = 0.0

        for p in positions:
            size = float(p.size or 0)
            outcome = (p.outcome or "").upper()
            slug = (p.slug or p.title or "")[:55]
            token_id = p.asset

            if size < 1:
                log.info("%-55s %-4s %8.2f                     %s", slug, outcome, size, "SKIP small")
                skipped_small += 1
                continue
            if not token_id:
                log.info("%-55s %-4s %8.2f                     %s", slug, outcome, size, "SKIP no_token")
                skipped_no_book += 1
                continue

            best_bid, _best_ask, _mid = await _get_book_prices(clob, token_id)
            if best_bid is None:
                log.info("%-55s %-4s %8.0f                     %s", slug, outcome, size, "SKIP no_book")
                skipped_no_book += 1
                continue
            if best_bid <= MIN_BID:
                log.info("%-55s %-4s %8.0f %7.4f                  %s",
                         slug, outcome, size, best_bid, "SKIP bid<=$0.01")
                skipped_low_bid += 1
                continue

            sell_price = quantize_order_price(best_bid * SELL_AT_FRAC_OF_BID, "SELL")
            sell_price = max(sell_price, MIN_BID)
            num_contracts = int(size)
            est_value = round(num_contracts * sell_price, 2)
            total_est += est_value

            tag = f"{slug[:30]} {outcome}"

            if not execute:
                log.info("%-55s %-4s %8d %7.4f %7.4f $%8.2f  %s",
                         slug, outcome, num_contracts, best_bid, sell_price, est_value, "DRY")
                continue

            order_id = await place_order(token_id, "SELL", sell_price, num_contracts, tag=tag)
            if order_id:
                log.info("%-55s %-4s %8d %7.4f %7.4f $%8.2f  SOLD order=%s",
                         slug, outcome, num_contracts, best_bid, sell_price, est_value, order_id[:20])
                sold += 1
            else:
                log.info("%-55s %-4s %8d %7.4f %7.4f $%8.2f  FAILED",
                         slug, outcome, num_contracts, best_bid, sell_price, est_value)
                failed += 1
            await asyncio.sleep(SLEEP_BETWEEN_ORDERS)

        log.info("-" * 110)
        log.info(
            "Total positions: %d  |  sold: %d  failed: %d  skipped: small=%d, low_bid=%d, no_book=%d  |  est proceeds: $%.2f",
            len(positions), sold, failed, skipped_small, skipped_low_bid, skipped_no_book, total_est,
        )
        if not execute:
            log.info("DRY RUN — re-run with --execute to actually sell.")
        return 0
    finally:
        try:
            await data_api.close()
        except Exception:
            pass


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--execute", action="store_true")
    args = p.parse_args()
    return asyncio.run(run(execute=args.execute))


if __name__ == "__main__":
    raise SystemExit(main())
