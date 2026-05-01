"""Repair whale_trades rows with bogus cost basis (price > 1.0).

Background: PolyNode's websocket stream emits one event per individual fill
within a multi-fill trade. With the old `on_conflict_do_nothing` ingester
behavior, the first partial fill landed in the DB (wrong num_contracts) and
the data-api's aggregated row was silently dropped. The current
wallet_ingester now upserts (data-api truth wins), but pre-existing rows are
still corrupt.

This script re-ingests every wallet that has at least one row with price > 1.
Idempotent: safe to re-run.

Usage:
    .venv/bin/python scripts/repair_whale_trades.py
"""

import asyncio
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

from sqlalchemy import text

from src.db import async_session
from src.indexer.wallet_ingester import ingest_wallet
from src.polymarket.data_api import DataAPIClient


async def main() -> int:
    async with async_session() as session:
        before = await session.execute(text(
            "SELECT COUNT(*) AS n, COUNT(DISTINCT wallet_address) AS w "
            "FROM whale_trades WHERE price > 1.0"
        ))
        row = before.first()
        print(f"BEFORE: {row.n} corrupt rows across {row.w} wallets")

        wallets_q = await session.execute(text(
            "SELECT DISTINCT wallet_address FROM whale_trades WHERE price > 1.0"
        ))
        wallets = [r[0] for r in wallets_q.all()]

    if not wallets:
        print("Nothing to repair.")
        return 0

    print(f"Repairing {len(wallets)} wallets...")
    client = DataAPIClient()
    try:
        for addr in wallets:
            n = await ingest_wallet(addr, full_backfill=True, client=client)
            print(f"  {addr[:12]}: re-ingested ({n} new rows, rest upserted)")
    finally:
        await client.close()

    async with async_session() as session:
        after = await session.execute(text(
            "SELECT COUNT(*) AS n FROM whale_trades WHERE price > 1.0"
        ))
        n_after = after.first().n
        print(f"AFTER: {n_after} corrupt rows remain")

    return 0 if n_after == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
