"""Auto-discover new whales from Polymarket category leaderboards."""

import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal

import aiohttp
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from src.db import async_session
from src.models import Wallet, WalletCategoryScore
from src.indexer.wallet_ingester import ingest_wallet
from src.indexer.position_builder import build_positions_for_wallet
from src.polymarket.data_api import DataAPIClient

logger = logging.getLogger(__name__)

LEADERBOARD_URL = "https://data-api.polymarket.com/v1/leaderboard"

# Categories to scan and their mapping to our internal categories
CATEGORY_MAP = {
    "politics": "politics",
    "crypto": "crypto_weekly",
    "economics": "macro",
}

# Minimum category P&L to consider a wallet
MIN_CATEGORY_PNL = 5000


async def discover_and_ingest() -> int:
    """Scan category leaderboards for new whales, ingest their data.
    Returns count of new whales added.
    """
    # Get existing wallet addresses
    async with async_session() as session:
        result = await session.execute(select(Wallet.address))
        existing = {r[0] for r in result.all()}

    logger.info("Scanning category leaderboards for new whales (%d already tracked)", len(existing))

    # Discover candidates from each category
    candidates = []
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as http:
        for api_cat, our_cat in CATEGORY_MAP.items():
            for page in range(1, 11):
                try:
                    async with http.get(LEADERBOARD_URL, params={
                        "metric": "profit", "timeframe": "all",
                        "page": page, "limit": 20, "category": api_cat,
                    }) as resp:
                        if resp.status != 200:
                            break
                        data = await resp.json()
                        items = data if isinstance(data, list) else []
                        if not items:
                            break
                        for entry in items:
                            addr = entry.get("proxyWallet", "")
                            pnl = float(entry.get("pnl", 0))
                            name = entry.get("userName", "")
                            if addr and pnl >= MIN_CATEGORY_PNL and addr not in existing:
                                candidates.append({
                                    "address": addr,
                                    "name": name,
                                    "category_pnl": pnl,
                                    "category": our_cat,
                                })
                except Exception as e:
                    logger.warning("Leaderboard fetch failed for %s page %d: %s", api_cat, page, e)
                    break
                await asyncio.sleep(0.2)

    # Dedupe by address
    seen = set()
    unique = []
    for c in candidates:
        if c["address"] not in seen:
            seen.add(c["address"])
            unique.append(c)

    if not unique:
        logger.info("No new whales found")
        return 0

    logger.info("Found %d new whale candidates, ingesting...", len(unique))

    data_client = DataAPIClient()
    added = 0

    for c in unique:
        addr = c["address"]
        try:
            # Ingest trades
            trades = await ingest_wallet(addr)
            positions = await build_positions_for_wallet(addr)

            # Pull closed-position P&L
            closed = await data_client.get_closed_positions(addr)
            total_pnl = sum(float(p.realized_pnl or 0) for p in closed)
            wins = sum(1 for p in closed if float(p.realized_pnl or 0) > 0)
            losses = sum(1 for p in closed if float(p.realized_pnl or 0) < 0)
            total = wins + losses

            # Update wallet record
            async with async_session() as session:
                wallet = await session.get(Wallet, addr)
                if wallet:
                    wallet.display_name = c["name"]
                    wallet.total_pnl_usdc = Decimal(str(round(total_pnl, 2)))

                    # Set category score
                    if total > 0:
                        wr = round(wins / total, 4)
                        pf = min(99, round(total_pnl / max(abs(total_pnl - sum(
                            float(p.realized_pnl or 0) for p in closed if float(p.realized_pnl or 0) > 0
                        )), 1), 2))
                    else:
                        wr = 0.60
                        pf = 2.0

                    stmt = insert(WalletCategoryScore).values(
                        wallet_address=addr,
                        category=c["category"],
                        win_rate=Decimal(str(wr)),
                        profit_factor=Decimal(str(min(pf, 99))),
                        expectancy=Decimal(str(round(total_pnl / max(total, 1), 2))),
                        trade_count=max(total, 10),
                        followability=Decimal("0.70"),
                        followability_provisional=True,
                        last_updated=datetime.now(timezone.utc),
                    ).on_conflict_do_nothing()
                    await session.execute(stmt)

                    # Classify
                    if total_pnl >= 50000:
                        wallet.copyability_class = "COPYABLE"
                        wallet.conviction_score = max(wallet.conviction_score, 45)
                    elif total_pnl >= 10000:
                        wallet.copyability_class = "WATCH"

                    await session.commit()

            added += 1
            logger.info(
                "  NEW WHALE: %s (%s) %dt %dp PnL=$%.0f %dW/%dL -> %s",
                c["name"][:20], c["category"], trades, positions,
                total_pnl, wins, losses,
                "COPYABLE" if total_pnl >= 50000 else "WATCH",
            )
        except Exception as e:
            logger.warning("  Failed to ingest %s: %s", addr[:12], str(e)[:80])

    await data_client.close()
    logger.info("Discovery complete: %d new whales added", added)

    try:
        from src.monitoring.telegram import send_alert
        if added > 0:
            await send_alert(f"Whale discovery: {added} new whales added from category leaderboards")
    except Exception:
        pass

    return added
