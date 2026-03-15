"""Discover new whale wallet candidates from Polymarket leaderboard."""

import asyncio
import json
import logging
from pathlib import Path

import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

LEADERBOARD_URL = "https://data-api.polymarket.com/v1/leaderboard"


async def discover():
    """Fetch top traders from leaderboard and save as seed candidates."""
    async with aiohttp.ClientSession() as session:
        candidates = []

        for page in range(1, 26):  # Up to 25 pages
            try:
                async with session.get(
                    LEADERBOARD_URL,
                    params={
                        "metric": "profit",
                        "timeframe": "all",
                        "page": page,
                        "limit": 20,
                        "category": "overall",
                    },
                ) as resp:
                    if resp.status != 200:
                        logger.warning("Leaderboard returned %d at page %d", resp.status, page)
                        break
                    data = await resp.json()

                    if not data:
                        break

                    items = data if isinstance(data, list) else data.get("leaderboard", [])
                    if not items:
                        break

                    for entry in items:
                        addr = entry.get("proxyWallet", entry.get("address", entry.get("user", "")))
                        pnl = float(entry.get("pnl", entry.get("profit", 0)))
                        volume = float(entry.get("vol", entry.get("volume", 0)))
                        name = entry.get("userName", "")

                        if addr and pnl > 10000:
                            candidates.append({
                                "address": addr,
                                "display_name": name,
                                "pnl": pnl,
                                "volume": volume,
                            })

                    logger.info("Page %d: got %d entries", page, len(items))
                    await asyncio.sleep(0.3)  # rate limit
            except Exception as e:
                logger.error("Failed to fetch leaderboard at page %d: %s", page, e)

    # Deduplicate by address
    seen = set()
    unique = []
    for c in candidates:
        if c["address"] not in seen:
            seen.add(c["address"])
            unique.append(c)
    candidates = unique

    # Sort by P&L
    candidates.sort(key=lambda x: x["pnl"], reverse=True)

    output = Path("data/discovered_wallets.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w") as f:
        json.dump(candidates, f, indent=2)

    logger.info("Discovered %d wallet candidates, saved to %s", len(candidates), output)

    # Print top 20
    for i, c in enumerate(candidates[:20]):
        logger.info(
            "  #%d: %s (%s) P&L=$%,.0f Vol=$%,.0f",
            i + 1, c["address"][:10], c.get("display_name", ""), c["pnl"], c["volume"],
        )


if __name__ == "__main__":
    asyncio.run(discover())
