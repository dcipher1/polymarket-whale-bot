"""Geoblock check for Polymarket access."""

import logging

import aiohttp

logger = logging.getLogger(__name__)

GEOBLOCK_URL = "https://polymarket.com/api/geoblock"


async def check_geoblock() -> bool:
    """Check if current IP is geoblocked by Polymarket.

    Returns True if BLOCKED, False if ALLOWED.
    """
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            async with session.get(GEOBLOCK_URL) as resp:
                if resp.status != 200:
                    logger.error("Geoblock check returned status %d", resp.status)
                    return True  # fail-closed

                data = await resp.json()
                blocked = data.get("blocked", True)
                country = data.get("country", "unknown")

                if blocked:
                    logger.error(
                        "GEOBLOCKED: Polymarket access blocked from country=%s", country
                    )
                else:
                    logger.info(
                        "Geoblock check passed: country=%s, access allowed", country
                    )

                return blocked
    except Exception as e:
        logger.error("Geoblock check failed with error: %s", e)
        return True  # fail-closed
