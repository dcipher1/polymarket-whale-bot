"""Thin wrapper around py-clob-client for CLOB API interactions."""

import logging
from typing import Any

import aiohttp

from src.config import settings
from src.events import cache_get, cache_set

logger = logging.getLogger(__name__)

CLOB_BASE_URL = "https://clob.polymarket.com"


class CLOBClient:
    """Async CLOB API client for market data (no auth needed for reads)."""

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def get_price(self, token_id: str) -> dict[str, float] | None:
        """Get price for a token. Uses midpoint endpoint (most reliable)."""
        cache_key = f"clob:price:{token_id}"
        cached = await cache_get(cache_key)
        if cached:
            return cached

        try:
            session = await self._get_session()
            # Use midpoint — most reliable endpoint
            async with session.get(
                f"{CLOB_BASE_URL}/midpoint", params={"token_id": token_id}
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    mid = float(data.get("mid", 0))
                    result = {"bid": mid, "ask": mid, "mid": mid}
                    await cache_set(cache_key, result, ex=5)
                    return result
        except Exception as e:
            logger.warning("Price fetch failed for %s: %s", token_id, e)
        return None

    async def get_midpoint(self, token_id: str) -> float | None:
        """Get midpoint price for a token."""
        try:
            session = await self._get_session()
            async with session.get(
                f"{CLOB_BASE_URL}/midpoint", params={"token_id": token_id}
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data.get("mid", 0))
        except Exception as e:
            logger.warning("Midpoint fetch failed for %s: %s", token_id, e)
        return None

    async def get_orderbook(self, token_id: str) -> dict | None:
        """Get full orderbook for a token."""
        try:
            session = await self._get_session()
            async with session.get(
                f"{CLOB_BASE_URL}/book", params={"token_id": token_id}
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
        except Exception as e:
            logger.warning("Orderbook fetch failed for %s: %s", token_id, e)
        return None
