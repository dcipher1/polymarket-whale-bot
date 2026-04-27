"""Thin wrapper around py-clob-client for CLOB API interactions."""

import logging
import math
from typing import Any

import aiohttp

from src.config import settings
from src.events import cache_get, cache_set

logger = logging.getLogger(__name__)

CLOB_BASE_URL = "https://clob.polymarket.com"


def _coerce_midpoint(value: Any) -> float | None:
    try:
        mid = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(mid) or math.isinf(mid) or mid < 0 or mid > 1:
        return None
    return mid


def _parse_midpoints_response(data: Any) -> dict[str, float | None]:
    """Normalize CLOB /midpoints response shapes into token_id -> midpoint."""
    parsed: dict[str, float | None] = {}
    if isinstance(data, dict):
        for token_id, value in data.items():
            if isinstance(value, dict):
                value = value.get("mid") or value.get("price")
            parsed[str(token_id)] = _coerce_midpoint(value)
        return parsed
    if isinstance(data, list):
        for item in data:
            if not isinstance(item, dict):
                continue
            token_id = item.get("token_id") or item.get("asset_id") or item.get("asset")
            if not token_id:
                continue
            parsed[str(token_id)] = _coerce_midpoint(item.get("mid") or item.get("price"))
    return parsed


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

    async def get_midpoints(self, token_ids: list[str]) -> dict[str, float | None]:
        """Get fresh midpoint prices for a batch of token IDs."""
        unique_ids = sorted({str(token_id) for token_id in token_ids if token_id})
        if not unique_ids:
            return {}
        try:
            session = await self._get_session()
            body = [{"token_id": token_id} for token_id in unique_ids]
            async with session.post(f"{CLOB_BASE_URL}/midpoints", json=body) as resp:
                if resp.status != 200:
                    logger.warning("Batch midpoint fetch failed with status %s", resp.status)
                    return {token_id: None for token_id in unique_ids}
                data = await resp.json()
                parsed = _parse_midpoints_response(data)
                return {token_id: parsed.get(token_id) for token_id in unique_ids}
        except Exception as e:
            logger.warning("Batch midpoint fetch failed for %d tokens: %s", len(unique_ids), e)
            return {token_id: None for token_id in unique_ids}

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
