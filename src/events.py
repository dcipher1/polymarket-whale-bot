import json
import logging
from typing import Any

import redis.asyncio as redis

from src.config import settings

logger = logging.getLogger(__name__)

_redis: redis.Redis | None = None


async def get_redis() -> redis.Redis:
    global _redis
    if _redis is None:
        _redis = redis.from_url(settings.redis_url, decode_responses=True)
    return _redis


async def publish(channel: str, data: dict[str, Any]) -> None:
    r = await get_redis()
    await r.publish(channel, json.dumps(data, default=str))
    logger.debug("Published to %s: %s", channel, data)


async def subscribe(channel: str):
    r = await get_redis()
    pubsub = r.pubsub()
    await pubsub.subscribe(channel)
    return pubsub


async def cache_set(key: str, value: Any, ex: int | None = None) -> None:
    r = await get_redis()
    await r.set(key, json.dumps(value, default=str), ex=ex)


async def cache_get(key: str) -> Any | None:
    r = await get_redis()
    val = await r.get(key)
    if val is not None:
        return json.loads(val)
    return None


# Event channels
CHANNEL_NEW_WHALE_TRADE = "whale:new_trade"

# --- Wallet balance helpers ---
BALANCE_CACHE_KEY = "wallet:usdc_balance"
BALANCE_CACHE_TTL = 300  # 5 minutes


async def update_cached_balance(balance: float) -> None:
    """Cache the current USDC wallet balance."""
    from datetime import datetime, timezone
    await cache_set(BALANCE_CACHE_KEY, {
        "balance": round(balance, 2),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }, ex=BALANCE_CACHE_TTL)


async def get_deployable_capital() -> float:
    """Get deployable capital: cached wallet balance minus reserve.

    Falls back to starting_capital if cache is empty (e.g. Redis down, first boot).
    """
    cached = await cache_get(BALANCE_CACHE_KEY)
    if cached and "balance" in cached:
        balance = float(cached["balance"])
        deployable = balance - settings.wallet_reserve_usdc
        return max(deployable, 0)

    # Fallback: use static config (legacy behavior)
    logger.debug("No cached balance — falling back to starting_capital")
    return settings.starting_capital * settings.max_total_exposure_pct
