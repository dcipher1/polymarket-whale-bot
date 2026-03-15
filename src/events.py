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
CHANNEL_NEW_SIGNAL = "whale:new_signal"
CHANNEL_SIGNAL_PROMOTED = "whale:signal_promoted"
