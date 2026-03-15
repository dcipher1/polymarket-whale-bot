"""Per-market fee detection and calculation."""

import logging
from datetime import datetime, timezone
from decimal import Decimal

import aiohttp
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.events import cache_get, cache_set
from src.models import MarketToken

logger = logging.getLogger(__name__)

CLOB_BASE_URL = "https://clob.polymarket.com"


class FeeInfo:
    def __init__(self, has_taker_fees: bool, taker_rate: float, maker_rate: float = 0.0):
        self.has_taker_fees = has_taker_fees
        self.taker_rate = taker_rate
        self.maker_rate = maker_rate

    def estimate_fee(self, price: float) -> float:
        """Estimate taker fee per contract using formula: 0.0625 * price * (1-price)."""
        if not self.has_taker_fees:
            return 0.0
        return 0.0625 * price * (1.0 - price)


async def get_token_fees(token_id: str, session: AsyncSession | None = None) -> FeeInfo:
    """Look up fee info for a token. Check cache, then DB, then API."""
    # Check cache
    cache_key = f"fees:{token_id}"
    cached = await cache_get(cache_key)
    if cached:
        return FeeInfo(
            has_taker_fees=cached["has_taker_fees"],
            taker_rate=cached["taker_rate"],
        )

    # Check DB
    if session:
        result = await session.execute(
            select(MarketToken).where(MarketToken.token_id == token_id)
        )
        token = result.scalar_one_or_none()
        if token and token.has_taker_fees is not None:
            info = FeeInfo(
                has_taker_fees=token.has_taker_fees,
                taker_rate=float(token.taker_fee_rate or 0),
            )
            await cache_set(cache_key, {
                "has_taker_fees": info.has_taker_fees,
                "taker_rate": info.taker_rate,
            }, ex=86400)
            return info

    # Fetch from API
    info = await _fetch_fee_from_api(token_id)

    # Store in DB
    if session:
        await session.execute(
            update(MarketToken)
            .where(MarketToken.token_id == token_id)
            .values(
                has_taker_fees=info.has_taker_fees,
                taker_fee_rate=Decimal(str(info.taker_rate)),
                fee_last_checked=datetime.now(timezone.utc),
            )
        )
        await session.commit()

    await cache_set(cache_key, {
        "has_taker_fees": info.has_taker_fees,
        "taker_rate": info.taker_rate,
    }, ex=86400)

    return info


async def _fetch_fee_from_api(token_id: str) -> FeeInfo:
    """Fetch fee info from CLOB API."""
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            async with session.get(
                f"{CLOB_BASE_URL}/fee-rate",
                params={"token_id": token_id},
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    has_fees = data.get("feeEnabled", data.get("feesEnabled", False))
                    rate = float(data.get("takerRate", data.get("taker_rate", 0)))
                    logger.info("Fee lookup for token %s: fees=%s rate=%s", token_id, has_fees, rate)
                    return FeeInfo(has_taker_fees=bool(has_fees), taker_rate=rate)
                else:
                    logger.warning("Fee lookup returned %d for token %s", resp.status, token_id)
    except Exception as e:
        logger.warning("Fee lookup failed for token %s: %s", token_id, e)

    # Default to assuming fees exist (fail-closed in strict mode)
    return FeeInfo(has_taker_fees=True, taker_rate=0.0625)
