"""One-time fee backfill for tokens with whale trades but NULL fee info."""

import asyncio
import logging

from sqlalchemy import select, and_, func

from src.db import async_session
from src.models import MarketToken, WhaleTrade
from src.polymarket.fees import get_token_fees

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def backfill_fees():
    """Fetch fee data for all tokens that have whale trades but missing fee info."""
    async with async_session() as session:
        # Find tokens with whale activity but NULL fee info
        whale_tokens = await session.execute(
            select(MarketToken).where(
                and_(
                    MarketToken.has_taker_fees.is_(None),
                    MarketToken.condition_id.in_(
                        select(WhaleTrade.condition_id.distinct())
                    ),
                )
            )
        )
        tokens = whale_tokens.scalars().all()

        logger.info("Found %d tokens with whale trades but missing fee info", len(tokens))

        filled = 0
        errors = 0
        for i, token in enumerate(tokens):
            try:
                await get_token_fees(token.token_id, session)
                filled += 1
                if (i + 1) % 50 == 0:
                    logger.info("Progress: %d/%d tokens processed", i + 1, len(tokens))
                    await session.commit()
            except Exception as e:
                errors += 1
                logger.debug("Fee fetch failed for %s: %s", token.token_id[:10], e)

            # Rate limit
            await asyncio.sleep(0.3)

        await session.commit()
        logger.info("Fee backfill complete: %d filled, %d errors out of %d tokens", filled, errors, len(tokens))


if __name__ == "__main__":
    asyncio.run(backfill_fees())
