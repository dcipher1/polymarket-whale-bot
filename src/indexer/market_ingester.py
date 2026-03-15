"""Market ingestion from Gamma API with classification and fee detection."""

import logging
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.db import async_session
from src.indexer.market_classifier import classify_market
from src.models import Market, MarketToken
from src.polymarket.gamma_api import GammaAPIClient, GammaMarket
from src.polymarket.fees import get_token_fees

logger = logging.getLogger(__name__)


async def ingest_markets() -> int:
    """Fetch all active markets, classify, store, and check fees. Returns count."""
    gamma = GammaAPIClient()
    try:
        markets = await gamma.get_all_active_markets()
        logger.info("Fetched %d markets from Gamma API", len(markets))

        count = 0
        async with async_session() as session:
            for market in markets:
                try:
                    await _upsert_market(session, market)
                    count += 1
                except Exception as e:
                    logger.error("Failed to ingest market %s: %s", market.condition_id, e)

            await session.commit()

        logger.info("Ingested %d markets", count)
        return count
    finally:
        await gamma.close()


async def _upsert_market(session: AsyncSession, gm: GammaMarket) -> None:
    tag_labels = gm.get_tag_labels()

    # Classify
    result = classify_market(
        question=gm.question,
        tags=tag_labels,
    )

    # Parse end date
    resolution_time = None
    if gm.end_date_iso:
        try:
            resolution_time = datetime.fromisoformat(gm.end_date_iso.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass

    # Parse volume
    volume = None
    try:
        volume = Decimal(str(gm.volume)) if gm.volume else None
    except Exception:
        pass

    # Parse created_at
    created_at = None
    if gm.created_at:
        try:
            created_at = datetime.fromisoformat(gm.created_at.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass

    # Upsert market
    stmt = insert(Market).values(
        condition_id=gm.condition_id,
        question=gm.question,
        slug=gm.slug,
        category=result.category,
        category_matched_keywords=result.matched_keywords or None,
        category_matched_tags=result.matched_tags or None,
        classification_source=result.source,
        resolution_time=resolution_time,
        resolved=gm.closed,
        tags=tag_labels or None,
        volume_usdc=volume,
        created_at=created_at,
        last_updated=datetime.now(timezone.utc),
    ).on_conflict_do_update(
        index_elements=["condition_id"],
        set_={
            "question": gm.question,
            "slug": gm.slug,
            "resolved": gm.closed,
            "volume_usdc": volume,
            "last_updated": datetime.now(timezone.utc),
            "tags": tag_labels or None,
        },
    )
    await session.execute(stmt)

    # Upsert tokens — pair clobTokenIds with outcomes
    outcomes_list = gm.get_outcomes_list()
    token_ids = gm.get_token_ids()

    for i, token_id in enumerate(token_ids):
        if not token_id:
            continue
        # Map outcome: first token = first outcome (usually Yes), second = second (usually No)
        outcome = outcomes_list[i].upper() if i < len(outcomes_list) else ("YES" if i == 0 else "NO")
        if outcome not in ("YES", "NO"):
            outcome = "YES" if i == 0 else "NO"

        token_stmt = insert(MarketToken).values(
            token_id=token_id,
            condition_id=gm.condition_id,
            outcome=outcome,
        ).on_conflict_do_nothing(index_elements=["token_id"])
        await session.execute(token_stmt)
