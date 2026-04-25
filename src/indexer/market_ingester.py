"""Market ingestion from Gamma API with classification and fee detection."""

import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select, and_, func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.db import async_session
from src.config import settings
from src.indexer.market_classifier import classify_market
from src.models import Market, MarketToken, WhaleTrade
from src.polymarket.gamma_api import GammaAPIClient, GammaMarket
from src.polymarket.fees import get_token_fees

logger = logging.getLogger(__name__)

# In-memory cache to avoid repeated DB/API hits within a cycle
_market_cache: dict[str, Market | None] = {}
# condition_id → slug mapping learned from Data API positions
_slug_cache: dict[str, str] = {}
_shared_gamma: GammaAPIClient | None = None


async def _get_gamma() -> GammaAPIClient:
    """Get or create a shared Gamma API client."""
    global _shared_gamma
    if _shared_gamma is None:
        _shared_gamma = GammaAPIClient()
    return _shared_gamma


async def ensure_market(condition_id: str, session: AsyncSession | None = None, slug: str | None = None) -> Market | None:
    """Get a market from DB, or fetch from Gamma API on-demand.

    This is the primary way to get market data. Replaces the bulk 35K fetch.
    ``slug`` is the Polymarket market slug from the Data API; when provided it
    enables reliable Gamma API lookups and is cached for future use.
    """
    if not condition_id:
        return None

    # Store slug in the cross-caller cache
    if slug:
        _slug_cache[condition_id] = slug
    elif condition_id in _slug_cache:
        slug = _slug_cache[condition_id]

    # Check in-memory cache first (skip cached None — those were from broken lookups)
    cached = _market_cache.get(condition_id)
    if cached is not None:
        return cached

    owns_session = session is None
    if owns_session:
        session = async_session()
        await session.__aenter__()

    try:
        # Check DB
        market = await session.get(Market, condition_id)
        if market:
            _market_cache[condition_id] = market
            return market

        # Fetch from Gamma API — pass slug for reliable lookup
        gamma = await _get_gamma()
        try:
            gm = await gamma.get_market(condition_id, slug=slug)
        except Exception as e:
            if "422" in str(e) or "Unprocessable" in str(e):
                # Don't cache None permanently — slug may arrive later
                return None
            raise

        if not gm:
            # Don't cache None permanently — slug may arrive later
            return None

        # Fetch event-level tags (Polymarket puts categories on events, not markets)
        event_tags = []
        for event in gm.events:
            eid = event.get("id")
            if eid:
                event_tags = await gamma.get_event_tags(str(eid))
                break

        await _upsert_market(session, gm, event_tags=event_tags)
        await session.commit()

        market = await session.get(Market, condition_id)
        _market_cache[condition_id] = market
        logger.debug("Fetched market %s on-demand: %s", condition_id[:10], gm.question[:50])
        return market
    finally:
        if owns_session:
            await session.__aexit__(None, None, None)


_refresh_sem = asyncio.Semaphore(5)


async def refresh_tracked_markets() -> int:
    """Refresh metadata for markets we're actively tracking. Replaces bulk 35K fetch."""
    gamma = await _get_gamma()

    async with async_session() as session:
        # Get all unresolved markets in our DB
        result = await session.execute(
            select(Market.condition_id).where(Market.resolved == False)
        )
        tracked_ids = [r[0] for r in result.all()]

    if not tracked_ids:
        logger.info("No tracked markets to refresh")
        return 0

    async def _refresh_one(condition_id: str) -> bool:
        async with _refresh_sem:
            try:
                gm = await gamma.get_market(condition_id, slug=_slug_cache.get(condition_id))
                if not gm:
                    return False
                async with async_session() as s:
                    await _upsert_market(s, gm)
                    await s.commit()
                _market_cache.pop(condition_id, None)
                return True
            except Exception as e:
                if "422" in str(e):
                    # Market gone from Gamma — mark resolved
                    async with async_session() as s:
                        market = await s.get(Market, condition_id)
                        if market:
                            meta = dict(market.meta or {})
                            meta["gamma_gone"] = True
                            market.meta = meta
                            await s.commit()
                else:
                    logger.debug("Failed to refresh market %s: %s", condition_id[:10], e)
                return False

    results = await asyncio.gather(*[_refresh_one(cid) for cid in tracked_ids], return_exceptions=True)
    updated = sum(1 for r in results if r is True)

    logger.info("Refreshed %d/%d tracked markets", updated, len(tracked_ids))
    return updated


def clear_market_cache():
    """Clear the in-memory market cache (call between cycles)."""
    _market_cache.clear()


def _parse_volume(volume_str: str) -> Decimal | None:
    try:
        return Decimal(str(volume_str)) if volume_str else None
    except Exception:
        return None


async def _backfill_fees(session: AsyncSession, whale_market_ids: set[str]) -> None:
    """Batch-populate fees for tokens with NULL fee info on markets with whale activity."""
    if not whale_market_ids:
        return

    result = await session.execute(
        select(MarketToken).where(
            and_(
                MarketToken.condition_id.in_(whale_market_ids),
                MarketToken.has_taker_fees.is_(None),
            )
        )
    )
    tokens_needing_fees = result.scalars().all()

    if not tokens_needing_fees:
        return

    logger.info("Backfilling fees for %d tokens with whale activity", len(tokens_needing_fees))
    filled = 0
    for token in tokens_needing_fees:
        try:
            await get_token_fees(token.token_id, session)
            filled += 1
        except Exception as e:
            logger.debug("Fee backfill failed for token %s: %s", token.token_id[:10], e)

    if filled > 0:
        logger.info("Backfilled fees for %d tokens", filled)


async def _upsert_market(session: AsyncSession, gm: GammaMarket, event_tags: list[str] | None = None) -> None:
    tag_labels = gm.get_tag_labels()

    # Merge event-level tags (fetched from /events/{id}) if market has none
    if event_tags and not tag_labels:
        tag_labels = [t.lower() for t in event_tags]
    elif event_tags:
        existing = set(tag_labels)
        for t in event_tags:
            if t.lower() not in existing:
                tag_labels.append(t.lower())

    # Classify — prefer Polymarket's own event category, fall back to keywords
    result = classify_market(
        question=gm.question,
        tags=tag_labels,
        polymarket_category=gm.get_event_category(),
    )

    # Parse end date
    resolution_time = None
    if gm.end_date_iso:
        try:
            resolution_time = datetime.fromisoformat(gm.end_date_iso.replace("Z", "+00:00"))
        except (ValueError, TypeError) as e:
            logger.debug("Failed to parse end_date for %s: %s", gm.condition_id[:10], e)

    # Parse volume
    volume = _parse_volume(gm.volume)

    # Parse liquidity and volume_24hr from Gamma API
    liquidity = None
    if gm.liquidity_num > 0:
        liquidity = Decimal(str(round(gm.liquidity_num, 2)))

    volume_24hr = None
    if gm.volume_24hr > 0:
        volume_24hr = Decimal(str(round(gm.volume_24hr, 2)))

    # Parse created_at
    created_at = None
    if gm.created_at:
        try:
            created_at = datetime.fromisoformat(gm.created_at.replace("Z", "+00:00"))
        except (ValueError, TypeError) as e:
            logger.debug("Failed to parse created_at for %s: %s", gm.condition_id[:10], e)

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
        liquidity_usdc=liquidity,
        volume_24hr_usdc=volume_24hr,
        created_at=created_at,
        last_updated=datetime.now(timezone.utc),
    ).on_conflict_do_update(
        index_elements=["condition_id"],
        set_={
            "question": gm.question,
            "slug": gm.slug,
            "resolved": gm.closed,
            "volume_usdc": volume,
            "liquidity_usdc": liquidity,
            "volume_24hr_usdc": volume_24hr,
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
