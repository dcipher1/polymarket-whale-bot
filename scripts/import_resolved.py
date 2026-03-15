"""Import resolved markets and compute whale P&L."""

import asyncio
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal

import aiohttp
from sqlalchemy import update, and_, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


async def import_resolved(max_pages: int = 100):
    from src.db import async_session
    from src.models import Market, MarketToken, WhalePosition
    from src.indexer.market_classifier import classify_market

    # Step 1: Fetch closed markets
    logger.info("Fetching closed markets from Gamma API...")
    all_closed = []
    async with aiohttp.ClientSession() as session:
        for offset in range(0, max_pages * 100, 100):
            try:
                async with session.get(
                    "https://gamma-api.polymarket.com/markets",
                    params={"closed": "true", "active": "false", "limit": 100, "offset": offset},
                ) as resp:
                    if resp.status != 200:
                        break
                    data = await resp.json()
                    if not data:
                        break
                    all_closed.extend(data)
                    if len(data) < 100:
                        break
            except Exception as e:
                logger.error("Failed at offset %d: %s", offset, e)
                break
            await asyncio.sleep(0.15)

    logger.info("Fetched %d closed markets", len(all_closed))

    # Step 2: Process each market individually
    resolved_count = 0
    errors = 0

    for idx, m in enumerate(all_closed):
        condition_id = m.get("conditionId", "")
        if not condition_id:
            continue

        try:
            async with async_session() as db:
                question = m.get("question", "")
                outcomes_str = m.get("outcomes", "[]")
                prices_str = m.get("outcomePrices", "[]")
                clob_tokens_str = m.get("clobTokenIds", "[]")
                end_date = m.get("endDate", "")
                volume = m.get("volume", "0")
                created_at_str = m.get("createdAt", "")

                # Parse
                try:
                    outcomes = json.loads(outcomes_str) if isinstance(outcomes_str, str) else outcomes_str
                    prices = json.loads(prices_str) if isinstance(prices_str, str) else prices_str
                    token_ids = json.loads(clob_tokens_str) if isinstance(clob_tokens_str, str) else clob_tokens_str
                except Exception:
                    continue

                # Determine winner
                winner_outcome = None
                if prices and outcomes and len(prices) == len(outcomes):
                    try:
                        float_prices = [float(p) for p in prices]
                        max_price = max(float_prices)
                        if max_price >= 0.90:
                            winner_idx = float_prices.index(max_price)
                            wo = outcomes[winner_idx].upper()
                            winner_outcome = wo if wo in ("YES", "NO") else ("YES" if winner_idx == 0 else "NO")
                    except Exception:
                        pass

                # Parse dates
                resolution_time = None
                if end_date:
                    try:
                        resolution_time = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                    except Exception:
                        pass

                created_at = None
                if created_at_str:
                    try:
                        created_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
                    except Exception:
                        pass

                # Classify
                result = classify_market(question)

                # Upsert market via raw SQL to avoid ORM issues
                await db.execute(text("""
                    INSERT INTO markets (condition_id, question, slug, category, classification_source,
                                        resolution_time, resolved, outcome, volume_usdc, created_at, last_updated, meta)
                    VALUES (:cid, :q, :slug, :cat, 'auto', :res_time, :resolved, :outcome, :vol, :created, NOW(), '{}')
                    ON CONFLICT (condition_id) DO UPDATE SET
                        resolved = :resolved, outcome = :outcome, volume_usdc = :vol, last_updated = NOW()
                """), {
                    "cid": condition_id, "q": question, "slug": m.get("slug", ""),
                    "cat": result.category, "res_time": resolution_time,
                    "resolved": True, "outcome": winner_outcome,
                    "vol": Decimal(str(volume)) if volume else None,
                    "created": created_at,
                })

                # Upsert tokens
                for i, token_id in enumerate(token_ids):
                    if not token_id:
                        continue
                    outcome = "YES" if i == 0 else "NO"
                    try:
                        await db.execute(text("""
                            INSERT INTO market_tokens (token_id, condition_id, outcome)
                            VALUES (:tid, :cid, :outcome)
                            ON CONFLICT (token_id) DO NOTHING
                        """), {"tid": token_id, "cid": condition_id, "outcome": outcome})
                    except Exception:
                        pass  # skip duplicate tokens

                if winner_outcome:
                    resolved_count += 1

                await db.commit()

        except Exception as e:
            errors += 1

        if (idx + 1) % 1000 == 0:
            logger.info("  Processed %d/%d (resolved=%d, errors=%d)", idx + 1, len(all_closed), resolved_count, errors)

    logger.info("Done: %d resolved markets imported, %d errors, %d total", resolved_count, errors, len(all_closed))

    # Step 3: Re-ingest wallet trades on new markets, then re-score
    logger.info("Re-ingesting wallet trades on newly imported markets...")
    from src.indexer.wallet_ingester import ingest_wallet
    from src.indexer.position_builder import build_positions_for_wallet
    from sqlalchemy import select
    from src.models import Wallet

    async with async_session() as db:
        result = await db.execute(select(Wallet.address))
        addresses = [r[0] for r in result.all()]

    new_total = 0
    for addr in addresses:
        try:
            n = await ingest_wallet(addr)
            if n > 0:
                await build_positions_for_wallet(addr)
                new_total += n
        except Exception:
            pass

    logger.info("Re-ingested %d new trades across %d wallets", new_total, len(addresses))

    # Step 4: Re-score
    logger.info("Re-scoring all wallets...")
    from src.scorer.wallet_scorer import score_all_wallets
    results = await score_all_wallets()
    for r in results:
        if r:
            best = r.get("best_category") or "-"
            logger.info("  %s conv=%d class=%s best=%s",
                        r["address"][:12], r["conviction_score"], r["copyability"], best)


if __name__ == "__main__":
    asyncio.run(import_resolved())
