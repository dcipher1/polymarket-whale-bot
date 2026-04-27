"""Backfill market/token metadata for whale_trades before FK validation."""

import argparse
import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy import text, select
from sqlalchemy.dialects.postgresql import insert

from src.db import async_session
from src.indexer.market_ingester import ensure_market, close_shared_gamma
from src.models import MarketToken, WhaleEventBacklog, WhaleTrade

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FK_CONSTRAINTS = (
    ("whale_trades", "fk_whale_trades_condition_id_markets"),
    ("whale_trades", "fk_whale_trades_token_id_market_tokens"),
    ("market_tokens", "fk_market_tokens_condition_id_markets"),
    ("whale_positions", "fk_whale_positions_condition_id_markets"),
    ("my_trades", "fk_my_trades_condition_id_markets"),
)


async def _orphan_counts() -> dict[str, int]:
    sql = text(
        """
        SELECT 'whale_trades_missing_market', count(*)
        FROM whale_trades wt
        LEFT JOIN markets m ON m.condition_id = wt.condition_id
        WHERE m.condition_id IS NULL
        UNION ALL
        SELECT 'whale_trades_missing_token', count(*)
        FROM whale_trades wt
        LEFT JOIN market_tokens mt ON mt.token_id = wt.token_id
        WHERE mt.token_id IS NULL
        UNION ALL
        SELECT 'market_tokens_missing_market', count(*)
        FROM market_tokens mt
        LEFT JOIN markets m ON m.condition_id = mt.condition_id
        WHERE m.condition_id IS NULL
        UNION ALL
        SELECT 'whale_positions_missing_market', count(*)
        FROM whale_positions wp
        LEFT JOIN markets m ON m.condition_id = wp.condition_id
        WHERE m.condition_id IS NULL
        UNION ALL
        SELECT 'my_trades_missing_market', count(*)
        FROM my_trades t
        LEFT JOIN markets m ON m.condition_id = t.condition_id
        WHERE m.condition_id IS NULL
        """
    )
    async with async_session() as session:
        rows = (await session.execute(sql)).fetchall()
    return {row[0]: int(row[1] or 0) for row in rows}


async def _fetch_orphans(limit: int) -> list[WhaleTrade]:
    async with async_session() as session:
        result = await session.execute(
            select(WhaleTrade)
            .where(
                text(
                    """
                    NOT EXISTS (
                        SELECT 1 FROM markets m
                        WHERE m.condition_id = whale_trades.condition_id
                    )
                    OR NOT EXISTS (
                        SELECT 1 FROM market_tokens mt
                        WHERE mt.token_id = whale_trades.token_id
                    )
                    """
                )
            )
            .order_by(WhaleTrade.timestamp.desc())
            .limit(limit)
        )
        return list(result.scalars().all())


async def _backlog_trade(session, trade: WhaleTrade, reason: str) -> None:
    raw = {
        "source": "historical_whale_trade",
        "whale_trade_id": trade.id,
        "wallet_address": trade.wallet_address,
        "condition_id": trade.condition_id,
        "token_id": trade.token_id,
        "outcome": trade.outcome,
        "side": trade.side,
        "price": str(trade.price),
        "num_contracts": str(trade.num_contracts),
        "tx_hash": trade.tx_hash,
        "timestamp": trade.timestamp.isoformat() if trade.timestamp else None,
    }
    stmt = insert(WhaleEventBacklog).values(
        provider="historical_backfill",
        wallet_address=trade.wallet_address,
        condition_id=trade.condition_id,
        token_id=trade.token_id,
        outcome=trade.outcome,
        side=trade.side,
        tx_hash=trade.tx_hash or f"whale_trade:{trade.id}",
        reason=reason,
        raw_event=raw,
        created_at=datetime.now(timezone.utc),
        last_attempt_at=datetime.now(timezone.utc),
        attempts=1,
    ).on_conflict_do_update(
        constraint="uq_whale_event_backlog_dedup",
        set_={
            "reason": reason,
            "raw_event": raw,
            "last_attempt_at": datetime.now(timezone.utc),
            "attempts": WhaleEventBacklog.attempts + 1,
            "resolved_at": None,
        },
    )
    await session.execute(stmt)


async def _resolve_backlog(session, trade: WhaleTrade) -> None:
    await session.execute(
        text(
            """
            UPDATE whale_event_backlog
            SET resolved_at = now()
            WHERE resolved_at IS NULL
              AND condition_id = :condition_id
              AND token_id = :token_id
              AND (
                  tx_hash = :tx_hash
                  OR raw_event->>'whale_trade_id' = :trade_id
              )
            """
        ),
        {
            "condition_id": trade.condition_id,
            "token_id": trade.token_id,
            "tx_hash": trade.tx_hash,
            "trade_id": str(trade.id),
        },
    )


async def _resolve_hydrated_backlog(apply: bool) -> int:
    """Close backlog rows once their market and token metadata now exist."""
    async with async_session() as session:
        result = await session.execute(
            text(
                """
                UPDATE whale_event_backlog b
                SET resolved_at = now()
                WHERE resolved_at IS NULL
                  AND EXISTS (
                      SELECT 1 FROM markets m
                      WHERE m.condition_id = b.condition_id
                  )
                  AND EXISTS (
                      SELECT 1 FROM market_tokens mt
                      WHERE mt.token_id = b.token_id
                  )
                """
            )
        )
        count = int(result.rowcount or 0)
        if apply:
            await session.commit()
        else:
            await session.rollback()
        return count


async def _backfill_once(limit: int, apply: bool) -> dict[str, int]:
    trades = await _fetch_orphans(limit)
    stats = {"checked": len(trades), "hydrated": 0, "backlogged": 0, "token_inserted": 0}
    market_cache: dict[str, bool] = {}
    token_conflicts: set[str] = set()
    async with async_session() as session:
        for trade in trades:
            market_ok = market_cache.get(trade.condition_id)
            if market_ok is None:
                market = await ensure_market(trade.condition_id, session)
                market_ok = market is not None
                market_cache[trade.condition_id] = market_ok
            if not market_ok:
                stats["backlogged"] += 1
                await _backlog_trade(session, trade, "market_hydration_failed")
                continue

            if trade.token_id in token_conflicts:
                stats["backlogged"] += 1
                await _backlog_trade(session, trade, "token_mapping_conflict")
                continue

            token = await session.get(MarketToken, trade.token_id)
            if not token:
                stmt = insert(MarketToken).values(
                    token_id=trade.token_id,
                    condition_id=trade.condition_id,
                    outcome=trade.outcome,
                ).on_conflict_do_nothing(index_elements=["token_id"])
                result = await session.execute(stmt)
                if result.rowcount > 0:
                    stats["token_inserted"] += 1
            elif token.condition_id != trade.condition_id or token.outcome != trade.outcome:
                token_conflicts.add(trade.token_id)
                stats["backlogged"] += 1
                await _backlog_trade(session, trade, "token_mapping_conflict")
                continue

            stats["hydrated"] += 1
            await _resolve_backlog(session, trade)

        if apply:
            await session.commit()
        else:
            await session.rollback()
    return stats


async def _validate_fks(apply: bool) -> None:
    counts = await _orphan_counts()
    blockers = {k: v for k, v in counts.items() if v}
    if blockers:
        raise SystemExit(f"Cannot validate FKs with orphan rows present: {blockers}")
    if not apply:
        logger.info("FK validation preconditions pass (dry-run)")
        return
    async with async_session() as session:
        for table, constraint in FK_CONSTRAINTS:
            logger.info("Validating %s.%s", table, constraint)
            await session.execute(text(f"ALTER TABLE {table} VALIDATE CONSTRAINT {constraint}"))
        await session.commit()


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="write DB changes")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--validate-fks", action="store_true")
    args = parser.parse_args()

    try:
        before = await _orphan_counts()
        logger.info("Before: %s", before)

        stats = await _backfill_once(args.limit, apply=args.apply)
        logger.info("%s stats: %s", "Applied" if args.apply else "Dry-run", stats)

        resolved_backlog = await _resolve_hydrated_backlog(args.apply)
        logger.info(
            "%s %d hydrated backlog rows",
            "Resolved" if args.apply else "Would resolve",
            resolved_backlog,
        )

        after = await _orphan_counts()
        logger.info("After: %s", after)

        if args.validate_fks:
            await _validate_fks(args.apply)
    finally:
        await close_shared_gamma()


if __name__ == "__main__":
    asyncio.run(main())
