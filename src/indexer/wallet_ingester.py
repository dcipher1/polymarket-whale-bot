"""Wallet trade and activity ingestion from Data API."""

import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from sqlalchemy import select, func, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.db import async_session
from src.events import publish, CHANNEL_NEW_WHALE_TRADE
from src.indexer.market_ingester import ensure_market
from src.models import Wallet, WhaleTrade, MarketToken, WhaleEventBacklog
from src.polymarket.data_api import DataAPIClient, Trade, Activity

logger = logging.getLogger(__name__)


async def ingest_wallet(
    address: str,
    full_backfill: bool = False,
    client: DataAPIClient | None = None,
) -> int:
    """Ingest all trades for a wallet. Returns count of new trades.

    Args:
        address: Wallet address to ingest.
        full_backfill: If True, fetch all trades regardless of last timestamp.
        client: Shared DataAPIClient instance. If None, creates a new one.
    """
    owns_client = client is None
    if owns_client:
        client = DataAPIClient()
    try:
        async with async_session() as session:
            # Ensure wallet exists
            await _ensure_wallet(session, address)

            # Get last ingested timestamp for incremental fetch
            since_ts = None
            previous_last_ts = None
            if not full_backfill:
                wallet = await session.get(Wallet, address)
                if wallet and wallet.last_ingested_ts:
                    previous_last_ts = wallet.last_ingested_ts
                    since_ts = previous_last_ts - timedelta(minutes=2)

            # Fetch trades via activity API (more complete than trades endpoint)
            # In incremental mode, stop paginating once we hit already-seen trades
            if since_ts:
                since_epoch = int(since_ts.timestamp())
                all_activity = []
                offset = 0
                for _ in range(30):
                    try:
                        batch = await client.get_activity(address, limit=100, offset=offset)
                    except Exception:
                        break
                    if not batch:
                        break
                    all_activity.extend(batch)
                    # Activity is newest-first; stop if oldest in batch is before cutoff
                    oldest_ts = min((int(float(a.timestamp or 0)) for a in batch if a.timestamp), default=0)
                    if oldest_ts and oldest_ts < since_epoch:
                        break
                    if len(batch) < 100:
                        break
                    offset += 100
            else:
                all_activity = await client.get_all_activity(address)

            trades = [a for a in all_activity if a.type == "TRADE"]

            new_count = 0
            latest_ts = previous_last_ts
            for trade in trades:
                inserted = await _upsert_trade(session, address, trade)
                if inserted:
                    new_count += 1

                # Track latest trade timestamp for next incremental fetch
                ts = _parse_timestamp(trade)
                if ts and (latest_ts is None or ts > latest_ts):
                    latest_ts = ts

            # Update wallet stats and last_ingested_ts. Advance only to the
            # newest observed trade timestamp so late-arriving API rows remain
            # inside the next incremental window.
            update_ts = latest_ts
            await _update_wallet_stats(session, address, update_ts)
            await session.commit()

            if new_count > 0:
                logger.info(
                    "Fetched %d new trades for wallet %s (%d checked)",
                    new_count, address[:10], len(trades),
                )
            elif not since_ts:
                logger.info(
                    "Fetched %d trades for wallet %s (full)",
                    len(trades), address[:10],
                )
            return new_count
    finally:
        if owns_client:
            await client.close()


async def _ensure_wallet(session: AsyncSession, address: str) -> None:
    stmt = insert(Wallet).values(
        address=address,
        first_seen=datetime.now(timezone.utc),
    ).on_conflict_do_nothing(index_elements=["address"])
    await session.execute(stmt)
    await session.flush()


def _parse_timestamp(trade: Trade | Activity) -> datetime | None:
    """Parse timestamp from a trade or activity object."""
    ts_str = trade.timestamp or getattr(trade, "match_time", "") or getattr(trade, "last_update", "")
    if not ts_str:
        return None
    try:
        ts_val = int(float(ts_str))
        if ts_val > 1_000_000_000:
            return datetime.fromtimestamp(ts_val, tz=timezone.utc)
    except (ValueError, TypeError):
        pass
    try:
        return datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


async def _upsert_trade(session: AsyncSession, wallet_address: str, trade: Trade | Activity) -> bool:
    """Insert a trade if not duplicate. Returns True if new."""
    # Parse trade data — handle both Trade and Activity models
    tx_hash = getattr(trade, "transaction_hash", "") or trade.id
    if not tx_hash:
        return False

    token_id = getattr(trade, "asset", "") or getattr(trade, "asset_id", "") or getattr(trade, "token_id", "")
    if not token_id:
        return False

    # Determine side
    side = trade.side.upper() if trade.side else getattr(trade, "trader_side", "").upper()
    if side not in ("BUY", "SELL"):
        return False

    # Determine outcome — look up from market_tokens table, or use outcomeIndex
    outcome_raw = trade.outcome.upper() if trade.outcome else ""
    if outcome_raw in ("YES", "NO"):
        outcome = outcome_raw
    else:
        # Map by outcomeIndex: 0=YES, 1=NO (Polymarket convention)
        outcome = "YES" if getattr(trade, "outcome_index", 0) == 0 else "NO"

    try:
        price = Decimal(str(trade.price))
        size = Decimal(str(trade.size))
    except Exception:
        return False

    size_usdc = price * size  # cost basis

    # Parse timestamp
    timestamp = _parse_timestamp(trade) or datetime.now(timezone.utc)

    # Determine condition_id
    condition_id = trade.condition_id or trade.market or ""
    if not condition_id:
        return False

    market = await ensure_market(condition_id, session, slug=getattr(trade, "slug", "") or None)
    if not market:
        await _backlog_trade(session, wallet_address, trade, condition_id, token_id, outcome, side, "market_hydration_failed")
        return False

    token = await session.get(MarketToken, token_id)
    if not token:
        token_stmt = insert(MarketToken).values(
            token_id=token_id,
            condition_id=condition_id,
            outcome=outcome,
        ).on_conflict_do_nothing(index_elements=["token_id"])
        await session.execute(token_stmt)
    elif token.condition_id != condition_id or token.outcome != outcome:
        await _backlog_trade(session, wallet_address, trade, condition_id, token_id, outcome, side, "token_mapping_conflict")
        return False

    insert_stmt = insert(WhaleTrade).values(
        wallet_address=wallet_address,
        condition_id=condition_id,
        token_id=token_id,
        side=side,
        outcome=outcome,
        price=price,
        size_usdc=size_usdc,
        num_contracts=size,
        timestamp=timestamp,
        tx_hash=tx_hash,
        detected_at=datetime.now(timezone.utc),
    )
    # Upsert (not do-nothing): PolyNode emits one event per individual fill within
    # a multi-fill trade, so a row may already exist with a partial num_contracts /
    # size_usdc. The data-api activity feed reports the aggregated total per
    # tx_hash, which is the canonical truth — let it overwrite the partial.
    stmt = insert_stmt.on_conflict_do_update(
        constraint="uq_whale_trades_dedup",
        set_={
            "side": insert_stmt.excluded.side,
            "outcome": insert_stmt.excluded.outcome,
            "price": insert_stmt.excluded.price,
            "size_usdc": insert_stmt.excluded.size_usdc,
            "num_contracts": insert_stmt.excluded.num_contracts,
            "timestamp": insert_stmt.excluded.timestamp,
        },
    # xmax=0 only for fresh inserts, so we can distinguish "newly seen trade"
    # (publish event) from "ingester reconciled a partial-fill row" (silent).
    ).returning(WhaleTrade.__table__.c.tx_hash, text("(xmax = 0)"))

    try:
        result = await session.execute(stmt)
        row = result.first()
        # row[0] = tx_hash, row[1] = (xmax = 0) — True for inserts, False for updates.
        is_new = bool(row[1]) if row is not None else False
    except Exception:
        await session.rollback()
        return False

    if is_new and side == "BUY":
        # Publish event for new buy trades (potential signal source)
        await publish(CHANNEL_NEW_WHALE_TRADE, {
            "wallet_address": wallet_address,
            "condition_id": condition_id,
            "token_id": token_id,
            "side": side,
            "outcome": outcome,
            "price": str(price),
            "size_usdc": str(size_usdc),
            "timestamp": timestamp.isoformat(),
        })

    return is_new


async def _backlog_trade(
    session: AsyncSession,
    wallet_address: str,
    trade: Trade | Activity,
    condition_id: str,
    token_id: str,
    outcome: str,
    side: str,
    reason: str,
) -> None:
    tx_hash = getattr(trade, "transaction_hash", "") or trade.id
    raw = {
        "source": "data_api_wallet_ingester",
        "wallet_address": wallet_address,
        "condition_id": condition_id,
        "token_id": token_id,
        "outcome": outcome,
        "side": side,
        "price": str(getattr(trade, "price", "")),
        "size": str(getattr(trade, "size", "")),
        "tx_hash": tx_hash,
        "timestamp": str(getattr(trade, "timestamp", "")),
        "title": getattr(trade, "title", ""),
        "slug": getattr(trade, "slug", ""),
    }
    stmt = insert(WhaleEventBacklog).values(
        provider="data_api",
        wallet_address=wallet_address,
        condition_id=condition_id,
        token_id=token_id,
        outcome=outcome,
        side=side,
        tx_hash=tx_hash or f"data_api:{wallet_address}:{condition_id}:{token_id}:{getattr(trade, 'timestamp', '')}",
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


async def _update_wallet_stats(
    session: AsyncSession,
    address: str,
    latest_ts: datetime | None = None,
) -> None:
    """Update aggregate stats on the wallet record."""
    result = await session.execute(
        select(func.count(WhaleTrade.id)).where(WhaleTrade.wallet_address == address)
    )
    total_trades = result.scalar() or 0

    result = await session.execute(
        select(func.max(WhaleTrade.timestamp)).where(WhaleTrade.wallet_address == address)
    )
    last_active = result.scalar()

    wallet = await session.get(Wallet, address)
    if wallet:
        wallet.total_trades = total_trades
        wallet.last_active = last_active
        if latest_ts:
            wallet.last_ingested_ts = latest_ts
