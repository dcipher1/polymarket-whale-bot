"""Wallet trade and activity ingestion from Data API."""

import logging
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.db import async_session
from src.events import publish, CHANNEL_NEW_WHALE_TRADE
from src.models import Wallet, WhaleTrade, Market
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
            if not full_backfill:
                wallet = await session.get(Wallet, address)
                if wallet and wallet.last_ingested_ts:
                    since_ts = wallet.last_ingested_ts

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
            latest_ts = since_ts
            for trade in trades:
                inserted = await _upsert_trade(session, address, trade)
                if inserted:
                    new_count += 1

                # Track latest trade timestamp for next incremental fetch
                ts = _parse_timestamp(trade)
                if ts and (latest_ts is None or ts > latest_ts):
                    latest_ts = ts

            # Update wallet stats and last_ingested_ts
            # Always advance to now so incremental polls don't re-fetch
            update_ts = datetime.now(timezone.utc) if since_ts else latest_ts
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

    stmt = insert(WhaleTrade).values(
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
    ).on_conflict_do_nothing(
        constraint="uq_whale_trades_dedup"
    )

    try:
        result = await session.execute(stmt)
        is_new = result.rowcount > 0
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
