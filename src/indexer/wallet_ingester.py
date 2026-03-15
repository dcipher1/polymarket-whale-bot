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


async def ingest_wallet(address: str, full_backfill: bool = False) -> int:
    """Ingest all trades for a wallet. Returns count of new trades."""
    client = DataAPIClient()
    try:
        async with async_session() as session:
            # Ensure wallet exists
            await _ensure_wallet(session, address)

            # Fetch trades
            trades = await client.get_all_trades(address)
            logger.info("Fetched %d trades for wallet %s", len(trades), address[:10])

            new_count = 0
            for trade in trades:
                inserted = await _upsert_trade(session, address, trade)
                if inserted:
                    new_count += 1

            # Fetch activity for redemptions
            activity = await client.get_all_activity(address)
            logger.info("Fetched %d activity records for wallet %s", len(activity), address[:10])

            # Update wallet stats
            await _update_wallet_stats(session, address)
            await session.commit()

            logger.info(
                "Ingested wallet %s: %d new trades out of %d total",
                address[:10], new_count, len(trades),
            )
            return new_count
    finally:
        await client.close()


async def _ensure_wallet(session: AsyncSession, address: str) -> None:
    stmt = insert(Wallet).values(
        address=address,
        first_seen=datetime.now(timezone.utc),
    ).on_conflict_do_nothing(index_elements=["address"])
    await session.execute(stmt)
    await session.flush()


async def _upsert_trade(session: AsyncSession, wallet_address: str, trade: Trade) -> bool:
    """Insert a trade if not duplicate. Returns True if new."""
    # Parse trade data
    tx_hash = trade.transaction_hash or trade.id
    if not tx_hash:
        return False

    token_id = trade.asset or trade.asset_id
    if not token_id:
        return False

    # Determine side
    side = trade.side.upper() if trade.side else trade.trader_side.upper()
    if side not in ("BUY", "SELL"):
        return False

    # Determine outcome — look up from market_tokens table, or use outcomeIndex
    outcome_raw = trade.outcome.upper() if trade.outcome else ""
    if outcome_raw in ("YES", "NO"):
        outcome = outcome_raw
    else:
        # Map by outcomeIndex: 0=YES, 1=NO (Polymarket convention)
        outcome = "YES" if trade.outcome_index == 0 else "NO"

    try:
        price = Decimal(str(trade.price))
        size = Decimal(str(trade.size))
    except Exception:
        return False

    size_usdc = price * size  # approximate

    # Parse timestamp — can be unix epoch or ISO string
    timestamp = None
    ts_str = trade.timestamp or trade.match_time or trade.last_update
    if ts_str:
        try:
            ts_val = int(float(ts_str))
            if ts_val > 1_000_000_000:
                timestamp = datetime.fromtimestamp(ts_val, tz=timezone.utc)
        except (ValueError, TypeError):
            pass
        if not timestamp:
            try:
                timestamp = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
            except (ValueError, TypeError):
                pass
    if not timestamp:
        timestamp = datetime.now(timezone.utc)

    # Determine condition_id
    condition_id = trade.condition_id or trade.market or ""

    # Skip if market doesn't exist in our DB (FK constraint)
    if condition_id:
        from src.models import Market
        market_exists = await session.get(Market, condition_id)
        if not market_exists:
            return False

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


async def _update_wallet_stats(session: AsyncSession, address: str) -> None:
    """Update aggregate stats on the wallet record."""
    result = await session.execute(
        select(func.count(WhaleTrade.id)).where(WhaleTrade.wallet_address == address)
    )
    total_trades = result.scalar() or 0

    result = await session.execute(
        select(func.max(WhaleTrade.timestamp)).where(WhaleTrade.wallet_address == address)
    )
    last_active = result.scalar()

    await session.execute(
        select(Wallet).where(Wallet.address == address)
    )
    wallet = await session.get(Wallet, address)
    if wallet:
        wallet.total_trades = total_trades
        wallet.last_active = last_active
