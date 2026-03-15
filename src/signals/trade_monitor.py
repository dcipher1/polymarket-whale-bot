"""Monitor COPYABLE wallets for new trades and generate signals."""

import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy import select, and_

from src.db import async_session
from src.indexer.wallet_ingester import ingest_wallet
from src.indexer.position_builder import build_positions_for_wallet
from src.models import Wallet, WhaleTrade, WhalePosition, Market, MarketToken, MyTrade
from src.signals.candidate_generator import generate_candidate
from src.signals.convergence import check_convergence
from src.signals.price_checker import check_price_viability
from src.execution.paper_executor import execute_paper_trade
from src.tracking.price_snapshots import schedule_snapshot

logger = logging.getLogger(__name__)


async def monitor_wallets_once() -> int:
    """Poll all COPYABLE wallets for new trades. Returns count of new trades."""
    async with async_session() as session:
        result = await session.execute(
            select(Wallet.address).where(Wallet.copyability_class == "COPYABLE")
        )
        addresses = [r[0] for r in result.all()]

    if not addresses:
        # Also monitor WATCH wallets if no COPYABLE ones yet
        async with async_session() as session:
            result = await session.execute(
                select(Wallet.address).where(
                    Wallet.copyability_class.in_(["COPYABLE", "WATCH"])
                )
            )
            addresses = [r[0] for r in result.all()]

    total_new = 0
    for address in addresses:
        try:
            new_count = await ingest_wallet(address)
            if new_count > 0:
                await build_positions_for_wallet(address)
                total_new += new_count

                # Generate signals for new trades from COPYABLE wallets
                await _process_new_trades(address)
        except Exception as e:
            logger.error("Failed to monitor wallet %s: %s", address[:10], e)

    if total_new > 0:
        logger.info("Detected %d new trades across %d wallets", total_new, len(addresses))

    return total_new


async def _process_new_trades(wallet_address: str) -> None:
    """Generate candidate signals and detect whale exits."""
    async with async_session() as session:
        wallet = await session.get(Wallet, wallet_address)
        if not wallet:
            return

        # Detect whale EXITS — alert if we have a paper trade on this market
        exit_result = await session.execute(
            select(WhalePosition)
            .where(
                and_(
                    WhalePosition.wallet_address == wallet_address,
                    WhalePosition.last_event_type.in_(["REDUCE", "CLOSE"]),
                )
            )
        )
        for exit_pos in exit_result.scalars().all():
            # Check if we have a paper trade on this market
            trade_result = await session.execute(
                select(MyTrade).where(
                    and_(
                        MyTrade.condition_id == exit_pos.condition_id,
                        MyTrade.resolved == False,
                    )
                )
            )
            affected_trade = trade_result.scalar_one_or_none()
            if affected_trade:
                event = exit_pos.last_event_type
                market = await session.get(Market, exit_pos.condition_id)
                q = market.question[:50] if market else exit_pos.condition_id[:10]
                logger.warning(
                    "WHALE EXIT: %s %s position on %s (we have paper trade #%d)",
                    wallet_address[:10], event, q, affected_trade.id,
                )
                await _alert(
                    f"WHALE EXIT: {wallet_address[:10]}... {event} "
                    f"{q} - we have paper trade #{affected_trade.id}!"
                )

        if wallet.copyability_class != "COPYABLE":
            return

        # Get recent positions with OPEN or ADD events
        result = await session.execute(
            select(WhalePosition)
            .where(
                and_(
                    WhalePosition.wallet_address == wallet_address,
                    WhalePosition.last_event_type.in_(["OPEN", "ADD"]),
                    WhalePosition.is_open == True,
                )
            )
        )
        positions = result.scalars().all()

        for pos in positions:
            # Check if market is in target universe
            market = await session.get(Market, pos.condition_id)
            if not market:
                continue
            category = market.category_override or market.category
            if category in ("other", "excluded", "ambiguous", None):
                continue

            avg_price = float(pos.avg_entry_price or 0)
            if avg_price <= 0:
                continue

            # Schedule price snapshots for followability
            token_result = await session.execute(
                select(MarketToken).where(
                    and_(
                        MarketToken.condition_id == pos.condition_id,
                        MarketToken.outcome == pos.outcome,
                    )
                )
            )
            token = token_result.scalar_one_or_none()
            if token:
                await schedule_snapshot(
                    wallet_address=wallet_address,
                    condition_id=pos.condition_id,
                    token_id=token.token_id,
                    outcome=pos.outcome,
                    category=category,
                    entry_price=avg_price,
                    entry_time=datetime.now(timezone.utc),
                )

            # Generate candidate signal
            signal_id = await generate_candidate(
                wallet_address=wallet_address,
                condition_id=pos.condition_id,
                outcome=pos.outcome,
                whale_price=avg_price,
                event_type=pos.last_event_type,
            )

            if signal_id:
                logger.info(
                    "CANDIDATE signal %d created: %s on %s %s at %.4f",
                    signal_id, wallet_address[:10], pos.condition_id[:10],
                    pos.outcome, avg_price,
                )

                await _alert(
                    f"CANDIDATE: Whale {wallet_address[:10]}... entered "
                    f"{market.question[:50]} - {pos.outcome} at ${avg_price:.4f}"
                )

                # Check convergence
                convergence_id = await check_convergence(signal_id)
                if convergence_id:
                    logger.info("CONVERGENCE signal %d from candidate %d", convergence_id, signal_id)

                    await _alert(
                        f"CONVERGENCE: 2+ whales agree on "
                        f"{market.question[:50]} - {pos.outcome}"
                    )

                    # Check price viability
                    is_live = await check_price_viability(convergence_id)
                    if is_live:
                        # Execute paper trade
                        trade_id = await execute_paper_trade(convergence_id)
                        if trade_id:
                            logger.info("PAPER TRADE %d executed from signal %d", trade_id, convergence_id)

                            await _alert(
                                f"PAPER TRADE #{trade_id}: "
                                f"{market.question[:50]} {pos.outcome} at ${avg_price:.4f} ($100)"
                            )


async def _alert(msg: str) -> None:
    """Send Telegram alert (best-effort)."""
    try:
        from src.monitoring.telegram import send_alert
        await send_alert(msg)
    except Exception:
        pass


async def run_trade_monitor(interval_seconds: float = 30):
    """Continuously monitor for new trades."""
    logger.info("Trade monitor started (interval=%ds)", interval_seconds)
    while True:
        try:
            await monitor_wallets_once()
        except Exception as e:
            logger.error("Trade monitor error: %s", e)
        await asyncio.sleep(interval_seconds)
