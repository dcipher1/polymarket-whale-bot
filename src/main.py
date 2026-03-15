"""Main entry point / orchestrator for the whale bot."""

import asyncio
import logging
import sys

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from src.config import settings

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


async def startup_checks() -> bool:
    """Run startup checks. Returns True if all pass."""
    from src.polymarket.geoblock import check_geoblock

    logger.info("Running startup checks...")

    # Geoblock check — always run at startup
    blocked = await check_geoblock()
    if blocked:
        logger.error("STARTUP HALTED: Polymarket access is geoblocked from this IP")
        try:
            from src.monitoring.telegram import send_alert
            await send_alert("STARTUP HALTED: Geoblocked from Polymarket")
        except Exception:
            pass
        return False

    logger.info("Startup checks passed")
    return True


async def run_bot():
    """Main bot loop."""
    if not await startup_checks():
        sys.exit(1)

    scheduler = AsyncIOScheduler()

    # Market ingestion — every 15 minutes
    from src.indexer.market_ingester import ingest_markets
    scheduler.add_job(ingest_markets, "interval", minutes=settings.market_refresh_interval_minutes, id="market_ingest")

    # Wallet rescoring — every 6 hours
    from src.scorer.wallet_scorer import score_all_wallets
    scheduler.add_job(score_all_wallets, "interval", hours=settings.wallet_rescore_interval_hours, id="wallet_rescore")

    # Signal expiry — every 5 minutes
    from src.signals.signal_manager import expire_stale_signals
    scheduler.add_job(expire_stale_signals, "interval", minutes=5, id="signal_expiry")

    # Resolution tracking — every 15 minutes
    from src.tracking.resolution import check_resolutions
    scheduler.add_job(check_resolutions, "interval", minutes=15, id="resolution_check")

    # Daily snapshot — once per day at 23:55 UTC
    from src.tracking.snapshots import take_daily_snapshot
    scheduler.add_job(take_daily_snapshot, "cron", hour=23, minute=55, id="daily_snapshot")

    # Whale discovery — every 24 hours, scan category leaderboards for new whales
    from src.indexer.whale_discovery import discover_and_ingest
    scheduler.add_job(discover_and_ingest, "interval", hours=24, id="whale_discovery")

    # Daily Telegram summary — 10pm UTC
    async def daily_summary():
        try:
            from src.tracking.pnl import get_portfolio_pnl
            from src.monitoring.telegram import send_alert
            from sqlalchemy import select, func
            pnl = await get_portfolio_pnl()
            async with async_session() as session:
                from src.models import Signal, Wallet
                pending = (await session.execute(
                    select(func.count(Signal.id)).where(Signal.status == "PENDING")
                )).scalar() or 0
                copyable = (await session.execute(
                    select(func.count(Wallet.address)).where(Wallet.copyability_class == "COPYABLE")
                )).scalar() or 0
            await send_alert(
                f"Daily Summary\n"
                f"P&L: ${pnl['total_pnl']:+,.2f}\n"
                f"Open: {pnl['open_positions']} positions (${pnl['open_exposure']:,.0f})\n"
                f"Record: {pnl['wins']}W/{pnl['losses']}L ({pnl['win_rate']:.0%})\n"
                f"Pending signals: {pending}\n"
                f"Tracking: {copyable} COPYABLE whales"
            )
        except Exception as e:
            logger.warning("Daily summary failed: %s", e)

    scheduler.add_job(daily_summary, "cron", hour=22, minute=0, id="daily_summary_tg")

    # Geoblock recheck — only in Phase 3+ (live execution)
    if settings.live_execution_enabled:
        from src.polymarket.geoblock import check_geoblock
        scheduler.add_job(
            check_geoblock, "interval",
            hours=settings.geoblock_check_interval_hours,
            id="geoblock_recheck",
        )

    scheduler.start()

    # Initial data load
    logger.info("Running initial market ingestion...")
    try:
        await ingest_markets()
    except Exception as e:
        logger.error("Initial market ingestion failed: %s", e)

    # Start trade monitor
    from src.signals.trade_monitor import run_trade_monitor
    monitor_task = asyncio.create_task(run_trade_monitor(interval_seconds=30))

    # Start price snapshot processor (for real followability)
    from src.tracking.price_snapshots import run_snapshot_processor, recompute_followability_from_snapshots
    snapshot_task = asyncio.create_task(run_snapshot_processor(interval_seconds=30))

    # Recompute followability from real snapshots — every 6 hours (alongside wallet rescoring)
    scheduler.add_job(recompute_followability_from_snapshots, "interval", hours=6, id="followability_recompute")

    # Start WebSocket (Phase 2)
    ws_task = None
    try:
        from src.polymarket.websocket import MarketWebSocket
        ws = MarketWebSocket()
        ws_task = asyncio.create_task(ws.connect())
    except Exception as e:
        logger.warning("WebSocket startup failed (non-fatal): %s", e)

    # Start Telegram bot if configured
    telegram_task = None
    if settings.telegram_bot_token:
        try:
            from src.monitoring.telegram import build_telegram_app
            tg_app = build_telegram_app()
            telegram_task = asyncio.create_task(tg_app.run_polling(close_loop=False))
            logger.info("Telegram bot started")
        except Exception as e:
            logger.warning("Telegram bot startup failed (non-fatal): %s", e)

    logger.info("Whale bot is running. Press Ctrl+C to stop.")

    try:
        await asyncio.gather(monitor_task, return_exceptions=True)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        scheduler.shutdown()


def main():
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
