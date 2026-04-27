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
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


async def ensure_vpn() -> bool:
    """Connect NordVPN to Montreal if not already connected. Returns True on success."""
    proc = await asyncio.create_subprocess_exec(
        "nordvpn", "status",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await proc.communicate()
    status = stdout.decode()

    if "Connected" in status:
        logger.info("NordVPN already connected")
        return True

    logger.info("Connecting NordVPN to Montreal...")
    proc = await asyncio.create_subprocess_exec(
        "nordvpn", "connect", "Canada", "Montreal",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    output = stdout.decode() + stderr.decode()

    if proc.returncode == 0 and "You are connected" in output:
        logger.info("NordVPN connected: %s", output.strip())
        await asyncio.sleep(2)  # let routing settle
        return True

    logger.error("NordVPN connection failed: %s", output.strip())
    return False


async def startup_checks() -> bool:
    """Run startup checks. Returns True if all pass."""
    from src.polymarket.geoblock import check_geoblock

    logger.info("Running startup checks...")

    # Geoblock check — connect VPN if blocked
    blocked = await check_geoblock()
    if blocked:
        logger.warning("Geoblocked — attempting NordVPN connect...")
        if not await ensure_vpn():
            logger.error("STARTUP HALTED: VPN connection failed")
            return False
        # Recheck after VPN
        blocked = await check_geoblock()
        if blocked:
            logger.error("STARTUP HALTED: Still geoblocked after VPN connect")
            try:
                from src.monitoring.telegram import send_alert
                await send_alert("STARTUP HALTED: Geoblocked even after VPN")
            except Exception:
                pass
            return False

    logger.info("Startup checks passed")

    # Live execution validation
    if settings.live_execution_enabled:
        logger.info("Live execution is ENABLED — validating CLOB credentials...")
        live_ok = await _validate_live_execution()
        if not live_ok:
            logger.warning(
                "Live execution validation FAILED — falling back to PAPER mode"
            )
            settings.live_execution_enabled = False
            try:
                from src.monitoring.telegram import send_alert
                await send_alert(
                    "WARNING: Live execution validation failed at startup. "
                    "Bot is running in PAPER mode."
                )
            except Exception:
                pass
    else:
        logger.info("Live execution is DISABLED — running in paper mode")

    return True


async def _validate_live_execution() -> bool:
    """Validate live execution prerequisites. Returns True if all checks pass."""
    from src.polymarket.clob_auth import get_auth_client

    # Check private key is set
    if not settings.polymarket_private_key:
        logger.error("POLYMARKET_PRIVATE_KEY is not set in .env")
        return False

    auth_client = get_auth_client()

    # Test CLOB connectivity
    logger.info("  Testing CLOB connectivity...")
    connected = await auth_client.test_connectivity()
    if not connected:
        logger.error("  CLOB connectivity test failed")
        return False
    logger.info("  CLOB connectivity: OK")

    # Check USDC balance and cache it for all modules
    logger.info("  Checking USDC balance...")
    try:
        balance = await auth_client.get_balance()
        logger.info("  USDC balance: $%.2f", balance)
        from src.events import update_cached_balance
        await update_cached_balance(balance)
        if balance < 1.0:
            logger.warning(
                "  USDC balance is very low ($%.2f) — orders may fail",
                balance,
            )
    except Exception as e:
        logger.error("  Balance check failed: %s", e)
        return False

    logger.info("  Live execution validation passed (balance=$%.2f)", balance)
    return True


async def run_bot():
    """Main bot loop."""
    if not await startup_checks():
        sys.exit(1)

    scheduler = AsyncIOScheduler()
    interval_job_defaults = {"coalesce": True, "max_instances": 1, "misfire_grace_time": 300}

    # Market refresh — lightweight update of tracked markets (on-demand fetch handles new ones)
    from src.indexer.market_ingester import refresh_tracked_markets
    scheduler.add_job(
        refresh_tracked_markets,
        "interval",
        minutes=settings.market_refresh_interval_minutes,
        id="market_refresh",
        **interval_job_defaults,
    )

    # Resolution tracking — every 15 minutes
    from src.tracking.resolution import check_resolutions, redeem_all_resolved
    scheduler.add_job(check_resolutions, "interval", minutes=15, id="resolution_check", **interval_job_defaults)

    # Redeem all resolved positions (wins + losses) — every 30 minutes
    scheduler.add_job(redeem_all_resolved, "interval", minutes=30, id="redemption_sweep", **interval_job_defaults)

    # Reconcile DB against Polymarket wallet — every 15 minutes
    from src.execution.reconcile import reconcile_positions as reconcile_job
    scheduler.add_job(reconcile_job, "interval", minutes=15, id="reconcile", **interval_job_defaults)

    # Daily snapshot — once per day at 23:55 UTC
    from src.tracking.snapshots import take_daily_snapshot
    scheduler.add_job(take_daily_snapshot, "cron", hour=23, minute=55, id="daily_snapshot")

    # Daily Telegram summary — 10pm UTC
    async def daily_summary():
        try:
            from src.tracking.pnl import get_portfolio_pnl
            from src.monitoring.telegram import send_alert
            from src.db import async_session
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
            **interval_job_defaults,
        )

    scheduler.start()

    # Ensure LAN access to dashboard port through NordVPN firewall
    try:
        proc = await asyncio.create_subprocess_exec(
            "sudo", "iptables", "-C", "INPUT", "-p", "tcp", "--dport", "8080",
            "-s", "192.168.0.0/16", "-j", "ACCEPT",
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        await proc.communicate()
        if proc.returncode != 0:
            proc = await asyncio.create_subprocess_exec(
                "sudo", "iptables", "-I", "INPUT", "-p", "tcp", "--dport", "8080",
                "-s", "192.168.0.0/16", "-j", "ACCEPT",
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
            )
            await proc.communicate()
    except Exception:
        pass

    # Start web dashboard early so it is available while the copy loop runs.
    dashboard_runner = None
    dashboard_site = None
    try:
        from aiohttp import web as _web
        from src.monitoring.web_dashboard import handle_dashboard
        dash_app = _web.Application()
        dash_app.router.add_get("/", handle_dashboard)
        dashboard_runner = _web.AppRunner(dash_app, access_log=None)
        await dashboard_runner.setup()
        dashboard_site = _web.TCPSite(dashboard_runner, "0.0.0.0", 8080)
        await dashboard_site.start()
        logger.info("Web dashboard running on http://0.0.0.0:8080")
    except Exception as e:
        logger.warning("Web dashboard startup failed (non-fatal): %s", e)

    # Seed the watch_whales into the DB + pull recent trades
    from src.indexer.wallet_ingester import ingest_wallet
    for addr in settings.watch_whales:
        try:
            new = await ingest_wallet(addr.lower())
            logger.info("Seeded watch whale %s (+%d trades)", addr[:12], new)
        except Exception as e:
            logger.warning("Failed to seed watch whale %s: %s", addr[:12], e)

    # Redeem resolved positions on-chain (burn losses, collect wins)
    from src.tracking.resolution import redeem_all_resolved
    try:
        redeemed = await redeem_all_resolved()
        logger.info("Startup redemption: %d markets redeemed", redeemed)
    except Exception as e:
        logger.error("Startup redemption failed: %s", e)

    # Step 3: Reconcile DB with clean wallet state
    from src.execution.reconcile import reconcile_positions
    try:
        recon = await reconcile_positions()
        logger.info("Position reconciliation: %s", recon)
    except Exception as e:
        logger.error("Position reconciliation failed: %s", e)

    # Start websocket copy path plus a slower polling reconciliation backstop.
    from src.signals.sync_positions import handle_polynode_wallet_event, run_sync_loop
    monitor_task = asyncio.create_task(
        run_sync_loop(interval_seconds=settings.wallet_sync_fallback_interval_seconds)
    )
    polynode_task = None
    if settings.polynode_enabled and settings.polynode_api_key:
        from src.polymarket.polynode_wallet_ws import PolyNodeWalletStream
        stream = PolyNodeWalletStream(handle_polynode_wallet_event)
        polynode_task = asyncio.create_task(stream.run())
        logger.info("PolyNode wallet websocket started")
    elif settings.polynode_enabled:
        logger.warning("PolyNode wallet websocket not started: POLYNODE_API_KEY is not set")

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

    # Keep-alive: run forever, restart the monitor if it dies
    tasks = {"monitor": monitor_task}
    if polynode_task is not None:
        tasks["polynode"] = polynode_task

    try:
        while True:
            done, _ = await asyncio.wait(
                tasks.values(),
                return_when=asyncio.FIRST_COMPLETED,
                timeout=60,
            )
            for task in done:
                name = next(k for k, v in tasks.items() if v is task)
                exc = task.exception() if not task.cancelled() else None
                if exc:
                    logger.error("Task '%s' crashed: %s — restarting in 5s", name, exc)
                else:
                    logger.warning("Task '%s' exited unexpectedly — restarting in 5s", name)
                await asyncio.sleep(5)
                if name == "monitor":
                    tasks[name] = asyncio.create_task(
                        run_sync_loop(interval_seconds=settings.wallet_sync_fallback_interval_seconds)
                    )
                elif name == "polynode":
                    from src.polymarket.polynode_wallet_ws import PolyNodeWalletStream
                    stream = PolyNodeWalletStream(handle_polynode_wallet_event)
                    tasks[name] = asyncio.create_task(stream.run())
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Shutting down...")
    finally:
        if dashboard_runner:
            await dashboard_runner.cleanup()
        scheduler.shutdown()


def main():
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
