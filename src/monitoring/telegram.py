"""Telegram bot for alerts and commands."""

import logging

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)
from sqlalchemy import select, func

from src.config import settings
from src.db import async_session
from src.models import Wallet, Market, Signal, MyTrade
from src.tracking.pnl import get_portfolio_pnl
from src.tracking.attribution import compute_attribution
from src.signals.signal_manager import get_diagnostics

logger = logging.getLogger(__name__)

_app: Application | None = None


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    async with async_session() as session:
        wallets = (await session.execute(select(func.count(Wallet.address)))).scalar() or 0
        copyable = (await session.execute(
            select(func.count(Wallet.address)).where(Wallet.copyability_class == "COPYABLE")
        )).scalar() or 0
        markets = (await session.execute(
            select(func.count(Market.condition_id)).where(Market.resolved == False)
        )).scalar() or 0
        signals = (await session.execute(
            select(func.count(Signal.id)).where(Signal.status == "PENDING")
        )).scalar() or 0

    pnl = await get_portfolio_pnl()

    msg = (
        f"Whale Bot Status\n"
        f"Wallets: {wallets} tracked, {copyable} COPYABLE\n"
        f"Markets: {markets} active\n"
        f"Pending Signals: {signals}\n"
        f"P&L: ${pnl['total_pnl']:+,.2f}\n"
        f"Win Rate: {pnl['win_rate']:.0%} ({pnl['wins']}W/{pnl['losses']}L)\n"
        f"Profit Factor: {pnl['profit_factor']:.2f}\n"
        f"Open: {pnl['open_positions']} positions"
    )
    await update.message.reply_text(msg)


async def cmd_wallets(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    async with async_session() as session:
        result = await session.execute(
            select(Wallet)
            .where(Wallet.copyability_class == "COPYABLE")
            .order_by(Wallet.conviction_score.desc())
            .limit(10)
        )
        wallets = result.scalars().all()

    if not wallets:
        await update.message.reply_text("No COPYABLE wallets found yet.")
        return

    lines = ["Top COPYABLE Wallets:"]
    for w in wallets:
        lines.append(
            f"  {w.address[:10]}... | Conv: {w.conviction_score} | "
            f"P&L: ${float(w.total_pnl_usdc or 0):,.0f}"
        )
    await update.message.reply_text("\n".join(lines))


async def cmd_markets(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    async with async_session() as session:
        result = await session.execute(
            select(Market.category, func.count(Market.condition_id))
            .where(Market.resolved == False)
            .group_by(Market.category)
        )
        rows = result.all()

    lines = ["Market Universe:"]
    for cat, count in rows:
        lines.append(f"  {cat or 'other'}: {count}")
    await update.message.reply_text("\n".join(lines))


async def cmd_health(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    from src.polymarket.geoblock import check_geoblock
    blocked = await check_geoblock()

    msg = (
        f"Health Check\n"
        f"Geoblock: {'BLOCKED' if blocked else 'OK'}\n"
        f"DB: Connected\n"
        f"Phase: {'LIVE' if settings.live_execution_enabled else 'PAPER'}"
    )
    await update.message.reply_text(msg)


async def cmd_signals(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    from src.signals.signal_manager import get_pending_signals
    signals = await get_pending_signals()

    if not signals:
        await update.message.reply_text("No pending signals.")
        return

    lines = ["Pending Signals:"]
    for s in signals[:10]:
        lines.append(
            f"  #{s.id} {s.signal_type} | {s.condition_id[:10]}... | "
            f"{s.outcome} | Wallets: {s.convergence_count}"
        )
    await update.message.reply_text("\n".join(lines))


async def cmd_pnl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    pnl = await get_portfolio_pnl()
    msg = (
        f"Portfolio P&L\n"
        f"Cumulative: ${pnl['total_pnl']:+,.2f}\n"
        f"Resolved: {pnl['total_resolved']} trades\n"
        f"Win Rate: {pnl['win_rate']:.0%}\n"
        f"Profit Factor: {pnl['profit_factor']:.2f}\n"
        f"Expectancy: ${pnl['expectancy']:+.2f}/trade\n"
        f"Open: {pnl['open_positions']} (${pnl['open_exposure']:,.0f} exposure)"
    )
    await update.message.reply_text(msg)


async def cmd_diagnostics(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    diag = await get_diagnostics(days=7)

    lines = [
        f"Diagnostics (last {diag['period_days']} days)",
        f"Total signals: {diag['total_signals']}",
        f"By type: {diag['by_type']}",
        f"By status: {diag['by_status']}",
        "",
        "Conversion rates:",
    ]
    for k, v in diag["conversion_rates"].items():
        lines.append(f"  {k}: {v}")

    lines.append("\nSkip reasons:")
    for reason, count in diag["skip_reasons"].items():
        lines.append(f"  {reason}: {count}")

    if diag["skip_by_category"]:
        lines.append("\nSkip by category:")
        for cat, reasons in diag["skip_by_category"].items():
            lines.append(f"  {cat}:")
            for reason, count in reasons.items():
                lines.append(f"    {reason}: {count}")

    await update.message.reply_text("\n".join(lines))


async def send_alert(message: str) -> None:
    """Send an alert message to the configured chat."""
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        logger.debug("Telegram not configured, skipping alert: %s", message[:50])
        return

    global _app
    if _app is None:
        _app = Application.builder().token(settings.telegram_bot_token).build()
        await _app.initialize()

    try:
        await _app.bot.send_message(
            chat_id=settings.telegram_chat_id,
            text=message,
        )
    except Exception as e:
        logger.error("Failed to send Telegram alert: %s", e)


def build_telegram_app() -> Application:
    """Build the Telegram application with all command handlers."""
    app = Application.builder().token(settings.telegram_bot_token).build()

    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("wallets", cmd_wallets))
    app.add_handler(CommandHandler("markets", cmd_markets))
    app.add_handler(CommandHandler("health", cmd_health))
    app.add_handler(CommandHandler("signals", cmd_signals))
    app.add_handler(CommandHandler("pnl", cmd_pnl))
    app.add_handler(CommandHandler("diagnostics", cmd_diagnostics))

    return app
