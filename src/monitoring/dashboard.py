"""Rich CLI dashboard for whale bot monitoring."""

import asyncio
import logging

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from sqlalchemy import select, func, and_

from src.db import async_session
from src.models import Wallet, Market, Signal, MyTrade, WalletCategoryScore
from src.tracking.pnl import get_portfolio_pnl

logger = logging.getLogger(__name__)
console = Console()


async def _wallet_table() -> Table:
    table = Table(title="Whale Leaderboard", show_lines=True)
    table.add_column("Address", style="cyan", max_width=12)
    table.add_column("P&L", justify="right", style="green")
    table.add_column("Conv.", justify="right")
    table.add_column("Class", style="bold")
    table.add_column("Best Cat", style="magenta")
    table.add_column("Win Rate", justify="right")
    table.add_column("Trades/Mo", justify="right")

    async with async_session() as session:
        result = await session.execute(
            select(Wallet)
            .order_by(Wallet.conviction_score.desc())
            .limit(15)
        )
        wallets = result.scalars().all()

        for w in wallets:
            # Get best category
            cat_result = await session.execute(
                select(WalletCategoryScore)
                .where(WalletCategoryScore.wallet_address == w.address)
                .order_by(WalletCategoryScore.win_rate.desc().nulls_last())
                .limit(1)
            )
            best_cat = cat_result.scalar_one_or_none()

            pnl_str = f"${float(w.total_pnl_usdc or 0):,.0f}"
            class_style = {
                "COPYABLE": "[bold green]COPYABLE[/]",
                "WATCH": "[yellow]WATCH[/]",
                "REJECT": "[red]REJECT[/]",
            }.get(w.copyability_class, w.copyability_class)

            table.add_row(
                w.address[:10] + "...",
                pnl_str,
                str(w.conviction_score),
                class_style,
                best_cat.category if best_cat else "-",
                f"{float(best_cat.win_rate or 0):.0%}" if best_cat else "-",
                f"{float(w.trades_per_month or 0):.0f}",
            )

    return table


async def _market_table() -> Table:
    table = Table(title="Market Universe", show_lines=True)
    table.add_column("Category", style="cyan")
    table.add_column("Count", justify="right")
    table.add_column("Next Resolution", justify="right")

    async with async_session() as session:
        from sqlalchemy import distinct
        result = await session.execute(
            select(
                Market.category,
                func.count(Market.condition_id),
                func.min(Market.resolution_time),
            )
            .where(Market.resolved == False)
            .group_by(Market.category)
        )
        rows = result.all()

        for cat, count, next_res in rows:
            res_str = next_res.strftime("%m/%d %H:%M") if next_res else "-"
            table.add_row(cat or "other", str(count), res_str)

    return table


async def _signals_table() -> Table:
    table = Table(title="Recent Signals", show_lines=True)
    table.add_column("ID", style="dim")
    table.add_column("Type", style="cyan")
    table.add_column("Market", max_width=30)
    table.add_column("Side")
    table.add_column("Status", style="bold")
    table.add_column("Wallets", justify="right")

    async with async_session() as session:
        result = await session.execute(
            select(Signal, Market)
            .join(Market, Signal.condition_id == Market.condition_id)
            .order_by(Signal.created_at.desc())
            .limit(10)
        )
        rows = result.all()

        for signal, market in rows:
            status_style = {
                "PENDING": "[yellow]PENDING[/]",
                "EXECUTED": "[green]EXECUTED[/]",
                "SKIPPED": "[red]SKIPPED[/]",
                "EXPIRED": "[dim]EXPIRED[/]",
            }.get(signal.status, signal.status)

            table.add_row(
                str(signal.id),
                signal.signal_type,
                (market.question or "")[:30],
                signal.outcome,
                status_style,
                str(signal.convergence_count),
            )

    return table


async def _pnl_panel() -> Panel:
    pnl = await get_portfolio_pnl()
    lines = [
        f"Total P&L: ${pnl['total_pnl']:+,.2f}",
        f"Win Rate: {pnl['win_rate']:.0%} ({pnl['wins']}W / {pnl['losses']}L)",
        f"Profit Factor: {pnl['profit_factor']:.2f}",
        f"Expectancy: ${pnl['expectancy']:+.2f}/trade",
        f"Open Positions: {pnl['open_positions']} (${pnl['open_exposure']:,.0f})",
    ]
    return Panel("\n".join(lines), title="Portfolio", border_style="green")


async def render_dashboard() -> None:
    """Render a single frame of the dashboard."""
    try:
        wallet_tbl = await _wallet_table()
        market_tbl = await _market_table()
        signals_tbl = await _signals_table()
        pnl_panel = await _pnl_panel()

        console.clear()
        console.print(Panel("Polymarket Whale Bot", style="bold blue"))
        console.print(pnl_panel)
        console.print(wallet_tbl)
        console.print(market_tbl)
        console.print(signals_tbl)
    except Exception as e:
        console.print(f"[red]Dashboard error: {e}[/]")


async def run_dashboard(refresh_interval: float = 30):
    """Run the dashboard with periodic refresh."""
    logger.info("Dashboard started (refresh every %ds)", refresh_interval)
    while True:
        await render_dashboard()
        await asyncio.sleep(refresh_interval)
