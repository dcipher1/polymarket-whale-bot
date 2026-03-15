"""Export performance report as CSV."""

import asyncio
import csv
import logging
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import select

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
logger = logging.getLogger(__name__)


async def export():
    from src.db import async_session
    from src.models import MyTrade, Wallet, Signal, Market
    from src.tracking.pnl import get_portfolio_pnl
    from src.tracking.attribution import compute_attribution

    output_dir = Path("data")
    output_dir.mkdir(exist_ok=True)

    # Export trades
    async with async_session() as session:
        result = await session.execute(
            select(MyTrade, Market)
            .join(Market, MyTrade.condition_id == Market.condition_id)
            .order_by(MyTrade.entry_timestamp)
        )
        rows = result.all()

    trades_path = output_dir / "trades_report.csv"
    with open(trades_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "trade_id", "market", "outcome", "entry_price", "exit_price",
            "size_usdc", "contracts", "pnl", "result", "category",
            "entry_time", "exit_time", "source_wallets",
        ])
        for trade, market in rows:
            writer.writerow([
                trade.id,
                (market.question or "")[:60],
                trade.outcome,
                trade.entry_price,
                trade.exit_price,
                trade.size_usdc,
                trade.num_contracts,
                trade.pnl_usdc,
                trade.trade_outcome,
                market.category,
                trade.entry_timestamp,
                trade.exit_timestamp,
                ",".join(trade.source_wallets or []),
            ])

    logger.info("Exported %d trades to %s", len(rows), trades_path)

    # Export attribution
    attribution = await compute_attribution()
    attr_path = output_dir / "attribution_report.csv"
    with open(attr_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["wallet", "total_pnl", "wins", "losses", "trade_count"])
        for addr, data in sorted(attribution.items(), key=lambda x: x[1]["total_pnl"], reverse=True):
            writer.writerow([addr, data["total_pnl"], data["wins"], data["losses"], data["trade_count"]])

    logger.info("Exported attribution to %s", attr_path)

    # Summary
    pnl = await get_portfolio_pnl()
    print(f"\nPortfolio Summary:")
    print(f"  Total P&L: ${pnl['total_pnl']:+,.2f}")
    print(f"  Win Rate: {pnl['win_rate']:.0%}")
    print(f"  Profit Factor: {pnl['profit_factor']:.2f}")
    print(f"  Expectancy: ${pnl['expectancy']:+.2f}/trade")


if __name__ == "__main__":
    asyncio.run(export())
