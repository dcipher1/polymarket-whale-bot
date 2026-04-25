"""Performance metrics: win rate, profit factor, gain/loss ratio, expectancy."""

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.models import WhaleTrade, Market

logger = logging.getLogger(__name__)


@dataclass
class PerformanceMetrics:
    win_rate: float = 0.0
    profit_factor: float = 0.0
    gain_loss_ratio: float = 0.0
    expectancy: float = 0.0
    trade_count: int = 0
    win_count: int = 0
    loss_count: int = 0
    total_wins_usdc: float = 0.0
    total_losses_usdc: float = 0.0
    avg_hold_hours: float = 0.0
    last_trade_ts: datetime | None = None


async def compute_performance(
    session: AsyncSession,
    wallet_address: str,
    category: str | None = None,
) -> PerformanceMetrics:
    """Compute performance metrics from whale_trades on resolved markets.

    For each resolved market, computes true PnL from trade history:
      PnL = sell_revenue + resolution_payout - buy_cost

    This handles both hold-to-resolution and active trading (sell-for-profit).
    """

    # Get all trades on resolved markets
    query = (
        select(WhaleTrade, Market)
        .join(Market, WhaleTrade.condition_id == Market.condition_id)
        .where(
            and_(
                WhaleTrade.wallet_address == wallet_address,
                Market.resolved == True,
                Market.outcome.isnot(None),
            )
        )
    )
    if category:
        query = query.where(Market.category == category)

    result = await session.execute(query)
    rows = result.all()

    if not rows:
        return PerformanceMetrics()

    # Group trades by (condition_id, outcome) — each is a "market position"
    # Key: (condition_id, outcome) → {buys, sells, market, timestamps}
    positions: dict[tuple[str, str], dict] = defaultdict(lambda: {
        "buy_cost": 0.0,
        "buy_contracts": 0.0,
        "sell_revenue": 0.0,
        "sell_contracts": 0.0,
        "market": None,
        "first_ts": None,
        "last_ts": None,
    })

    for trade, market in rows:
        outcome = trade.outcome.upper()
        key = (trade.condition_id, outcome)
        pos = positions[key]
        pos["market"] = market

        price = float(trade.price or 0)
        contracts = float(trade.num_contracts or 0)
        ts = trade.timestamp

        if trade.side == "BUY":
            pos["buy_cost"] += price * contracts
            pos["buy_contracts"] += contracts
        elif trade.side == "SELL":
            pos["sell_revenue"] += price * contracts
            pos["sell_contracts"] += contracts

        if pos["first_ts"] is None or ts < pos["first_ts"]:
            pos["first_ts"] = ts
        if pos["last_ts"] is None or ts > pos["last_ts"]:
            pos["last_ts"] = ts

    wins = []
    losses = []
    hold_hours = []
    latest_qualifying_ts = None

    for (cid, outcome), pos in positions.items():
        market = pos["market"]
        buy_contracts = pos["buy_contracts"]
        if buy_contracts == 0:
            continue  # sell-only (no initial position) — skip

        # Penny-pick filter: skip high-entry positions
        avg_buy_price = pos["buy_cost"] / buy_contracts
        if avg_buy_price > settings.max_entry_price_trade:
            continue

        # Track most recent qualifying trade
        if pos["last_ts"] and (latest_qualifying_ts is None or pos["last_ts"] > latest_qualifying_ts):
            latest_qualifying_ts = pos["last_ts"]

        # Compute PnL: sell_revenue + resolution_payout - buy_cost
        remaining = max(buy_contracts - pos["sell_contracts"], 0)
        won = outcome == market.outcome.upper()
        resolution_payout = remaining * (1.0 if won else 0.0)
        pnl = pos["sell_revenue"] + resolution_payout - pos["buy_cost"]

        if pnl > 0:
            wins.append(pnl)
        elif pnl < 0:
            losses.append(abs(pnl))
        # pnl == 0 treated as neither

        # Hold duration
        if pos["first_ts"] and pos["last_ts"]:
            delta = pos["last_ts"] - pos["first_ts"]
            hold_hours.append(delta.total_seconds() / 3600)

    total_trades = len(wins) + len(losses)
    if total_trades == 0:
        return PerformanceMetrics()

    total_wins = sum(wins)
    total_losses = sum(losses)

    win_rate = len(wins) / total_trades if total_trades > 0 else 0
    profit_factor = total_wins / total_losses if total_losses > 0 else float("inf") if total_wins > 0 else 0
    avg_win = total_wins / len(wins) if wins else 0
    avg_loss = total_losses / len(losses) if losses else 0
    gain_loss_ratio = avg_win / avg_loss if avg_loss > 0 else float("inf") if avg_win > 0 else 0
    expectancy = (total_wins - total_losses) / total_trades if total_trades > 0 else 0

    avg_hold = sum(hold_hours) / len(hold_hours) if hold_hours else 0

    return PerformanceMetrics(
        win_rate=round(win_rate, 4),
        profit_factor=round(min(profit_factor, 9999), 2),
        gain_loss_ratio=round(min(gain_loss_ratio, 9999), 2),
        expectancy=round(expectancy, 4),
        trade_count=total_trades,
        win_count=len(wins),
        loss_count=len(losses),
        total_wins_usdc=round(total_wins, 2),
        total_losses_usdc=round(total_losses, 2),
        avg_hold_hours=round(avg_hold, 2),
        last_trade_ts=latest_qualifying_ts,
    )
