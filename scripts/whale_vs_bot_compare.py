"""Per-day whale-vs-bot post-mortem for weather markets.

For every weather market resolving on a given date, print a side-by-side block
showing what each watched whale did, what our bot did, and where the two
diverged (skipped signals, partial fills, slippage). Read-only.

Usage:
    python3 -m scripts.whale_vs_bot_compare 2026-04-25
"""

import argparse
import asyncio
import logging
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import select, and_, cast
from sqlalchemy.types import Date as SQLDate

from src.db import async_session
from src.models import Market, MarketToken, MyTrade, Signal, WhaleTrade
from src.polymarket.clob_client import CLOBClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
)
log = logging.getLogger("wvb")

WHALES = [
    "0x8aa29c27241b6909a7c4d6cb4f400267aa215a0b",
    "0x0eeea56c3509e68fb655d0a2ddadc303957274e8",
    "0x9c68b13a2c9b6d2e80826f26cea746cc22ba7936",
]

WHALE_LABELS = {addr: addr[:10] for addr in WHALES}


@dataclass
class Position:
    """Aggregated buys+sells for a single (actor, condition_id, outcome)."""
    buy_cost: float = 0.0
    buy_contracts: float = 0.0
    sell_revenue: float = 0.0
    sell_contracts: float = 0.0
    first_ts: datetime | None = None
    last_ts: datetime | None = None

    @property
    def avg_buy_price(self) -> float:
        return self.buy_cost / self.buy_contracts if self.buy_contracts else 0.0

    @property
    def remaining_contracts(self) -> float:
        return max(self.buy_contracts - self.sell_contracts, 0.0)

    def realized_pnl(self, market_outcome: str | None, position_outcome: str) -> float | None:
        if market_outcome is None:
            return None
        won = market_outcome.upper() == position_outcome.upper()
        payout = self.remaining_contracts * (1.0 if won else 0.0)
        return self.sell_revenue + payout - self.buy_cost

    def mtm_pnl(self, mid: float | None) -> float | None:
        if mid is None:
            return None
        return self.sell_revenue + self.remaining_contracts * mid - self.buy_cost


@dataclass
class MarketBlock:
    market: Market
    yes_token_id: str | None = None
    no_token_id: str | None = None
    yes_mid: float | None = None
    no_mid: float | None = None
    # keyed by (actor, outcome) where actor is a whale address or "bot"
    positions: dict[tuple[str, str], Position] = field(default_factory=dict)
    # signals where the bot did NOT trade — list of (whale_address, outcome, status, reason)
    skips: list[tuple[str, str, str, str | None]] = field(default_factory=list)


def parse_date(s: str) -> date:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        sys.exit(f"Invalid date: {s!r} (expected YYYY-MM-DD)")


async def load_markets(session, target_date: date) -> list[Market]:
    q = (
        select(Market)
        .where(
            and_(
                Market.category == "weather",
                cast(Market.resolution_time, SQLDate) == target_date,
            )
        )
        .order_by(Market.resolution_time)
    )
    return list((await session.execute(q)).scalars())


async def load_tokens(session, condition_ids: list[str]) -> dict[str, dict[str, str]]:
    """Returns {condition_id: {"YES": token_id, "NO": token_id}}."""
    if not condition_ids:
        return {}
    q = select(MarketToken).where(MarketToken.condition_id.in_(condition_ids))
    out: dict[str, dict[str, str]] = defaultdict(dict)
    for tok in (await session.execute(q)).scalars():
        out[tok.condition_id][tok.outcome.upper()] = tok.token_id
    return out


async def load_whale_trades(session, condition_ids: list[str]) -> list[WhaleTrade]:
    if not condition_ids:
        return []
    q = (
        select(WhaleTrade)
        .where(
            and_(
                WhaleTrade.condition_id.in_(condition_ids),
                WhaleTrade.wallet_address.in_(WHALES),
            )
        )
        .order_by(WhaleTrade.timestamp)
    )
    return list((await session.execute(q)).scalars())


async def load_my_trades(session, condition_ids: list[str]) -> list[MyTrade]:
    if not condition_ids:
        return []
    q = select(MyTrade).where(MyTrade.condition_id.in_(condition_ids))
    return list((await session.execute(q)).scalars())


async def load_skips(session, condition_ids: list[str]) -> list[Signal]:
    if not condition_ids:
        return []
    q = select(Signal).where(
        and_(
            Signal.condition_id.in_(condition_ids),
            Signal.status.in_(["SKIPPED", "REJECTED"]),
        )
    )
    return list((await session.execute(q)).scalars())


def aggregate_whale_trades(trades: list[WhaleTrade], blocks: dict[str, MarketBlock]) -> None:
    for t in trades:
        block = blocks.get(t.condition_id)
        if block is None:
            continue
        actor = t.wallet_address.lower()
        outcome = t.outcome.upper()
        key = (actor, outcome)
        pos = block.positions.setdefault(key, Position())
        price = float(t.price or 0)
        contracts = float(t.num_contracts or 0)
        if contracts == 0 and price > 0:
            contracts = float(t.size_usdc or 0) / price
        if t.side == "BUY":
            pos.buy_cost += price * contracts
            pos.buy_contracts += contracts
        elif t.side == "SELL":
            pos.sell_revenue += price * contracts
            pos.sell_contracts += contracts
        if pos.first_ts is None or t.timestamp < pos.first_ts:
            pos.first_ts = t.timestamp
        if pos.last_ts is None or t.timestamp > pos.last_ts:
            pos.last_ts = t.timestamp


def aggregate_my_trades(trades: list[MyTrade], blocks: dict[str, MarketBlock]) -> None:
    """One MyTrade row = one entry (and possibly an exit if exit_price set).

    Reconstructs cost basis and revenue rather than trusting pnl_usdc, because
    the reconciler is known to mislabel resolved trades.
    """
    for t in trades:
        if t.fill_status not in ("FILLED", "PARTIAL", "PAPER"):
            continue
        block = blocks.get(t.condition_id)
        if block is None:
            continue
        outcome = t.outcome.upper()
        key = ("bot", outcome)
        pos = block.positions.setdefault(key, Position())

        entry_price = float(t.entry_price or 0)
        contracts = float(t.num_contracts or 0)
        if contracts == 0 and entry_price > 0:
            contracts = float(t.size_usdc or 0) / entry_price
        if entry_price and contracts:
            pos.buy_cost += entry_price * contracts
            pos.buy_contracts += contracts
            if pos.first_ts is None or (t.entry_timestamp and t.entry_timestamp < pos.first_ts):
                pos.first_ts = t.entry_timestamp

        if t.exit_price is not None and t.exit_timestamp is not None:
            exit_price = float(t.exit_price)
            pos.sell_revenue += exit_price * contracts
            pos.sell_contracts += contracts
            if pos.last_ts is None or t.exit_timestamp > pos.last_ts:
                pos.last_ts = t.exit_timestamp
        else:
            if pos.last_ts is None or (t.entry_timestamp and t.entry_timestamp > (pos.last_ts or t.entry_timestamp)):
                pos.last_ts = t.entry_timestamp


def attach_skips(skips: list[Signal], blocks: dict[str, MarketBlock]) -> None:
    """Only attach skips that reference one of our 3 watched whales."""
    whale_set = set(w.lower() for w in WHALES)
    for s in skips:
        block = blocks.get(s.condition_id)
        if block is None:
            continue
        wallets = [w.lower() for w in (s.source_wallets or []) if w.lower() in whale_set]
        if not wallets:
            continue
        for w in wallets:
            block.skips.append((w, s.outcome.upper(), s.status, s.status_reason))


async def fill_midpoints(blocks: dict[str, MarketBlock], tokens_by_market: dict[str, dict[str, str]]) -> None:
    """For unresolved markets, fetch live midpoint per token. Skip resolved markets."""
    clob = CLOBClient()
    try:
        for cid, block in blocks.items():
            block.yes_token_id = tokens_by_market.get(cid, {}).get("YES")
            block.no_token_id = tokens_by_market.get(cid, {}).get("NO")
            if block.market.resolved:
                continue
            if block.yes_token_id:
                block.yes_mid = await clob.get_midpoint(block.yes_token_id)
            if block.no_token_id:
                block.no_mid = await clob.get_midpoint(block.no_token_id)
    finally:
        await clob.close()


def fmt_money(v: float | None, width: int = 9) -> str:
    if v is None:
        return f"{'—':>{width}}"
    return f"{v:+{width-1}.2f}"


def fmt_price(v: float | None) -> str:
    if v is None or v == 0:
        return "  —  "
    return f"{v:.3f}"


def render_market_block(block: MarketBlock) -> None:
    m = block.market
    status = f"resolved {m.outcome}" if m.resolved and m.outcome else "unresolved"
    print(f"\n=== {m.resolution_time:%Y-%m-%d %H:%MZ}  {m.question}  [{status}]  ===")
    print(f"    cid={m.condition_id[:14]}  vol=${float(m.volume_usdc or 0):,.0f}  "
          f"liq=${float(m.liquidity_usdc or 0):,.0f}")
    if not m.resolved:
        print(f"    midpoints  YES={fmt_price(block.yes_mid)}  NO={fmt_price(block.no_mid)}")

    header = f"    {'actor':<14} {'side':<4} {'contracts':>10} {'avg buy':>9} {'sold':>10} {'remain':>9} {'realized':>10} {'MTM':>10}"
    print(header)
    print("    " + "-" * (len(header) - 4))

    rows_to_print = []
    # one row per whale, even if no trade — makes divergence visible
    for whale in WHALES:
        for outcome in ("YES", "NO"):
            pos = block.positions.get((whale, outcome))
            if pos is None or pos.buy_contracts == 0:
                continue
            mid = block.yes_mid if outcome == "YES" else block.no_mid
            rpnl = pos.realized_pnl(m.outcome, outcome) if m.resolved else None
            mtm = pos.mtm_pnl(mid) if not m.resolved else None
            rows_to_print.append((
                f"whale {WHALE_LABELS[whale]}", outcome,
                pos.buy_contracts, pos.avg_buy_price,
                pos.sell_contracts, pos.remaining_contracts,
                rpnl, mtm,
            ))
        if not any(block.positions.get((whale, o)) for o in ("YES", "NO")):
            rows_to_print.append((f"whale {WHALE_LABELS[whale]}", "—",
                                  0.0, 0.0, 0.0, 0.0, None, None))

    for outcome in ("YES", "NO"):
        pos = block.positions.get(("bot", outcome))
        if pos is None or pos.buy_contracts == 0:
            continue
        mid = block.yes_mid if outcome == "YES" else block.no_mid
        rpnl = pos.realized_pnl(m.outcome, outcome) if m.resolved else None
        mtm = pos.mtm_pnl(mid) if not m.resolved else None
        rows_to_print.append((
            "bot", outcome,
            pos.buy_contracts, pos.avg_buy_price,
            pos.sell_contracts, pos.remaining_contracts,
            rpnl, mtm,
        ))
    if not any(block.positions.get(("bot", o)) for o in ("YES", "NO")):
        rows_to_print.append(("bot", "—", 0.0, 0.0, 0.0, 0.0, None, None))

    for actor, side, c, avg, sold, rem, rpnl, mtm in rows_to_print:
        if side == "—":
            print(f"    {actor:<14} {'—':<4} {'(no trade)':>10}")
            continue
        print(
            f"    {actor:<14} {side:<4} "
            f"{c:>10.1f} {avg:>9.3f} {sold:>10.1f} {rem:>9.1f} "
            f"{fmt_money(rpnl, 10)} {fmt_money(mtm, 10)}"
        )

    # divergence / skip lines
    div_lines = []
    for whale in WHALES:
        whale_total = sum(
            (block.positions.get((whale, o)) or Position()).buy_contracts
            for o in ("YES", "NO")
        )
        bot_total = sum(
            (block.positions.get(("bot", o)) or Position()).buy_contracts
            for o in ("YES", "NO")
        )
        if whale_total > 0 and bot_total > 0:
            ratio = bot_total / whale_total
            div_lines.append(f"copy ratio  {WHALE_LABELS[whale]}: {bot_total:.1f}/{whale_total:.1f} = {ratio*100:.0f}%")
        elif whale_total > 0 and bot_total == 0:
            div_lines.append(f"NOT COPIED  {WHALE_LABELS[whale]}: whale {whale_total:.1f} contracts, bot 0")

    for whale, outcome, status, reason in block.skips:
        div_lines.append(f"skip {WHALE_LABELS[whale]} {outcome}: {status} ({reason or 'no reason'})")

    for line in div_lines:
        print(f"    » {line}")


def render_summary(target_date: date, blocks: dict[str, MarketBlock]) -> None:
    print("\n" + "#" * 80)
    print(f"SUMMARY  {target_date}")
    print("#" * 80)

    per_whale = {w: {"markets": 0, "whale_pnl": 0.0, "bot_copied": 0,
                     "bot_pnl": 0.0, "skips": 0} for w in WHALES}
    bot_total_pnl = 0.0
    bot_market_count = 0
    bot_seen = set()

    for cid, block in blocks.items():
        m = block.market
        for whale in WHALES:
            had_position = False
            for outcome in ("YES", "NO"):
                pos = block.positions.get((whale, outcome))
                if pos is None or pos.buy_contracts == 0:
                    continue
                had_position = True
                mid = block.yes_mid if outcome == "YES" else block.no_mid
                pnl = (pos.realized_pnl(m.outcome, outcome) if m.resolved else pos.mtm_pnl(mid)) or 0.0
                per_whale[whale]["whale_pnl"] += pnl
            if had_position:
                per_whale[whale]["markets"] += 1
                bot_present = any(
                    (block.positions.get(("bot", o)) or Position()).buy_contracts > 0
                    for o in ("YES", "NO")
                )
                if bot_present:
                    per_whale[whale]["bot_copied"] += 1
            per_whale[whale]["skips"] += sum(1 for s in block.skips if s[0] == whale)

        for outcome in ("YES", "NO"):
            pos = block.positions.get(("bot", outcome))
            if pos is None or pos.buy_contracts == 0:
                continue
            if cid not in bot_seen:
                bot_seen.add(cid)
                bot_market_count += 1
            mid = block.yes_mid if outcome == "YES" else block.no_mid
            pnl = (pos.realized_pnl(m.outcome, outcome) if m.resolved else pos.mtm_pnl(mid)) or 0.0
            bot_total_pnl += pnl

    print(f"  {'whale':<14} {'markets':>8} {'whale PnL':>12} {'bot copied':>12} {'skips':>7}")
    for whale in WHALES:
        s = per_whale[whale]
        print(f"  {WHALE_LABELS[whale]:<14} {s['markets']:>8d} "
              f"{s['whale_pnl']:>+12.2f} {s['bot_copied']:>9d}/{s['markets']:<2d} {s['skips']:>7d}")
    print(f"\n  bot total: {bot_market_count} markets traded, PnL ${bot_total_pnl:+,.2f} "
          f"({'realized' if all(b.market.resolved for b in blocks.values()) else 'mixed realized + MTM'})")


async def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("date", help="YYYY-MM-DD (resolution date)")
    args = ap.parse_args()
    target_date = parse_date(args.date)

    async with async_session() as session:
        markets = await load_markets(session, target_date)
        if not markets:
            print(f"No weather markets resolved on {target_date}")
            return
        log.info("Loaded %d weather markets resolving on %s", len(markets), target_date)

        cids = [m.condition_id for m in markets]
        tokens = await load_tokens(session, cids)
        whale_trades = await load_whale_trades(session, cids)
        my_trades = await load_my_trades(session, cids)
        skips = await load_skips(session, cids)

    blocks: dict[str, MarketBlock] = {m.condition_id: MarketBlock(market=m) for m in markets}
    aggregate_whale_trades(whale_trades, blocks)
    aggregate_my_trades(my_trades, blocks)
    attach_skips(skips, blocks)
    await fill_midpoints(blocks, tokens)

    for cid in cids:
        render_market_block(blocks[cid])
    render_summary(target_date, blocks)


if __name__ == "__main__":
    asyncio.run(main())
