"""Analyze weather-market trading behavior of 5 target whales.

Usage:
    python3 -m scripts.weather_whale_analysis --ingest   # pull fresh trades first
    python3 -m scripts.weather_whale_analysis            # skip ingest, just analyze
"""

import argparse
import asyncio
import logging
import re
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass

from sqlalchemy import select, and_

from src.db import async_session
from src.indexer.market_ingester import ensure_market
from src.indexer.wallet_ingester import ingest_wallet
from src.models import Market, Wallet, WhaleTrade
from src.polymarket.data_api import DataAPIClient
from src.scorer.performance import compute_performance

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
)
log = logging.getLogger("wwa")

WHALES = [
    "0x69730b8697ffcac061e5e7888fd5a220b288b296",
    "0xc34f6b088bb9172625ee1ea2ee8da9ac4f037d2e",
    "0x5a3b0183ccdc34989fb4c58e853e2a6ef6f1957b",
    "0x044f334595a7fd42c143e11c8ec47f23c8d1d1f1",
    "0x8aa29c27241b6909a7c4d6cb4f400267aa215a0b",
    "0x85bd6a676a1b399f849b22332b1f7eca15c87f75",
    "0xca710c8aeb10752df279055a82a3c0a53c09610b",
]

CITY_PATTERNS = [
    "Los Angeles", "New York City", "New York", "San Francisco",
    "Washington D.C.", "Washington", "Miami", "Chicago", "Dallas",
    "Houston", "Austin", "Philadelphia", "Phoenix", "Seattle",
    "Denver", "Boston", "Atlanta", "Detroit", "Minneapolis",
    "Portland", "San Diego", "Las Vegas", "Orlando", "Tampa",
    "Nashville", "St. Louis", "Kansas City",
]

PRICE_BUCKETS = [
    (0.00, 0.20, "0.00-0.20"),
    (0.20, 0.40, "0.20-0.40"),
    (0.40, 0.60, "0.40-0.60"),
    (0.60, 0.80, "0.60-0.80"),
    (0.80, 0.95, "0.80-0.95"),
    (0.95, 1.01, "0.95+"),
]


def classify_shape(question: str) -> str:
    q = question.lower()
    if "between" in q and "°f" in q and re.search(r"\d+[- ]\d+\s*°f", q):
        return "bucket"
    if "or below" in q or "or lower" in q:
        return "ceiling"
    if "or above" in q or "or higher" in q:
        return "floor"
    return "other"


def extract_city(question: str) -> str:
    for city in CITY_PATTERNS:
        if city.lower() in question.lower():
            return city
    return "?"


def price_bucket(price: float) -> str:
    for lo, hi, label in PRICE_BUCKETS:
        if lo <= price < hi:
            return label
    return "?"


@dataclass
class PositionPnL:
    condition_id: str
    question: str
    outcome: str
    market_outcome: str | None
    resolved: bool
    buy_cost: float
    buy_contracts: float
    sell_revenue: float
    sell_contracts: float
    avg_buy_price: float
    first_ts: object
    last_ts: object

    @property
    def pnl(self) -> float | None:
        if not self.resolved or self.market_outcome is None:
            return None
        remaining = max(self.buy_contracts - self.sell_contracts, 0)
        won = self.outcome.upper() == self.market_outcome.upper()
        payout = remaining * (1.0 if won else 0.0)
        return self.sell_revenue + payout - self.buy_cost

    @property
    def is_win(self) -> bool | None:
        p = self.pnl
        if p is None:
            return None
        return p > 0


async def ingest_all():
    log.info("Ingesting fresh trades for %d wallets (full backfill)...", len(WHALES))
    client = DataAPIClient()
    all_slugs: dict[str, str] = {}  # condition_id -> slug
    try:
        for addr in WHALES:
            try:
                new = await asyncio.wait_for(
                    ingest_wallet(addr, full_backfill=True, client=client),
                    timeout=240,
                )
                log.info("  %s  +%d new trades", addr[:12], new)
                # Collect slugs for market backfill
                activity = await client.get_all_activity(addr)
                for a in activity:
                    cid = getattr(a, "condition_id", None) or getattr(a, "market", None)
                    slug = getattr(a, "slug", None)
                    if cid and slug and cid not in all_slugs:
                        all_slugs[cid] = slug
            except asyncio.TimeoutError:
                log.warning("  %s  TIMEOUT", addr[:12])
            except Exception as e:
                log.error("  %s  ERROR: %s", addr[:12], str(e)[:100])

        log.info("Backfilling markets for %d unique condition_ids...", len(all_slugs))
        done = 0
        skipped = 0
        for cid, slug in all_slugs.items():
            try:
                m = await ensure_market(cid, slug=slug)
                if m is None:
                    skipped += 1
                else:
                    done += 1
            except Exception as e:
                skipped += 1
                log.debug("  ensure_market failed for %s: %s", cid[:10], e)
            if (done + skipped) % 100 == 0:
                log.info("  progress: %d loaded / %d skipped", done, skipped)
        log.info("Market backfill complete: %d loaded, %d skipped", done, skipped)
    finally:
        await client.close()


async def fetch_weather_trades(session, addr: str):
    q = (
        select(WhaleTrade, Market)
        .join(Market, WhaleTrade.condition_id == Market.condition_id)
        .where(and_(WhaleTrade.wallet_address == addr, Market.category == "weather"))
        .order_by(WhaleTrade.timestamp)
    )
    res = await session.execute(q)
    return res.all()


def build_positions(rows) -> list[PositionPnL]:
    positions: dict[tuple[str, str], PositionPnL] = {}
    for trade, market in rows:
        key = (trade.condition_id, trade.outcome.upper())
        pos = positions.get(key)
        if pos is None:
            pos = PositionPnL(
                condition_id=trade.condition_id,
                question=market.question,
                outcome=trade.outcome.upper(),
                market_outcome=market.outcome,
                resolved=market.resolved,
                buy_cost=0.0,
                buy_contracts=0.0,
                sell_revenue=0.0,
                sell_contracts=0.0,
                avg_buy_price=0.0,
                first_ts=trade.timestamp,
                last_ts=trade.timestamp,
            )
            positions[key] = pos
        price = float(trade.price or 0)
        contracts = float(trade.num_contracts or 0)
        if trade.side == "BUY":
            pos.buy_cost += price * contracts
            pos.buy_contracts += contracts
        elif trade.side == "SELL":
            pos.sell_revenue += price * contracts
            pos.sell_contracts += contracts
        if trade.timestamp < pos.first_ts:
            pos.first_ts = trade.timestamp
        if trade.timestamp > pos.last_ts:
            pos.last_ts = trade.timestamp
    for pos in positions.values():
        if pos.buy_contracts > 0:
            pos.avg_buy_price = pos.buy_cost / pos.buy_contracts
    return list(positions.values())


def render_bar(count: int, maxv: int, width: int = 30) -> str:
    if maxv == 0:
        return ""
    return "█" * int(width * count / maxv)


async def analyze_wallet(session, addr: str) -> dict:
    wallet = await session.get(Wallet, addr)
    name = (wallet.display_name if wallet else None) or addr[:10]

    rows = await fetch_weather_trades(session, addr)
    if not rows:
        return {"addr": addr, "name": name, "trades": 0}

    # Raw trade-level stats
    buy_trades = [t for t, _ in rows if t.side == "BUY"]
    sell_trades = [t for t, _ in rows if t.side == "SELL"]
    buy_usdc = sum(float(t.size_usdc or 0) for t in buy_trades)
    sell_usdc = sum(float(t.size_usdc or 0) for t in sell_trades)

    buy_yes = sum(1 for t in buy_trades if t.outcome.upper() == "YES")
    buy_no = sum(1 for t in buy_trades if t.outcome.upper() == "NO")

    buy_prices = [float(t.price or 0) for t in buy_trades]
    price_hist = Counter()
    for p in buy_prices:
        price_hist[price_bucket(p)] += 1

    shape_hist = Counter()
    city_hist = Counter()
    seen_markets = set()
    for t, m in rows:
        if m.condition_id in seen_markets:
            continue
        seen_markets.add(m.condition_id)
        shape_hist[classify_shape(m.question)] += 1
        city_hist[extract_city(m.question)] += 1

    positions = build_positions(rows)
    resolved_positions = [p for p in positions if p.resolved and p.buy_contracts > 0]
    wins = [p for p in resolved_positions if p.is_win]
    losses = [p for p in resolved_positions if p.is_win is False]
    pnl_realized = sum(p.pnl or 0 for p in resolved_positions)
    total_wins = sum(p.pnl for p in wins)
    total_losses = sum(-p.pnl for p in losses)
    wr_unfiltered = len(wins) / (len(wins) + len(losses)) if (wins or losses) else 0
    pf_unfiltered = (total_wins / total_losses) if total_losses > 0 else float("inf") if total_wins > 0 else 0

    # compute_performance applies max_entry_price_trade filter (0.90 default)
    perf = await compute_performance(session, addr, category="weather")

    # Hold hours (time between first buy and last activity in position)
    hold_hours_list = []
    for p in resolved_positions:
        if p.first_ts and p.last_ts:
            hold_hours_list.append((p.last_ts - p.first_ts).total_seconds() / 3600)
    avg_hold_pos = statistics.mean(hold_hours_list) if hold_hours_list else 0

    # Position sizes
    pos_sizes = [p.buy_cost for p in positions if p.buy_contracts > 0]
    avg_size = statistics.mean(pos_sizes) if pos_sizes else 0
    med_size = statistics.median(pos_sizes) if pos_sizes else 0
    max_size = max(pos_sizes) if pos_sizes else 0

    # Top 5 wins / losses by abs PnL
    scored = [(p, p.pnl) for p in resolved_positions if p.pnl is not None]
    top_wins = sorted((x for x in scored if x[1] > 0), key=lambda x: -x[1])[:5]
    top_losses = sorted((x for x in scored if x[1] < 0), key=lambda x: x[1])[:5]

    return {
        "addr": addr,
        "name": name,
        "trades": len(rows),
        "buy_count": len(buy_trades),
        "sell_count": len(sell_trades),
        "buy_usdc": buy_usdc,
        "sell_usdc": sell_usdc,
        "buy_yes": buy_yes,
        "buy_no": buy_no,
        "markets": len(seen_markets),
        "price_hist": price_hist,
        "shape_hist": shape_hist,
        "city_hist": city_hist,
        "positions_resolved": len(resolved_positions),
        "wins": len(wins),
        "losses": len(losses),
        "wr_unfiltered": wr_unfiltered,
        "pf_unfiltered": pf_unfiltered,
        "pnl_realized": pnl_realized,
        "total_wins_usdc": total_wins,
        "total_losses_usdc": total_losses,
        "perf_filtered": perf,  # entries <=0.90 only
        "avg_hold_hours": avg_hold_pos,
        "avg_size": avg_size,
        "med_size": med_size,
        "max_size": max_size,
        "top_wins": top_wins,
        "top_losses": top_losses,
        "first_trade": min(t.timestamp for t, _ in rows),
        "last_trade": max(t.timestamp for t, _ in rows),
    }


def print_wallet_report(s: dict) -> None:
    print("\n" + "=" * 78)
    print(f"{s['name']}  ({s['addr']})")
    print("=" * 78)
    if s["trades"] == 0:
        print("  No weather trades.")
        return

    print(f"  Trades:       {s['trades']}  (BUY {s['buy_count']} / SELL {s['sell_count']})")
    print(f"  Unique markets: {s['markets']}")
    print(f"  Window:       {s['first_trade'].date()} → {s['last_trade'].date()}")
    print(f"  Volume:       BUY ${s['buy_usdc']:,.0f}  |  SELL ${s['sell_usdc']:,.0f}")
    print(f"  Buy mix:      YES {s['buy_yes']}  |  NO {s['buy_no']}")

    print(f"\n  Resolved positions: {s['positions_resolved']}  "
          f"(W {s['wins']} / L {s['losses']})")
    print(f"  Win rate:     {s['wr_unfiltered']*100:.1f}%  "
          f"(all entries)   "
          f"{s['perf_filtered'].win_rate*100:.1f}% (entries ≤0.90, n={s['perf_filtered'].trade_count})")
    pf = s["pf_unfiltered"]
    pf_str = f"{pf:.2f}" if pf != float("inf") else "∞"
    pf_filt = s["perf_filtered"].profit_factor
    print(f"  Profit factor: {pf_str}  (all)   {pf_filt} (≤0.90)")
    print(f"  Realized PnL:  ${s['pnl_realized']:+,.2f}  "
          f"(wins ${s['total_wins_usdc']:,.0f} / losses ${s['total_losses_usdc']:,.0f})")
    print(f"  Expectancy (≤0.90):  ${float(s['perf_filtered'].expectancy):+.2f}/trade")
    print(f"  Avg hold:      {s['avg_hold_hours']:.1f}h  (entry → last activity)")
    print(f"  Position size: avg ${s['avg_size']:.0f}  "
          f"median ${s['med_size']:.0f}  max ${s['max_size']:.0f}")

    print("\n  Entry price distribution (BUY trades):")
    maxv = max(s["price_hist"].values()) if s["price_hist"] else 0
    for _, _, label in PRICE_BUCKETS:
        c = s["price_hist"].get(label, 0)
        print(f"    {label:10s} {c:4d}  {render_bar(c, maxv)}")

    print("\n  Market shape (unique markets):")
    for shape, c in s["shape_hist"].most_common():
        print(f"    {shape:10s} {c}")

    print("\n  Top cities (unique markets):")
    for city, c in s["city_hist"].most_common(8):
        print(f"    {city:20s} {c}")

    if s["top_wins"]:
        print("\n  Top 5 winning positions:")
        for p, pnl in s["top_wins"]:
            print(f"    ${pnl:+8.2f}  {p.outcome}@{p.avg_buy_price:.2f}  {p.question[:60]}")
    if s["top_losses"]:
        print("\n  Top 5 losing positions:")
        for p, pnl in s["top_losses"]:
            print(f"    ${pnl:+8.2f}  {p.outcome}@{p.avg_buy_price:.2f}  {p.question[:60]}")


def print_summary(results: list[dict]) -> None:
    print("\n" + "#" * 78)
    print("CROSS-WHALE SUMMARY")
    print("#" * 78)
    print(f"{'whale':20s} {'trades':>7s} {'mkts':>5s} {'WR':>6s} {'PF':>6s} "
          f"{'PnL$':>10s} {'avg$':>7s} {'top-city':<20s}")
    print("-" * 78)
    for s in results:
        if s["trades"] == 0:
            print(f"{s['name'][:20]:20s} {'—':>7s}")
            continue
        top_city = s["city_hist"].most_common(1)[0][0] if s["city_hist"] else "?"
        wr = f"{s['wr_unfiltered']*100:.0f}%"
        pf = s["pf_unfiltered"]
        pf_str = f"{pf:.1f}" if pf != float("inf") else "∞"
        print(f"{s['name'][:20]:20s} {s['trades']:>7d} {s['markets']:>5d} "
              f"{wr:>6s} {pf_str:>6s} {s['pnl_realized']:>+10.0f} "
              f"{s['avg_size']:>7.0f} {top_city[:20]:<20s}")


def print_insights(results: list[dict]) -> None:
    active = [s for s in results if s["trades"] > 0]
    if not active:
        print("\nNo whales have weather trades.")
        return

    print("\n" + "#" * 78)
    print("WHAT THE PROFITABLE WHALES ARE DOING")
    print("#" * 78)

    winners = [s for s in active if s["pnl_realized"] > 0]
    losers = [s for s in active if s["pnl_realized"] <= 0]

    print(f"\nNet-positive on weather: {len(winners)}/{len(active)} active whales")
    for s in sorted(winners, key=lambda x: -x["pnl_realized"]):
        print(f"  + {s['name']:18s} ${s['pnl_realized']:+,.0f}  "
              f"WR {s['wr_unfiltered']*100:.0f}%  "
              f"PF {s['pf_unfiltered'] if s['pf_unfiltered']!=float('inf') else '∞':.2f}  "
              f"n={s['positions_resolved']}")
    for s in sorted(losers, key=lambda x: x["pnl_realized"]):
        print(f"  - {s['name']:18s} ${s['pnl_realized']:+,.0f}  "
              f"WR {s['wr_unfiltered']*100:.0f}%  n={s['positions_resolved']}")

    print("\nSide/outcome bias (BUY trades):")
    for s in active:
        total = s["buy_yes"] + s["buy_no"]
        if total == 0:
            continue
        yes_pct = 100 * s["buy_yes"] / total
        print(f"  {s['name']:18s} YES {yes_pct:4.0f}%   NO {100-yes_pct:4.0f}%")

    print("\nEntry-price lean (where BUY volume clusters):")
    for s in active:
        ph = s["price_hist"]
        if not ph:
            continue
        total = sum(ph.values())
        low = sum(ph.get(b, 0) for b in ["0.00-0.20", "0.20-0.40"])
        mid = sum(ph.get(b, 0) for b in ["0.40-0.60", "0.60-0.80"])
        high = sum(ph.get(b, 0) for b in ["0.80-0.95", "0.95+"])
        print(f"  {s['name']:18s} low<0.40 {100*low/total:4.0f}%   "
              f"mid 0.40-0.80 {100*mid/total:4.0f}%   "
              f"high>0.80 {100*high/total:4.0f}%")

    print("\nMarket-shape preference:")
    for s in active:
        sh = s["shape_hist"]
        if not sh:
            continue
        parts = [f"{shape}={c}" for shape, c in sh.most_common()]
        print(f"  {s['name']:18s} {'  '.join(parts)}")

    print("\nHold time (first buy → last activity on position):")
    for s in active:
        if s["positions_resolved"] == 0:
            continue
        print(f"  {s['name']:18s} avg {s['avg_hold_hours']:.1f}h  "
              f"(~{s['avg_hold_hours']/24:.1f} days)")


async def main(ingest: bool):
    if ingest:
        await ingest_all()

    results = []
    async with async_session() as session:
        for addr in WHALES:
            log.info("Analyzing %s...", addr[:12])
            r = await analyze_wallet(session, addr)
            results.append(r)

    for r in results:
        print_wallet_report(r)
    print_summary(results)
    print_insights(results)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--ingest", action="store_true",
                    help="Pull fresh trades before analyzing")
    args = ap.parse_args()
    asyncio.run(main(args.ingest))
