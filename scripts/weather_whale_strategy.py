"""Per-whale strategy breakdown on weather markets.

Pulls each whale's full trade activity from Polymarket Data API, filters to
weather markets by title, reconstructs position lifecycles, and reports how
each whale trades weather: what markets, what side, what prices, and whether
they hold to resolution or exit early."""

import asyncio
import logging
import re
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable

from src.polymarket.data_api import Activity, DataAPIClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
)
log = logging.getLogger("wws")

WHALES = [
    "0x69730b8697ffcac061e5e7888fd5a220b288b296",
    "0xc34f6b088bb9172625ee1ea2ee8da9ac4f037d2e",
    "0x5a3b0183ccdc34989fb4c58e853e2a6ef6f1957b",
    "0x044f334595a7fd42c143e11c8ec47f23c8d1d1f1",
    "0x8aa29c27241b6909a7c4d6cb4f400267aa215a0b",
    "0x85bd6a676a1b399f849b22332b1f7eca15c87f75",
    "0xca710c8aeb10752df279055a82a3c0a53c09610b",
]

WEATHER_TITLE = re.compile(r"highest temperature.*°F", re.IGNORECASE)
BUCKET_RE = re.compile(r"between\s*\d+[-– ]\s*\d+\s*°F", re.IGNORECASE)
CEILING_RE = re.compile(r"\d+\s*°F\s+or\s+(below|lower)", re.IGNORECASE)
FLOOR_RE = re.compile(r"\d+\s*°F\s+or\s+(above|higher)", re.IGNORECASE)
DATE_IN_TITLE = re.compile(r"on\s+([A-Z][a-z]+\s+\d+)", re.IGNORECASE)

CITIES = [
    "Los Angeles", "New York City", "New York", "San Francisco",
    "Washington D.C.", "Washington", "Miami", "Chicago", "Dallas",
    "Houston", "Austin", "Philadelphia", "Phoenix", "Seattle",
    "Denver", "Boston", "Atlanta", "Detroit", "Minneapolis",
    "Portland", "San Diego", "Las Vegas", "Orlando", "Tampa",
    "Nashville", "St. Louis", "Kansas City", "Sacramento",
]

PRICE_BUCKETS = [
    (0.00, 0.20, "0.00-0.20"),
    (0.20, 0.40, "0.20-0.40"),
    (0.40, 0.60, "0.40-0.60"),
    (0.60, 0.80, "0.60-0.80"),
    (0.80, 0.95, "0.80-0.95"),
    (0.95, 1.01, "0.95+"),
]


def classify_shape(title: str) -> str:
    if BUCKET_RE.search(title):
        return "bucket"
    if CEILING_RE.search(title):
        return "ceiling"
    if FLOOR_RE.search(title):
        return "floor"
    return "other"


def extract_city(title: str) -> str:
    low = title.lower()
    for c in CITIES:
        if c.lower() in low:
            return c
    return "?"


def price_bucket(price: float) -> str:
    for lo, hi, label in PRICE_BUCKETS:
        if lo <= price < hi:
            return label
    return "?"


def is_weather(a: Activity) -> bool:
    return bool(WEATHER_TITLE.search(a.title or ""))


def parse_ts(a: Activity) -> datetime | None:
    try:
        if a.timestamp:
            return datetime.fromtimestamp(int(float(a.timestamp)), tz=timezone.utc)
    except Exception:
        pass
    return None


@dataclass
class Event:
    side: str
    ts: datetime
    price: float
    size: float

    @property
    def cost(self) -> float:
        return self.price * self.size


@dataclass
class PositionLife:
    condition_id: str
    outcome: str
    title: str
    events: list[Event] = field(default_factory=list)

    @property
    def buys(self) -> list[Event]:
        return [e for e in self.events if e.side == "BUY"]

    @property
    def sells(self) -> list[Event]:
        return [e for e in self.events if e.side == "SELL"]

    @property
    def bought_contracts(self) -> float:
        return sum(e.size for e in self.buys)

    @property
    def sold_contracts(self) -> float:
        return sum(e.size for e in self.sells)

    @property
    def buy_cost(self) -> float:
        return sum(e.cost for e in self.buys)

    @property
    def sell_revenue(self) -> float:
        return sum(e.cost for e in self.sells)

    @property
    def avg_buy_price(self) -> float:
        return self.buy_cost / self.bought_contracts if self.bought_contracts else 0.0

    @property
    def avg_sell_price(self) -> float:
        return self.sell_revenue / self.sold_contracts if self.sold_contracts else 0.0

    @property
    def first_buy_ts(self) -> datetime | None:
        return min((e.ts for e in self.buys), default=None)

    @property
    def last_ts(self) -> datetime | None:
        return max((e.ts for e in self.events), default=None)

    @property
    def last_event(self) -> Event | None:
        if not self.events:
            return None
        return max(self.events, key=lambda e: e.ts)

    @property
    def hold_hours(self) -> float:
        fb = self.first_buy_ts
        lt = self.last_ts
        if fb and lt:
            return (lt - fb).total_seconds() / 3600
        return 0.0

    @property
    def sold_fraction(self) -> float:
        return self.sold_contracts / self.bought_contracts if self.bought_contracts else 0.0

    @property
    def is_full_exit(self) -> bool:
        """Fully sold out and last event is SELL → exited before resolution."""
        if self.bought_contracts == 0:
            return False
        # allow small rounding
        fully_closed = abs(self.bought_contracts - self.sold_contracts) / self.bought_contracts < 0.02
        le = self.last_event
        return fully_closed and le is not None and le.side == "SELL"

    @property
    def had_any_sell(self) -> bool:
        return any(e.side == "SELL" for e in self.events)


def f(val: str | None) -> float:
    try:
        return float(val or 0)
    except Exception:
        return 0.0


@dataclass
class WhaleStrategy:
    addr: str
    positions: list[PositionLife] = field(default_factory=list)

    @property
    def trade_count(self) -> int:
        return sum(len(p.events) for p in self.positions)

    @property
    def buy_count(self) -> int:
        return sum(len(p.buys) for p in self.positions)

    @property
    def sell_count(self) -> int:
        return sum(len(p.sells) for p in self.positions)

    @property
    def buy_usdc(self) -> float:
        return sum(p.buy_cost for p in self.positions)

    @property
    def sell_usdc(self) -> float:
        return sum(p.sell_revenue for p in self.positions)


def render_bar(count: int, maxv: int, width: int = 28) -> str:
    if maxv <= 0:
        return ""
    return "█" * max(1 if count > 0 else 0, int(width * count / maxv))


def pct_str(num: int, denom: int) -> str:
    if denom == 0:
        return "—"
    return f"{100 * num / denom:.0f}%"


async def collect(addr: str, client: DataAPIClient) -> WhaleStrategy:
    activity = await client.get_all_activity(addr, max_pages=50)
    trades = [a for a in activity if a.type == "TRADE" and is_weather(a)]

    by_pos: dict[tuple[str, str], PositionLife] = {}
    for a in trades:
        ts = parse_ts(a)
        if not ts:
            continue
        side = (a.side or "").upper()
        if side not in ("BUY", "SELL"):
            continue
        outcome = (a.outcome or "").upper()
        if outcome not in ("YES", "NO"):
            outcome = "YES" if (a.outcome_index or 0) == 0 else "NO"
        cid = a.condition_id or a.market
        if not cid:
            continue
        key = (cid, outcome)
        pos = by_pos.get(key)
        if pos is None:
            pos = PositionLife(condition_id=cid, outcome=outcome, title=a.title or "")
            by_pos[key] = pos
        pos.events.append(Event(
            side=side, ts=ts, price=f(a.price), size=f(a.size),
        ))

    positions = list(by_pos.values())
    log.info("%s  activity=%d  weather_trades=%d  positions=%d",
             addr[:12], len(activity), len(trades), len(positions))
    return WhaleStrategy(addr=addr, positions=positions)


def print_whale(s: WhaleStrategy) -> None:
    print("\n" + "=" * 80)
    print(f"  {s.addr}")
    print("=" * 80)

    if not s.positions:
        print("  No weather trades.")
        return

    first = min((p.first_buy_ts for p in s.positions if p.first_buy_ts), default=None)
    last = max((p.last_ts for p in s.positions if p.last_ts), default=None)
    print(f"  {s.trade_count} trades ({s.buy_count} BUY / {s.sell_count} SELL)  "
          f"across {len(s.positions)} positions")
    print(f"  Volume: BUY ${s.buy_usdc:,.0f}  SELL ${s.sell_usdc:,.0f}  "
          f"window {first.date() if first else '?'} → {last.date() if last else '?'}")

    # ---- Market preferences ----
    city_hist = Counter(extract_city(p.title) for p in s.positions)
    shape_hist = Counter(classify_shape(p.title) for p in s.positions)

    print("\n  Cities (by position count):")
    for city, c in city_hist.most_common(6):
        print(f"    {city:<22s} {c:>4d}")

    print(f"\n  Shapes: " + "   ".join(f"{k}={v}" for k, v in shape_hist.most_common()))

    # ---- Side preference ----
    buy_yes = sum(1 for p in s.positions for e in p.buys if p.outcome == "YES")
    buy_no = sum(1 for p in s.positions for e in p.buys if p.outcome == "NO")
    yes_pct = pct_str(buy_yes, buy_yes + buy_no)
    no_pct = pct_str(buy_no, buy_yes + buy_no)
    print(f"\n  BUY side mix (trades):  YES {buy_yes} ({yes_pct})   NO {buy_no} ({no_pct})")

    # Side × shape crosstab (positions)
    cross: dict[str, Counter] = defaultdict(Counter)
    for p in s.positions:
        cross[classify_shape(p.title)][p.outcome] += 1
    print("  Side × shape (positions):")
    for shape in sorted(cross.keys(), key=lambda k: -sum(cross[k].values())):
        y = cross[shape].get("YES", 0)
        n = cross[shape].get("NO", 0)
        print(f"    {shape:<10s}  YES {y:>3d}  NO {n:>3d}  ({pct_str(y, y+n)} YES)")

    # ---- Entry price distribution ----
    price_hist = Counter()
    price_dollars: dict[str, float] = defaultdict(float)
    for p in s.positions:
        for e in p.buys:
            label = price_bucket(e.price)
            price_hist[label] += 1
            price_dollars[label] += e.cost
    total_buys = sum(price_hist.values())
    maxv = max(price_hist.values()) if price_hist else 0
    print("\n  Entry-price distribution (BUY trades):")
    for _, _, label in PRICE_BUCKETS:
        c = price_hist.get(label, 0)
        d = price_dollars.get(label, 0.0)
        bar = render_bar(c, maxv)
        print(f"    {label:10s} n={c:>4d} ({pct_str(c, total_buys)})  ${d:>7,.0f}  {bar}")

    # ---- Exit behavior ----
    n_any_sell = sum(1 for p in s.positions if p.had_any_sell)
    n_full_exit = sum(1 for p in s.positions if p.is_full_exit)
    n_partial = sum(1 for p in s.positions if p.had_any_sell and not p.is_full_exit)
    n_hold = len(s.positions) - n_any_sell
    hold_hours = [p.hold_hours for p in s.positions if p.hold_hours > 0]
    hold_exit_hours = [p.hold_hours for p in s.positions if p.is_full_exit and p.hold_hours > 0]

    print("\n  Exit behavior (across ALL positions, resolved or not):")
    print(f"    Never sold (held):         {n_hold:>4d} ({pct_str(n_hold, len(s.positions))})")
    print(f"    Full exit (sold pre-res):  {n_full_exit:>4d} ({pct_str(n_full_exit, len(s.positions))})")
    print(f"    Partial sell:              {n_partial:>4d} ({pct_str(n_partial, len(s.positions))})")
    if hold_hours:
        print(f"    Hold time (first buy → last event):  "
              f"avg {statistics.mean(hold_hours):.1f}h  median {statistics.median(hold_hours):.1f}h")
    if hold_exit_hours:
        print(f"    Full-exit hold time:  avg {statistics.mean(hold_exit_hours):.1f}h  "
              f"median {statistics.median(hold_exit_hours):.1f}h")

    # For full-exits: avg entry vs avg exit price (proxy for "taking profits")
    exit_positions = [p for p in s.positions if p.is_full_exit and p.bought_contracts > 0]
    if exit_positions:
        mean_entry = statistics.mean(p.avg_buy_price for p in exit_positions)
        mean_exit = statistics.mean(p.avg_sell_price for p in exit_positions)
        wins = sum(1 for p in exit_positions if p.avg_sell_price > p.avg_buy_price)
        print(f"    Full-exit entry→exit:  avg entry {mean_entry:.3f} → avg exit {mean_exit:.3f}  "
              f"({pct_str(wins, len(exit_positions))} exited higher)")

    # ---- Conviction / sizing ----
    single_buy = sum(1 for p in s.positions if len(p.buys) == 1)
    multi_buy = len(s.positions) - single_buy
    buys_per_pos = [len(p.buys) for p in s.positions if p.buys]
    pos_sizes = [p.buy_cost for p in s.positions if p.buy_cost > 0]
    avg_buys = statistics.mean(buys_per_pos) if buys_per_pos else 0
    avg_size = statistics.mean(pos_sizes) if pos_sizes else 0
    med_size = statistics.median(pos_sizes) if pos_sizes else 0
    max_size = max(pos_sizes) if pos_sizes else 0
    p90_size = statistics.quantiles(pos_sizes, n=10)[-1] if len(pos_sizes) >= 10 else max_size

    print("\n  Conviction / sizing:")
    print(f"    1-click positions:   {single_buy:>4d} ({pct_str(single_buy, len(s.positions))})")
    print(f"    Averaged-in (2+):    {multi_buy:>4d} ({pct_str(multi_buy, len(s.positions))})")
    print(f"    Avg BUYs/position:   {avg_buys:.1f}")
    print(f"    Position size $:     avg ${avg_size:,.0f}   median ${med_size:,.0f}   "
          f"p90 ${p90_size:,.0f}   max ${max_size:,.0f}")


def print_summary(whales: list[WhaleStrategy]) -> None:
    print("\n" + "#" * 100)
    print("CROSS-WHALE SUMMARY — weather strategy profile")
    print("#" * 100)
    print(f"{'whale':<14} {'trades':>7} {'pos':>4} {'YES%':>5} "
          f"{'avg$':>7} {'max$':>7} {'hold':>5} {'held%':>5} {'exit%':>5}  "
          f"{'top city':<18} {'top shape':<10}")
    print("-" * 100)
    for s in whales:
        if not s.positions:
            continue
        buy_yes = sum(1 for p in s.positions for e in p.buys if p.outcome == "YES")
        buy_no = sum(1 for p in s.positions for e in p.buys if p.outcome == "NO")
        yes_p = 100 * buy_yes / (buy_yes + buy_no) if (buy_yes + buy_no) else 0
        n_hold = sum(1 for p in s.positions if not p.had_any_sell)
        n_full_exit = sum(1 for p in s.positions if p.is_full_exit)
        held_p = 100 * n_hold / len(s.positions)
        exit_p = 100 * n_full_exit / len(s.positions)
        sizes = [p.buy_cost for p in s.positions if p.buy_cost > 0]
        avg_sz = statistics.mean(sizes) if sizes else 0
        max_sz = max(sizes) if sizes else 0
        hold_hours = [p.hold_hours for p in s.positions if p.hold_hours > 0]
        avg_hold = statistics.mean(hold_hours) if hold_hours else 0
        city_hist = Counter(extract_city(p.title) for p in s.positions)
        shape_hist = Counter(classify_shape(p.title) for p in s.positions)
        top_city = city_hist.most_common(1)[0][0] if city_hist else "?"
        top_shape = shape_hist.most_common(1)[0][0] if shape_hist else "?"
        print(f"{s.addr[:12]:<14} {s.trade_count:>7d} {len(s.positions):>4d} "
              f"{yes_p:>4.0f}% ${avg_sz:>5.0f} ${max_sz:>5.0f} "
              f"{avg_hold:>4.1f}h {held_p:>4.0f}% {exit_p:>4.0f}%  "
              f"{top_city[:18]:<18} {top_shape:<10}")


async def main():
    client = DataAPIClient()
    whales: list[WhaleStrategy] = []
    try:
        for addr in WHALES:
            try:
                whales.append(await collect(addr.lower(), client))
            except Exception as e:
                log.error("collect failed for %s: %s", addr[:12], e)
                whales.append(WhaleStrategy(addr=addr))
    finally:
        await client.close()

    for s in whales:
        print_whale(s)
    print_summary(whales)


if __name__ == "__main__":
    asyncio.run(main())
