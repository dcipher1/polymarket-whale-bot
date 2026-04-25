"""Authoritative PnL for watch-whales.

Two sources:
  1) Polymarket's user-pnl-api — wallet-wide windowed PnL, matches the PM UI.
  2) /positions + /closed-positions filtered to weather by title — directional
     weather-only approximation.

The wallet-wide number is authoritative; the weather-approx undercounts because
neg-risk merges, fully sold-out positions, and some redemptions drop out of the
per-position endpoints."""

import asyncio
import logging
import re
from dataclasses import dataclass, field

from src.polymarket.data_api import DataAPIClient, Position

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
)
log = logging.getLogger("wwlp")

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
INTERVALS = ["1d", "1w", "1m", "all"]


def is_weather(p: Position) -> bool:
    return bool(WEATHER_TITLE.search(p.title or ""))


def f(val: str | None) -> float:
    try:
        return float(val or 0)
    except Exception:
        return 0.0


def pos_pnl(p: Position) -> float:
    """Total PnL for a single position = unrealized + realized."""
    return f(p.cash_pnl) + f(p.realized_pnl)


def window_delta(series: list[dict]) -> float:
    if not series:
        return 0.0
    return float(series[-1].get("p", 0)) - float(series[0].get("p", 0))


def window_last(series: list[dict]) -> float:
    if not series:
        return 0.0
    return float(series[-1].get("p", 0))


@dataclass
class WhaleReport:
    addr: str
    closed_all: list[Position] = field(default_factory=list)
    open_all: list[Position] = field(default_factory=list)
    pnl_series: dict[str, list[dict]] = field(default_factory=dict)

    @property
    def closed_weather(self) -> list[Position]:
        return [p for p in self.closed_all if is_weather(p)]

    @property
    def open_weather(self) -> list[Position]:
        return [p for p in self.open_all if is_weather(p)]

    @property
    def wallet_pnl_now(self) -> float:
        s = self.pnl_series.get("all", [])
        return window_last(s)

    @property
    def weather_concentration(self) -> float:
        total = len(self.closed_all) + len(self.open_all)
        weather = len(self.closed_weather) + len(self.open_weather)
        return weather / total if total else 0.0

    @property
    def weather_pnl_approx(self) -> float:
        return (sum(f(p.realized_pnl) for p in self.closed_weather)
                + sum(pos_pnl(p) for p in self.open_weather))

    @property
    def weather_closed_pnl(self) -> float:
        return sum(f(p.realized_pnl) for p in self.closed_weather)

    @property
    def weather_open_mtm(self) -> float:
        return sum(pos_pnl(p) for p in self.open_weather)


async def collect(addr: str, client: DataAPIClient) -> WhaleReport:
    r = WhaleReport(addr=addr)
    r.open_all = await client.get_positions(addr)
    r.closed_all = await client.get_closed_positions(addr)
    for iv in INTERVALS:
        r.pnl_series[iv] = await client.get_user_pnl_series(addr, interval=iv)
    log.info("%s  open=%d (weather %d)  closed=%d (weather %d)",
             addr[:12], len(r.open_all), len(r.open_weather),
             len(r.closed_all), len(r.closed_weather))
    return r


def print_whale(r: WhaleReport) -> None:
    print("\n" + "=" * 80)
    print(f"  {r.addr}")
    print("=" * 80)

    # Wallet-wide (PM-authoritative)
    d1 = window_delta(r.pnl_series.get("1d", []))
    d7 = window_delta(r.pnl_series.get("1w", []))
    d30 = window_delta(r.pnl_series.get("1m", []))
    dall = window_last(r.pnl_series.get("all", []))
    print(f"  WALLET PnL (PM)       1d ${d1:+8.0f}   7d ${d7:+8.0f}   30d ${d30:+8.0f}   all-time ${dall:+9.0f}")

    # Weather-only (approx)
    print(f"  WEATHER PnL (approx)  closed ${r.weather_closed_pnl:+8.0f}   "
          f"open MTM ${r.weather_open_mtm:+8.0f}   "
          f"total ${r.weather_pnl_approx:+8.0f}")

    # Scope context
    print(f"  WEATHER concentration {r.weather_concentration*100:.0f}%  "
          f"(weather {len(r.closed_weather) + len(r.open_weather)} / "
          f"total {len(r.closed_all) + len(r.open_all)} positions)")

    if r.open_weather:
        print("\n  Top 5 open weather positions by cost basis:")
        for p in sorted(r.open_weather, key=lambda x: -f(x.initial_value))[:5]:
            pct = f(p.percent_pnl)
            print(f"    ${f(p.initial_value):>7.0f}  {p.outcome:<3}  "
                  f"avg {f(p.avg_price):.3f} → cur {f(p.cur_price):.3f}  "
                  f"pnl ${pos_pnl(p):+7.2f} ({pct:+6.1f}%)  "
                  f"{(p.title or '')[:55]}")

        pos_winners = sorted(r.open_weather, key=lambda x: -pos_pnl(x))[:3]
        pos_losers = sorted(r.open_weather, key=lambda x: pos_pnl(x))[:3]
        if pos_winners and pos_pnl(pos_winners[0]) > 0:
            print("\n  Biggest open winners:")
            for p in pos_winners:
                if pos_pnl(p) <= 0:
                    break
                print(f"    ${pos_pnl(p):+8.2f}  {p.outcome:<3}  "
                      f"avg {f(p.avg_price):.3f} → cur {f(p.cur_price):.3f}  "
                      f"{(p.title or '')[:55]}")
        if pos_losers and pos_pnl(pos_losers[0]) < 0:
            print("\n  Biggest open losers:")
            for p in pos_losers:
                if pos_pnl(p) >= 0:
                    break
                print(f"    ${pos_pnl(p):+8.2f}  {p.outcome:<3}  "
                      f"avg {f(p.avg_price):.3f} → cur {f(p.cur_price):.3f}  "
                      f"{(p.title or '')[:55]}")


def print_summary(reports: list[WhaleReport]) -> None:
    print("\n" + "#" * 92)
    print("CROSS-WHALE SUMMARY")
    print("  Wallet PnL: Polymarket user-pnl-api (authoritative, matches PM profile UI).")
    print("  Weather PnL: directional approximation (neg-risk merges undercount this number).")
    print("#" * 92)
    print(f"{'whale':<14} {'1d':>8} {'7d':>8} {'30d':>9} {'all':>10}  "
          f"{'weather~':>9} {'wx%':>5}  {'open':>5} {'closed':>7}")
    print("-" * 92)
    for r in reports:
        d1 = window_delta(r.pnl_series.get("1d", []))
        d7 = window_delta(r.pnl_series.get("1w", []))
        d30 = window_delta(r.pnl_series.get("1m", []))
        dall = window_last(r.pnl_series.get("all", []))
        wx = r.weather_pnl_approx
        conc = r.weather_concentration * 100
        print(f"{r.addr[:12]:<14} "
              f"{d1:>+8.0f} {d7:>+8.0f} {d30:>+9.0f} {dall:>+10.0f}  "
              f"{wx:>+9.0f} {conc:>4.0f}%  "
              f"{len(r.open_weather):>5} {len(r.closed_weather):>7}")


async def main():
    client = DataAPIClient()
    reports = []
    try:
        for addr in WHALES:
            try:
                reports.append(await collect(addr.lower(), client))
            except Exception as e:
                log.error("collect failed for %s: %s", addr[:12], e)
                reports.append(WhaleReport(addr=addr))
    finally:
        await client.close()

    for r in reports:
        print_whale(r)
    print_summary(reports)


if __name__ == "__main__":
    asyncio.run(main())
