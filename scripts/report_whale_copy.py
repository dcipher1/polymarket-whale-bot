"""Per-whale copy report with fill state, decision reason, and MTM."""

from __future__ import annotations

import argparse
import asyncio
import io
import math
import re
import sys
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from rich import box
from rich.console import Console
from rich.table import Table
from sqlalchemy import text

from src.config import settings
from src.db import async_session
from src.polymarket.clob_client import CLOBClient
from src.polymarket.data_api import DataAPIClient
from src.signals.weather_resolution import parse_dt, weather_local_resolution_cutoff


_PNL_INTERVAL_BY_DAYS = {1: "1d", 7: "1w", 30: "1m"}


async def _fetch_bot_realized(start_utc: datetime) -> float:
    """Sum bot's realized PnL from my_trades over the window. Authoritative cash PnL
    on positions that closed during the window (resolved + exit_timestamp >= start)."""
    async with async_session() as session:
        result = await session.execute(
            text(
                "SELECT COALESCE(SUM(pnl_usdc), 0)::float AS realized "
                "FROM my_trades "
                "WHERE resolved = TRUE AND exit_timestamp >= :start"
            ),
            {"start": start_utc},
        )
        row = result.first()
        return float(row.realized) if row else 0.0


async def _fetch_pnl_deltas(addrs: list[str], days: int) -> dict[str, float]:
    """Fetch windowed PnL delta from user-pnl-api for each address.

    The user-pnl-api is the same source the Polymarket UI uses and is the
    authoritative truth for windowed PnL — it includes realized + unrealized,
    unlike the per-position MTM which only marks open positions. See
    project memory `reference_pm_pnl_api.md`.
    """
    interval = _PNL_INTERVAL_BY_DAYS.get(days, "all")
    fidelity = "1h" if days <= 1 else ("12h" if days <= 7 else "1d")
    out: dict[str, float] = {}
    client = DataAPIClient()
    try:
        for addr in addrs:
            try:
                series = await client.get_user_pnl_series(addr, interval=interval, fidelity=fidelity)
            except Exception:
                series = []
            if series and len(series) >= 2:
                out[addr.lower()] = float(series[-1]["p"]) - float(series[0]["p"])
            else:
                out[addr.lower()] = 0.0
    finally:
        await client.close()
    return out


class FreshPriceError(RuntimeError):
    """Raised when the MTM report cannot get required fresh live prices."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code


def _money(value) -> str:
    if value is None:
        return "n/a"
    value = float(value)
    if math.isnan(value) or math.isinf(value):
        return "n/a"
    return f"${value:,.2f}"


def _num(value) -> str:
    if value is None:
        return "0"
    value = float(value)
    if abs(value - round(value)) < 0.005:
        return f"{value:,.0f}"
    return f"{value:,.2f}"


def _short_market(question: str, condition_id: str) -> str:
    text_value = (question or condition_id[:10]).replace("|", "/").replace("\n", " ")
    replacements = (
        ("Will the highest temperature in ", ""),
        (" be between ", " "),
        (" be ", " "),
        (" on April ", " Apr"),
    )
    for old, new in replacements:
        text_value = text_value.replace(old, new)
    return text_value[:64]


_WEATHER_QUESTION_RE = re.compile(
    r"^Will the highest temperature in (?P<city>.+?) be "
    r"(?:(?:between )?(?P<temp>.+?)) on (?P<month>[A-Za-z]+) (?P<day>\d{1,2})\??$"
)


def _position_parts(question: str, condition_id: str) -> tuple[str, str, str]:
    """Return display columns (city, temp, date) for a weather market title."""
    clean = (question or "").replace("|", "/").replace("\n", " ").strip()
    clean = re.sub(r"\s+", " ", clean)
    match = _WEATHER_QUESTION_RE.match(clean)
    if not match:
        return _short_market(question, condition_id), "", ""
    city = match.group("city").strip()
    temp = match.group("temp").strip()
    date = f"{match.group('month')[:3]}{int(match.group('day'))}"
    return city, temp, date


def _copy_state(row) -> str:
    attempts = int(row.order_attempts or 0)
    filled = float(row.filled_contracts or 0)
    whale = float(row.whale_contracts or 0)
    if attempts <= 0:
        return "NO ORDER"
    if filled <= 0:
        return "NO FILL"
    ratio = filled / whale if whale > 0 else 0
    if ratio < 0.95:
        return f"PARTIAL {ratio:.0%}"
    if ratio <= 1.05:
        return "FILLED"
    return f"OVERCOPIED {ratio:.0%}"


def _position_copy_status(whale_contracts, filled_contracts, order_attempts) -> str:
    whale = float(whale_contracts or 0)
    filled = float(filled_contracts or 0)
    attempts = int(order_attempts or 0)
    if attempts <= 0:
        return "NO ORDER"
    if filled <= 0:
        return "NO FILL"
    ratio = filled / whale if whale > 0 else 0
    if ratio < 0.95:
        return "PARTIAL"
    if ratio <= 1.05:
        return "FULL"
    return "OVERCOPIED"


def _copy_pct(whale_contracts, filled_contracts) -> str:
    whale = float(whale_contracts or 0)
    if whale <= 0:
        return "0%"
    return f"{float(filled_contracts or 0) / whale:.0%}"


def _fallback_reason(row) -> str:
    if row.latest_decision_code and row.latest_decision_code != "SELL_IGNORED":
        return row.latest_decision_code
    if int(row.order_attempts or 0) > 0 and float(row.filled_contracts or 0) <= 0:
        return "orders_cancelled/no_fill"
    if int(row.order_attempts or 0) > 0:
        return "filled/part-filled"
    if not row.question:
        return "missing_market"
    if row.category != "weather":
        return f"non-weather:{row.category or 'unknown'}"

    first_whale_ts = parse_dt(getattr(row, "first_whale_ts", None))
    last_whale_ts = parse_dt(getattr(row, "last_whale_ts", None)) or first_whale_ts
    cutoff = weather_local_resolution_cutoff(
        title=row.question,
        category=row.category,
        resolution_time=getattr(row, "resolution_time", None),
        now_utc=first_whale_ts,
    )
    if cutoff is not None and first_whale_ts is not None:
        if first_whale_ts >= cutoff:
            return "PAST_CITY_CUTOFF"
        if last_whale_ts is not None and last_whale_ts >= cutoff:
            return "MIXED_CITY_CUTOFF"
    return "NO_DECISION_LOGGED"


async def _fetch_position_breakdown_rows(start_utc: datetime, end_utc: datetime, tz_name: str):
    sql = text(
        """
        WITH wb AS (
            SELECT wt.wallet_address,
                   wt.condition_id,
                   wt.outcome,
                   (wt.timestamp AT TIME ZONE :tz)::date AS local_day,
                   min(wt.token_id) AS token_id,
                   sum(wt.num_contracts)::float AS whale_contracts,
                   sum(wt.size_usdc)::float AS whale_cost,
                   count(*) AS whale_buy_fills,
                   min(wt.timestamp) AS first_whale_ts,
                   max(wt.timestamp) AS last_whale_ts
            FROM whale_trades wt
            WHERE wt.side = 'BUY'
              AND wt.timestamp >= :start_utc
              AND wt.timestamp < :end_utc
              AND wt.wallet_address = ANY(:wallets)
            GROUP BY 1, 2, 3, 4
        ),
        ot AS (
            SELECT lower(sw.wallet_address) AS wallet_address,
                   t.condition_id,
                   t.outcome,
                   (t.entry_timestamp AT TIME ZONE :tz)::date AS local_day,
                   count(*) AS order_attempts,
                   count(*) FILTER (WHERE t.fill_status = 'CANCELLED') AS cancelled_orders,
                   coalesce(sum(t.num_contracts) FILTER (WHERE t.fill_status IN ('FILLED','PARTIAL')), 0)::float AS filled_contracts,
                   coalesce(sum(t.size_usdc) FILTER (WHERE t.fill_status IN ('FILLED','PARTIAL')), 0)::float AS filled_cost,
                   string_agg(DISTINCT t.fill_status, ',') AS statuses,
                   min(t.entry_timestamp) AS first_order_ts,
                   max(t.entry_timestamp) AS last_order_ts
            FROM my_trades t
            CROSS JOIN LATERAL unnest(t.source_wallets) AS sw(wallet_address)
            WHERE t.entry_timestamp >= :start_utc
              AND t.entry_timestamp < :end_utc
              AND t.source_wallets IS NOT NULL
            GROUP BY 1, 2, 3, 4
        ),
        latest_trade_decision AS (
            SELECT DISTINCT ON (
                       wt.wallet_address,
                       wt.condition_id,
                       wt.outcome,
                       (wt.timestamp AT TIME ZONE :tz)::date
                   )
                   wt.wallet_address,
                   wt.condition_id,
                   wt.outcome,
                   (wt.timestamp AT TIME ZONE :tz)::date AS local_day,
                   cd.decision_code,
                   cd.decision_reason,
                   cd.decision_source,
                   cd.latency_seconds,
                   cd.decided_at
            FROM whale_trades wt
            JOIN copy_decisions cd ON cd.whale_trade_id = wt.id
            WHERE wt.side = 'BUY'
              AND wt.timestamp >= :start_utc
              AND wt.timestamp < :end_utc
              AND wt.wallet_address = ANY(:wallets)
              AND cd.decision_code <> 'SELL_IGNORED'
            ORDER BY wt.wallet_address, wt.condition_id, wt.outcome, (wt.timestamp AT TIME ZONE :tz)::date, cd.decided_at DESC
        ),
        latest_day_decision AS (
            SELECT DISTINCT ON (
                       wallet_address,
                       condition_id,
                       outcome,
                       (decided_at AT TIME ZONE :tz)::date
                   )
                   wallet_address,
                   condition_id,
                   outcome,
                   (decided_at AT TIME ZONE :tz)::date AS local_day,
                   decision_code,
                   decision_reason,
                   decision_source,
                   latency_seconds,
                   decided_at
            FROM copy_decisions
            WHERE decided_at >= :start_utc
              AND decided_at < :end_utc
              AND whale_trade_id IS NULL
              AND decision_code <> 'SELL_IGNORED'
            ORDER BY wallet_address, condition_id, outcome, (decided_at AT TIME ZONE :tz)::date, decided_at DESC
        )
        SELECT wb.*,
               coalesce(m.question, '') AS question,
               coalesce(m.category_override, m.category, '') AS category,
               m.resolution_time,
               coalesce(ot.order_attempts, 0) AS order_attempts,
               coalesce(ot.cancelled_orders, 0) AS cancelled_orders,
               coalesce(ot.filled_contracts, 0)::float AS filled_contracts,
               coalesce(ot.filled_cost, 0)::float AS filled_cost,
               coalesce(ot.statuses, '') AS statuses,
               ot.first_order_ts,
               ot.last_order_ts,
               coalesce(ltd.decision_code, ldd.decision_code) AS latest_decision_code,
               coalesce(ltd.decision_reason, ldd.decision_reason) AS latest_decision_reason,
               coalesce(ltd.decision_source, ldd.decision_source) AS latest_decision_source,
               coalesce(ltd.latency_seconds, ldd.latency_seconds) AS latest_latency_seconds,
               coalesce(ltd.decided_at, ldd.decided_at) AS latest_decided_at
        FROM wb
        LEFT JOIN ot
          ON ot.wallet_address = wb.wallet_address
         AND ot.condition_id = wb.condition_id
         AND ot.outcome = wb.outcome
         AND ot.local_day = wb.local_day
        LEFT JOIN markets m ON m.condition_id = wb.condition_id
        LEFT JOIN latest_trade_decision ltd
          ON ltd.wallet_address = wb.wallet_address
         AND ltd.condition_id = wb.condition_id
         AND ltd.outcome = wb.outcome
         AND ltd.local_day = wb.local_day
        LEFT JOIN latest_day_decision ldd
          ON ldd.wallet_address = wb.wallet_address
         AND ldd.condition_id = wb.condition_id
         AND ldd.outcome = wb.outcome
         AND ldd.local_day = wb.local_day
        ORDER BY wb.local_day, array_position(:wallets, wb.wallet_address), wb.whale_cost DESC
        """
    )
    async with async_session() as session:
        result = await session.execute(
            sql,
            {
                "start_utc": start_utc,
                "end_utc": end_utc,
                "wallets": [w.lower() for w in settings.watch_whales],
                "tz": tz_name,
            },
        )
        return result.fetchall()


async def _fetch_rows(start_utc: datetime, end_utc: datetime, tz_name: str):
    sql = text(
        """
        WITH wb AS (
            SELECT wt.wallet_address,
                   wt.condition_id,
                   wt.outcome,
                   (wt.timestamp AT TIME ZONE :tz)::date AS local_day,
                   min(wt.token_id) AS token_id,
                   sum(wt.num_contracts)::float AS whale_contracts,
                   sum(wt.size_usdc)::float AS whale_cost,
                   count(*) AS whale_events,
                   min(wt.timestamp) AS first_whale_ts,
                   max(wt.timestamp) AS last_whale_ts,
                   min(wt.detected_at) AS first_detected_at,
                   min(wt.id) AS first_whale_trade_id
            FROM whale_trades wt
            WHERE wt.side = 'BUY'
              AND wt.timestamp >= :start_utc
              AND wt.timestamp < :end_utc
              AND wt.wallet_address = ANY(:wallets)
            GROUP BY 1, 2, 3, 4
        ),
        ot AS (
            SELECT lower(unnest(source_wallets)) AS wallet_address,
                   condition_id,
                   outcome,
                   count(*) AS order_attempts,
                   count(*) FILTER (WHERE fill_status = 'CANCELLED') AS cancelled_orders,
                   (sum(num_contracts) FILTER (WHERE fill_status IN ('FILLED','PARTIAL')))::float AS filled_contracts,
                   (sum(size_usdc) FILTER (WHERE fill_status IN ('FILLED','PARTIAL')))::float AS filled_cost,
                   string_agg(DISTINCT fill_status, ',') AS statuses,
                   min(entry_timestamp) AS first_order_ts
            FROM my_trades
            WHERE entry_timestamp >= :start_utc
              AND entry_timestamp < :end_utc
              AND source_wallets IS NOT NULL
            GROUP BY 1, 2, 3
        ),
        latest_trade_decision AS (
            SELECT DISTINCT ON (
                       wt.wallet_address,
                       wt.condition_id,
                       wt.outcome,
                       (wt.timestamp AT TIME ZONE :tz)::date
                   )
                   wt.wallet_address,
                   wt.condition_id,
                   wt.outcome,
                   (wt.timestamp AT TIME ZONE :tz)::date AS local_day,
                   cd.decision_code,
                   cd.decision_reason,
                   cd.decision_source,
                   cd.latency_seconds,
                   cd.decided_at
            FROM whale_trades wt
            JOIN copy_decisions cd ON cd.whale_trade_id = wt.id
            WHERE wt.side = 'BUY'
              AND wt.timestamp >= :start_utc
              AND wt.timestamp < :end_utc
              AND wt.wallet_address = ANY(:wallets)
              AND cd.decision_code <> 'SELL_IGNORED'
            ORDER BY wt.wallet_address, wt.condition_id, wt.outcome, (wt.timestamp AT TIME ZONE :tz)::date, cd.decided_at DESC
        ),
        latest_day_decision AS (
            SELECT DISTINCT ON (
                       wallet_address,
                       condition_id,
                       outcome,
                       (decided_at AT TIME ZONE :tz)::date
                   )
                   wallet_address,
                   condition_id,
                   outcome,
                   (decided_at AT TIME ZONE :tz)::date AS local_day,
                   decision_code,
                   decision_reason,
                   decision_source,
                   latency_seconds,
                   decided_at
            FROM copy_decisions
            WHERE decided_at >= :start_utc
              AND decided_at < :end_utc
              AND whale_trade_id IS NULL
              AND decision_code <> 'SELL_IGNORED'
            ORDER BY wallet_address, condition_id, outcome, (decided_at AT TIME ZONE :tz)::date, decided_at DESC
        )
        SELECT wb.*,
               coalesce(m.question, '') AS question,
               coalesce(m.category_override, m.category, '') AS category,
               m.resolved,
               m.outcome AS resolved_outcome,
               m.resolution_time,
               coalesce(ot.order_attempts, 0) AS order_attempts,
               coalesce(ot.cancelled_orders, 0) AS cancelled_orders,
               coalesce(ot.filled_contracts, 0)::float AS filled_contracts,
               coalesce(ot.filled_cost, 0)::float AS filled_cost,
               coalesce(ot.statuses, '') AS statuses,
               ot.first_order_ts,
               coalesce(ltd.decision_code, ldd.decision_code) AS latest_decision_code,
               coalesce(ltd.decision_reason, ldd.decision_reason) AS latest_decision_reason,
               coalesce(ltd.decision_source, ldd.decision_source) AS latest_decision_source,
               coalesce(ltd.latency_seconds, ldd.latency_seconds) AS latest_latency_seconds
        FROM wb
        LEFT JOIN ot
          ON ot.wallet_address = wb.wallet_address
         AND ot.condition_id = wb.condition_id
         AND ot.outcome = wb.outcome
        LEFT JOIN markets m ON m.condition_id = wb.condition_id
        LEFT JOIN latest_trade_decision ltd
          ON ltd.wallet_address = wb.wallet_address
         AND ltd.condition_id = wb.condition_id
         AND ltd.outcome = wb.outcome
         AND ltd.local_day = wb.local_day
        LEFT JOIN latest_day_decision ldd
          ON ldd.wallet_address = wb.wallet_address
         AND ldd.condition_id = wb.condition_id
         AND ldd.outcome = wb.outcome
         AND ldd.local_day = wb.local_day
        ORDER BY wb.local_day, wb.wallet_address, wb.whale_cost DESC
        """
    )
    async with async_session() as session:
        result = await session.execute(
            sql,
            {
                "start_utc": start_utc,
                "end_utc": end_utc,
                "wallets": [w.lower() for w in settings.watch_whales],
                "tz": tz_name,
            },
        )
        return result.fetchall()


def _needs_live_price(row) -> bool:
    return bool(row.token_id) and not (getattr(row, "resolved", False) and getattr(row, "resolved_outcome", None))


def _missing_price_message(rows, missing_tokens: set[str]) -> str:
    labels = []
    seen = set()
    for row in rows:
        if row.token_id not in missing_tokens or row.token_id in seen:
            continue
        seen.add(row.token_id)
        labels.append(f"{_short_market(row.question, row.condition_id)} {row.outcome} ({row.token_id[:16]})")
        if len(labels) >= 10:
            break
    more = max(len(missing_tokens) - len(labels), 0)
    suffix = f"; +{more} more" if more else ""
    return f"missing fresh CLOB midpoints for {len(missing_tokens)} token(s): " + "; ".join(labels) + suffix


async def _midpoints(
    rows,
    timeout_seconds: float,
    *,
    allow_missing: bool = False,
) -> tuple[dict[str, float | None], datetime | None]:
    clob = CLOBClient()
    try:
        tokens = sorted({row.token_id for row in rows if _needs_live_price(row)})
        if not tokens:
            return {}, None
        fetched_at = datetime.now(ZoneInfo("UTC"))
        try:
            mids = await asyncio.wait_for(clob.get_midpoints(tokens), timeout=timeout_seconds)
        except asyncio.TimeoutError as exc:
            raise FreshPriceError(
                "clob_timeout",
                f"timed out after {timeout_seconds:.1f}s fetching {len(tokens)} live midpoint(s)",
            ) from exc
        except Exception as exc:
            raise FreshPriceError(
                "network_unavailable",
                f"could not fetch {len(tokens)} live midpoint(s): {exc}",
            ) from exc
        missing = {token for token in tokens if mids.get(token) is None}
        if missing and not allow_missing:
            raise FreshPriceError("missing_midpoints", _missing_price_message(rows, missing))
        return mids, fetched_at
    finally:
        await clob.close()


def _local_window(days: int, tz_name: str) -> tuple[datetime, datetime]:
    tz = ZoneInfo(tz_name)
    today = datetime.now(tz).date()
    start_local = datetime.combine(today - timedelta(days=days - 1), datetime.min.time(), tzinfo=tz)
    end_local = datetime.combine(today + timedelta(days=1), datetime.min.time(), tzinfo=tz)
    return start_local.astimezone(ZoneInfo("UTC")), end_local.astimezone(ZoneInfo("UTC"))


def _render_markdown(
    rows,
    mids,
    tz_name: str,
    *,
    price_fetched_at: datetime | None = None,
    price_source: str = "clob_midpoints",
    pnl_deltas: dict[str, float] | None = None,
    bot_pnl_delta: float | None = None,
    bot_realized: float | None = None,
    days: int = 1,
) -> str:
    names = {wallet.lower(): wallet[:10] for wallet in settings.watch_whales}
    lines = [
        "# Whale Copy Report",
        f"Timezone: {tz_name}",
        f"Window: last {days}d",
        "",
        "**Headline PnL summary** = REALIZED + UNREALIZED (windowed delta from user-pnl-api;",
        "                          matches Polymarket UI; includes redemptions, sells, open MTM).",
        "**Per-position MTM**     = UNREALIZED only (current_mid × open_contracts − cost_basis;",
        "                          excludes realized gains from closed/sold/redeemed positions —",
        "                          those are already counted in the headline).",
        f"price_source={price_source}",
    ]
    if price_fetched_at is not None:
        lines.append(f"price_fetched_at={price_fetched_at.isoformat()}")
        lines.append("price_age_seconds=0")

    enriched = []
    for row in rows:
        mid = mids.get(row.token_id)
        if getattr(row, "resolved", False) and getattr(row, "resolved_outcome", None):
            mid = 1.0 if row.resolved_outcome == row.outcome else 0.0
        whale_contracts = float(row.whale_contracts or 0)
        whale_cost = float(row.whale_cost or 0)
        filled_contracts = float(row.filled_contracts or 0)
        filled_cost = float(row.filled_cost or 0)
        whale_mtm = None if mid is None else whale_contracts * mid - whale_cost
        our_mtm = 0.0 if filled_contracts <= 0 else (None if mid is None else filled_contracts * mid - filled_cost)
        enriched.append((row, mid, whale_mtm, our_mtm))

    summary = {}
    for row in rows:
        key = str(row.local_day)
        summary.setdefault(key, {"positions": 0, "ordered": 0, "filled": 0})
        summary[key]["positions"] += 1
        summary[key]["ordered"] += int((row.order_attempts or 0) > 0)
        summary[key]["filled"] += int((row.filled_contracts or 0) > 0)

    pnl_deltas = pnl_deltas or {}
    whales_total = sum(pnl_deltas.get(w.lower(), 0.0) for w in settings.watch_whales)
    bot_delta = bot_pnl_delta or 0.0
    bot_realized_v = bot_realized or 0.0
    bot_open_mtm = sum((our_mtm or 0.0) for _, _, _, our_mtm in enriched if our_mtm is not None)
    bot_split_total = bot_realized_v + bot_open_mtm
    capture_pct = (bot_delta / whales_total * 100) if whales_total else 0.0

    lines.append(f"\n## Headline PnL (last {days}d) — REALIZED + UNREALIZED")
    lines.append("| Wallet | PnL |")
    lines.append("|---|---:|")
    for wallet in settings.watch_whales:
        lines.append(f"| {wallet[:10]} | {_money(pnl_deltas.get(wallet.lower(), 0.0))} |")
    lines.append(f"| **Whales total** | **{_money(whales_total)}** |")
    lines.append(f"| Bot (user-pnl-api) | {_money(bot_delta)} |")
    lines.append(f"| **Capture %** | **{capture_pct:.0f}%** |")

    lines.append(f"\n## Bot PnL split (last {days}d, source=my_trades + open MTM)")
    lines.append("| Component | $ |")
    lines.append("|---|---:|")
    lines.append(f"| Realized (closed positions in window) | {_money(bot_realized_v)} |")
    lines.append(f"| Open MTM (in-window positions still open) | {_money(bot_open_mtm)} |")
    lines.append(f"| **Total (split)** | **{_money(bot_split_total)}** |")
    lines.append(f"| Cross-check vs user-pnl-api | {_money(bot_delta)} (Δ {_money(bot_split_total - bot_delta)}) |")
    lines.append("> Δ explained by: fees, in-flight orders, and unrealized changes on positions opened *before* the window.")

    lines.append("\n## Copy quality (per-day)")
    lines.append("| Day | Positions | Ordered | Filled |")
    lines.append("|---|---:|---:|---:|")
    for day, data in sorted(summary.items()):
        lines.append(
            f"| {day} | {data['positions']} | {data['ordered']} | {data['filled']} |"
        )

    for day in sorted({str(row.local_day) for row in rows}):
        lines.append(f"\n## {day}")
        for wallet, name in names.items():
            wallet_rows = [item for item in enriched if str(item[0].local_day) == day and item[0].wallet_address == wallet]
            if not wallet_rows:
                continue
            lines.append(f"\n### Whale {name}")
            lines.append("| Market | Side | Whale shares/cost | Mid | Whale MTM | Our status | Our fill/cost | Our MTM | Decision | Source/latency |")
            lines.append("|---|---:|---:|---:|---:|---|---:|---:|---|---|")
            for row, mid, whale_mtm, our_mtm in wallet_rows:
                source = row.latest_decision_source or "no_decision"
                if row.latest_latency_seconds is not None:
                    source = f"{source}/{float(row.latest_latency_seconds):.1f}s"
                market_name = _short_market(row.question, row.condition_id)
                if getattr(row, "resolved", False) and getattr(row, "resolved_outcome", None):
                    market_name = f"{market_name} [RES {row.resolved_outcome}]"
                lines.append(
                    "| "
                    + " | ".join(
                        [
                            market_name,
                            row.outcome,
                            f"{_num(row.whale_contracts)} / {_money(row.whale_cost)}",
                            "n/a" if mid is None else f"{mid:.3f}",
                            _money(whale_mtm),
                            _copy_state(row),
                            f"{_num(row.filled_contracts)} / {_money(row.filled_cost)}",
                            _money(our_mtm),
                            _fallback_reason(row),
                            source,
                        ]
                    )
                    + " |"
                )
    return "\n".join(lines)


def _render_text(
    rows,
    mids,
    tz_name: str,
    *,
    price_fetched_at: datetime | None = None,
    price_source: str = "clob_midpoints",
    pnl_deltas: dict[str, float] | None = None,
    bot_pnl_delta: float | None = None,
    bot_realized: float | None = None,
    days: int = 1,
) -> str:
    """Plain-text rendering of the whale copy report (mirrors _render_markdown)."""
    pnl_deltas = pnl_deltas or {}
    whales_total = sum(pnl_deltas.get(w.lower(), 0.0) for w in settings.watch_whales)
    bot_delta = bot_pnl_delta or 0.0
    bot_realized_v = bot_realized or 0.0
    # Sum bot's open MTM across in-window rows.
    bot_open_mtm = 0.0
    for row in rows:
        mid = mids.get(row.token_id)
        if getattr(row, "resolved", False) and getattr(row, "resolved_outcome", None):
            mid = 1.0 if row.resolved_outcome == row.outcome else 0.0
        filled_contracts = float(row.filled_contracts or 0)
        filled_cost = float(row.filled_cost or 0)
        if mid is None or filled_contracts <= 0:
            continue
        bot_open_mtm += filled_contracts * mid - filled_cost
    bot_split_total = bot_realized_v + bot_open_mtm
    capture_pct = (bot_delta / whales_total * 100) if whales_total else 0.0

    lines = [
        "Whale Copy Report",
        f"Timezone: {tz_name}",
        f"Window: last {days}d",
        "",
        "Headline PnL summary = REALIZED + UNREALIZED (user-pnl-api windowed delta;",
        "                      matches Polymarket UI; includes redemptions/sells/open MTM).",
        "Per-position MTM     = UNREALIZED only (current_mid × open_contracts − cost_basis;",
        "                      excludes realized gains from closed positions, which are",
        "                      already counted in the headline above).",
        f"price_source={price_source}",
    ]
    if price_fetched_at is not None:
        lines.append(f"price_fetched_at={price_fetched_at.isoformat()}")

    lines.append(f"\nHeadline PnL (last {days}d) — REALIZED + UNREALIZED")
    lines.append("-" * 40)
    for wallet in settings.watch_whales:
        lines.append(f"  {wallet[:10]:<22s} {_money(pnl_deltas.get(wallet.lower(), 0.0)):>12s}")
    lines.append("-" * 40)
    lines.append(f"  {'Whales total':<22s} {_money(whales_total):>12s}")
    lines.append(f"  {'Bot (user-pnl-api)':<22s} {_money(bot_delta):>12s}")
    lines.append(f"  {'Capture %':<22s} {capture_pct:>11.0f}%")

    lines.append(f"\nBot PnL split (last {days}d, source=my_trades + open MTM)")
    lines.append("-" * 40)
    lines.append(f"  {'Realized (closed)':<22s} {_money(bot_realized_v):>12s}")
    lines.append(f"  {'Open MTM (in-window)':<22s} {_money(bot_open_mtm):>12s}")
    lines.append("-" * 40)
    lines.append(f"  {'Total (split)':<22s} {_money(bot_split_total):>12s}")
    lines.append(f"  {'vs user-pnl-api':<22s} {_money(bot_delta):>12s}  (Δ {_money(bot_split_total - bot_delta)})")
    lines.append("  Δ = fees + in-flight orders + MTM changes on positions opened before window")

    summary = {}
    for row in rows:
        key = str(row.local_day)
        summary.setdefault(key, {"positions": 0, "ordered": 0, "filled": 0})
        summary[key]["positions"] += 1
        summary[key]["ordered"] += int((row.order_attempts or 0) > 0)
        summary[key]["filled"] += int((row.filled_contracts or 0) > 0)

    lines.append("\nCopy quality (per-day)")
    lines.append(f"  {'Day':<12s} {'Positions':>10s} {'Ordered':>10s} {'Filled':>10s}")
    for day, data in sorted(summary.items()):
        lines.append(f"  {day:<12s} {data['positions']:>10d} {data['ordered']:>10d} {data['filled']:>10d}")

    enriched = []
    for row in rows:
        mid = mids.get(row.token_id)
        if getattr(row, "resolved", False) and getattr(row, "resolved_outcome", None):
            mid = 1.0 if row.resolved_outcome == row.outcome else 0.0
        whale_contracts = float(row.whale_contracts or 0)
        whale_cost = float(row.whale_cost or 0)
        filled_contracts = float(row.filled_contracts or 0)
        filled_cost = float(row.filled_cost or 0)
        whale_mtm = None if mid is None else whale_contracts * mid - whale_cost
        our_mtm = 0.0 if filled_contracts <= 0 else (None if mid is None else filled_contracts * mid - filled_cost)
        enriched.append((row, mid, whale_mtm, our_mtm))

    names = {wallet.lower(): wallet[:10] for wallet in settings.watch_whales}
    for day in sorted({str(row.local_day) for row in rows}):
        lines.append(f"\nDay {day}")
        for wallet, name in names.items():
            wallet_rows = [item for item in enriched if str(item[0].local_day) == day and item[0].wallet_address == wallet]
            if not wallet_rows:
                continue
            lines.append(f"\n  Whale {name}")
            lines.append(f"    {'Market':<42s} {'Side':<5s} {'Whale':>14s} {'Mid':>6s} {'WMTM':>9s} {'Our':>14s} {'OurMTM':>9s}  Decision")
            for row, mid, whale_mtm, our_mtm in wallet_rows:
                market_name = _short_market(row.question, row.condition_id)
                if getattr(row, "resolved", False) and getattr(row, "resolved_outcome", None):
                    market_name = f"{market_name} [RES {row.resolved_outcome}]"
                lines.append(
                    f"    {market_name[:42]:<42s} "
                    f"{row.outcome:<5s} "
                    f"{(_num(row.whale_contracts) + '/' + _money(row.whale_cost)):>14s} "
                    f"{('n/a' if mid is None else f'{mid:.3f}'):>6s} "
                    f"{_money(whale_mtm):>9s} "
                    f"{(_num(row.filled_contracts) + '/' + _money(row.filled_cost)):>14s} "
                    f"{_money(our_mtm):>9s}  {_fallback_reason(row)}"
                )
    return "\n".join(lines)


def _render_position_breakdown_markdown(rows, tz_name: str) -> str:
    names = {wallet.lower(): wallet[:10] for wallet in settings.watch_whales}
    wallet_order = {wallet.lower(): idx for idx, wallet in enumerate(settings.watch_whales)}
    lines = [
        "# Whale Aggregate Buy Copy Breakdown",
        f"Timezone: {tz_name}",
        "One row is one aggregate whale BUY position: wallet + market + side + local day.",
    ]

    summary: dict[tuple[str, str], dict[str, float | int]] = {}
    for row in rows:
        day = str(row.local_day)
        wallet = row.wallet_address
        key = (day, wallet)
        status = _position_copy_status(row.whale_contracts, row.filled_contracts, row.order_attempts)
        unfilled_gap = max(float(row.whale_contracts or 0) - float(row.filled_contracts or 0), 0.0)
        data = summary.setdefault(
            key,
            {
                "positions": 0,
                "whale_contracts": 0.0,
                "filled_contracts": 0.0,
                "unfilled_gap": 0.0,
                "FULL": 0,
                "PARTIAL": 0,
                "NO FILL": 0,
                "NO ORDER": 0,
                "OVERCOPIED": 0,
            },
        )
        data["positions"] += 1
        data["whale_contracts"] += float(row.whale_contracts or 0)
        data["filled_contracts"] += float(row.filled_contracts or 0)
        data["unfilled_gap"] += unfilled_gap
        data[status] += 1

    lines.append("\n## Summary")
    lines.append("| Day | Whale | Positions | Whale contracts | Bot filled | Unfilled gap | Full | Partial | No fill | No order | Overcopied |")
    lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for (day, wallet), data in sorted(summary.items(), key=lambda item: (item[0][0], wallet_order.get(item[0][1], 999))):
        lines.append(
            "| "
            + " | ".join(
                [
                    day,
                    names.get(wallet, wallet[:10]),
                    _num(data["positions"]),
                    _num(data["whale_contracts"]),
                    _num(data["filled_contracts"]),
                    _num(data["unfilled_gap"]),
                    _num(data["FULL"]),
                    _num(data["PARTIAL"]),
                    _num(data["NO FILL"]),
                    _num(data["NO ORDER"]),
                    _num(data["OVERCOPIED"]),
                ]
            )
            + " |"
        )

    lines.append("\n## Positions")
    lines.append(
        "| Day | Whale | City | Temp | Date | Side | Whale fills | Whale contracts | Whale cost | "
        "Bot attempts | Bot filled | Gap | Copy % | Status | Reason | Source |"
    )
    lines.append("|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|")
    for row in sorted(rows, key=lambda r: (str(r.local_day), wallet_order.get(r.wallet_address, 999), r.question, r.outcome)):
        whale_contracts = float(row.whale_contracts or 0)
        filled_contracts = float(row.filled_contracts or 0)
        unfilled_gap = max(whale_contracts - filled_contracts, 0.0)
        status = _position_copy_status(whale_contracts, filled_contracts, row.order_attempts)
        city, temp, market_date = _position_parts(row.question, row.condition_id)
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.local_day),
                    names.get(row.wallet_address, row.wallet_address[:10]),
                    city,
                    temp,
                    market_date,
                    row.outcome,
                    _num(row.whale_buy_fills),
                    _num(whale_contracts),
                    _money(row.whale_cost),
                    _num(row.order_attempts),
                    _num(filled_contracts),
                    _num(unfilled_gap),
                    _copy_pct(whale_contracts, filled_contracts),
                    status,
                    _fallback_reason(row),
                    _source_label(row),
                ]
            )
            + " |"
        )
    return "\n".join(lines)


def _source_label(row) -> str:
    source = row.latest_decision_source or "no_decision"
    if row.latest_latency_seconds is not None:
        source = f"{source}/{float(row.latest_latency_seconds):.1f}s"
    return source


def _position_summary(rows):
    summary: dict[tuple[str, str], dict[str, float | int]] = {}
    for row in rows:
        day = str(row.local_day)
        wallet = row.wallet_address
        key = (day, wallet)
        status = _position_copy_status(row.whale_contracts, row.filled_contracts, row.order_attempts)
        unfilled_gap = max(float(row.whale_contracts or 0) - float(row.filled_contracts or 0), 0.0)
        data = summary.setdefault(
            key,
            {
                "positions": 0,
                "whale_contracts": 0.0,
                "filled_contracts": 0.0,
                "unfilled_gap": 0.0,
                "FULL": 0,
                "PARTIAL": 0,
                "NO FILL": 0,
                "NO ORDER": 0,
                "OVERCOPIED": 0,
            },
        )
        data["positions"] += 1
        data["whale_contracts"] += float(row.whale_contracts or 0)
        data["filled_contracts"] += float(row.filled_contracts or 0)
        data["unfilled_gap"] += unfilled_gap
        data[status] += 1
    return summary


def _render_position_breakdown_text(rows, tz_name: str, width: int = 220) -> str:
    names = {wallet.lower(): wallet[:10] for wallet in settings.watch_whales}
    wallet_order = {wallet.lower(): idx for idx, wallet in enumerate(settings.watch_whales)}
    console = Console(
        file=io.StringIO(),
        record=True,
        width=width,
        color_system=None,
        force_terminal=False,
    )

    console.print("Whale Aggregate Buy Copy Breakdown")
    console.print(f"Timezone: {tz_name}")
    console.print("One row = aggregate whale BUY position by wallet + market + side + local day.")
    console.print()

    summary_table = Table(title="Summary", box=box.ROUNDED, show_edge=True)
    for column, justify in (
        ("Day", "left"),
        ("Whale", "left"),
        ("Pos", "right"),
        ("Whale ct", "right"),
        ("Bot ct", "right"),
        ("Gap", "right"),
        ("Full", "right"),
        ("Part", "right"),
        ("No fill", "right"),
        ("No order", "right"),
        ("Over", "right"),
    ):
        summary_table.add_column(column, justify=justify, no_wrap=True)

    for (day, wallet), data in sorted(
        _position_summary(rows).items(),
        key=lambda item: (item[0][0], wallet_order.get(item[0][1], 999)),
    ):
        summary_table.add_row(
            day,
            names.get(wallet, wallet[:10]),
            _num(data["positions"]),
            _num(data["whale_contracts"]),
            _num(data["filled_contracts"]),
            _num(data["unfilled_gap"]),
            _num(data["FULL"]),
            _num(data["PARTIAL"]),
            _num(data["NO FILL"]),
            _num(data["NO ORDER"]),
            _num(data["OVERCOPIED"]),
        )
    console.print(summary_table)

    detail = Table(title="Positions", box=box.ROUNDED, show_edge=True, show_lines=True)
    for column, justify, nowrap in (
        ("Day", "left", True),
        ("Whale", "left", True),
        ("City", "left", False),
        ("Temp", "right", True),
        ("Date", "left", True),
        ("Side", "center", True),
        ("W fills", "right", True),
        ("W ct", "right", True),
        ("W cost", "right", True),
        ("Attempts", "right", True),
        ("Bot ct", "right", True),
        ("Gap", "right", True),
        ("Copy", "right", True),
        ("Status", "left", True),
        ("Reason", "left", True),
        ("Source", "left", True),
    ):
        detail.add_column(column, justify=justify, no_wrap=nowrap)

    for row in sorted(rows, key=lambda r: (str(r.local_day), wallet_order.get(r.wallet_address, 999), r.question, r.outcome)):
        whale_contracts = float(row.whale_contracts or 0)
        filled_contracts = float(row.filled_contracts or 0)
        unfilled_gap = max(whale_contracts - filled_contracts, 0.0)
        city, temp, market_date = _position_parts(row.question, row.condition_id)
        detail.add_row(
            str(row.local_day),
            names.get(row.wallet_address, row.wallet_address[:10]),
            city,
            temp,
            market_date,
            row.outcome,
            _num(row.whale_buy_fills),
            _num(whale_contracts),
            _money(row.whale_cost),
            _num(row.order_attempts),
            _num(filled_contracts),
            _num(unfilled_gap),
            _copy_pct(whale_contracts, filled_contracts),
            _position_copy_status(whale_contracts, filled_contracts, row.order_attempts),
            _fallback_reason(row),
            _source_label(row),
        )
    console.print()
    console.print(detail)
    return console.export_text(styles=False).rstrip()


def _emit_report(rendered: str, output_file: str | None) -> None:
    if not output_file:
        print(rendered)
        return

    path = Path(output_file).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{rendered}\n", encoding="utf-8")
    print(f"Wrote report to {path}")


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=2)
    parser.add_argument("--timezone", default="America/Montreal")
    parser.add_argument("--midpoint-timeout", type=float, default=5.0)
    parser.add_argument("--price-timeout-seconds", type=float, default=None)
    parser.add_argument("--no-live-midpoints", action="store_true")
    parser.add_argument("--allow-missing-live-prices", action="store_true")
    parser.add_argument("--position-breakdown", action="store_true")
    parser.add_argument("--format", choices=("markdown", "text"), default="text")
    parser.add_argument("--output-file")
    args = parser.parse_args()
    price_timeout = args.price_timeout_seconds if args.price_timeout_seconds is not None else args.midpoint_timeout

    start_utc, end_utc = _local_window(args.days, args.timezone)
    if args.position_breakdown:
        rows = await _fetch_position_breakdown_rows(start_utc, end_utc, args.timezone)
        if args.format == "markdown":
            rendered = _render_position_breakdown_markdown(rows, args.timezone)
        else:
            rendered = _render_position_breakdown_text(rows, args.timezone)
        _emit_report(rendered, args.output_file)
        return

    rows = await _fetch_rows(start_utc, end_utc, args.timezone)
    if args.no_live_midpoints:
        mids = {}
        price_fetched_at = None
        price_source = "disabled"
    else:
        mids, price_fetched_at = await _midpoints(
            rows,
            price_timeout,
            allow_missing=args.allow_missing_live_prices,
        )
        price_source = "clob_midpoints"

    addrs = list(settings.watch_whales) + [settings.polymarket_wallet_address]
    deltas = await _fetch_pnl_deltas([a for a in addrs if a], args.days)
    bot_delta = deltas.get((settings.polymarket_wallet_address or "").lower(), 0.0)
    bot_realized = await _fetch_bot_realized(start_utc)

    render_fn = _render_markdown if args.format == "markdown" else _render_text
    rendered = render_fn(
        rows,
        mids,
        args.timezone,
        price_fetched_at=price_fetched_at,
        price_source=price_source,
        pnl_deltas=deltas,
        bot_pnl_delta=bot_delta,
        bot_realized=bot_realized,
        days=args.days,
    )
    _emit_report(rendered, args.output_file)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except FreshPriceError as exc:
        print(f"ERROR fresh_price_required: {exc}", file=sys.stderr)
        raise SystemExit(2)
