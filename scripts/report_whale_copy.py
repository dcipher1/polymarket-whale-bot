"""Per-whale copy report with fill state, decision reason, and MTM."""

from __future__ import annotations

import argparse
import asyncio
import io
import math
from datetime import datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from rich import box
from rich.console import Console
from rich.table import Table
from sqlalchemy import text

from src.config import settings
from src.db import async_session
from src.polymarket.clob_client import CLOBClient


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
        return "orders cancelled/no fill"
    if int(row.order_attempts or 0) > 0:
        return "filled/part-filled"
    if not row.question:
        return "missing market"
    if row.category != "weather":
        return f"non-weather:{row.category or 'unknown'}"
    if row.resolution_time and row.resolution_time < datetime.now(row.resolution_time.tzinfo):
        return "past resolution/stale"
    return "no order logged"


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
        latest_decision AS (
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
               ld.decision_code AS latest_decision_code,
               ld.decision_reason AS latest_decision_reason,
               ld.decision_source AS latest_decision_source,
               ld.latency_seconds AS latest_latency_seconds,
               ld.decided_at AS latest_decided_at
        FROM wb
        LEFT JOIN ot
          ON ot.wallet_address = wb.wallet_address
         AND ot.condition_id = wb.condition_id
         AND ot.outcome = wb.outcome
         AND ot.local_day = wb.local_day
        LEFT JOIN markets m ON m.condition_id = wb.condition_id
        LEFT JOIN latest_decision ld
          ON ld.wallet_address = wb.wallet_address
         AND ld.condition_id = wb.condition_id
         AND ld.outcome = wb.outcome
         AND ld.local_day = wb.local_day
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
        latest_decision AS (
            SELECT DISTINCT ON (wallet_address, condition_id, outcome)
                   wallet_address,
                   condition_id,
                   outcome,
                   decision_code,
                   decision_reason,
                   decision_source,
                   latency_seconds,
                   decided_at
            FROM copy_decisions
            WHERE decided_at >= :start_utc
              AND decided_at < :end_utc
              AND decision_code <> 'SELL_IGNORED'
            ORDER BY wallet_address, condition_id, outcome, decided_at DESC
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
               ld.decision_code AS latest_decision_code,
               ld.decision_reason AS latest_decision_reason,
               ld.decision_source AS latest_decision_source,
               ld.latency_seconds AS latest_latency_seconds
        FROM wb
        LEFT JOIN ot
          ON ot.wallet_address = wb.wallet_address
         AND ot.condition_id = wb.condition_id
         AND ot.outcome = wb.outcome
        LEFT JOIN markets m ON m.condition_id = wb.condition_id
        LEFT JOIN latest_decision ld
          ON ld.wallet_address = wb.wallet_address
         AND ld.condition_id = wb.condition_id
         AND ld.outcome = wb.outcome
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


async def _midpoints(rows, timeout_seconds: float) -> dict[str, float | None]:
    clob = CLOBClient()
    try:
        tokens = sorted({row.token_id for row in rows if row.token_id})
        sem = asyncio.Semaphore(24)
        mids: dict[str, float | None] = {}

        async def one(token_id: str):
            async with sem:
                try:
                    mids[token_id] = await asyncio.wait_for(
                        clob.get_midpoint(token_id),
                        timeout=timeout_seconds,
                    )
                except asyncio.TimeoutError:
                    mids[token_id] = None

        await asyncio.gather(*(one(token) for token in tokens))
        return mids
    finally:
        await clob.close()


def _local_window(days: int, tz_name: str) -> tuple[datetime, datetime]:
    tz = ZoneInfo(tz_name)
    today = datetime.now(tz).date()
    start_local = datetime.combine(today - timedelta(days=days - 1), datetime.min.time(), tzinfo=tz)
    end_local = datetime.combine(today + timedelta(days=1), datetime.min.time(), tzinfo=tz)
    return start_local.astimezone(ZoneInfo("UTC")), end_local.astimezone(ZoneInfo("UTC"))


def _render_markdown(rows, mids, tz_name: str) -> str:
    names = {wallet.lower(): wallet[:10] for wallet in settings.watch_whales}
    lines = [
        "# Whale Copy Report",
        f"Timezone: {tz_name}",
        "MTM uses current CLOB midpoint when available; resolved markets use 1/0.",
    ]
    summary = {}
    for row in rows:
        key = str(row.local_day)
        summary.setdefault(key, {"positions": 0, "ordered": 0, "filled": 0, "whale_mtm": 0.0, "our_mtm": 0.0})
        summary[key]["positions"] += 1
        summary[key]["ordered"] += int((row.order_attempts or 0) > 0)
        summary[key]["filled"] += int((row.filled_contracts or 0) > 0)

    enriched = []
    for row in rows:
        mid = mids.get(row.token_id)
        if row.resolved and row.resolved_outcome:
            mid = 1.0 if row.resolved_outcome == row.outcome else 0.0
        whale_contracts = float(row.whale_contracts or 0)
        whale_cost = float(row.whale_cost or 0)
        filled_contracts = float(row.filled_contracts or 0)
        filled_cost = float(row.filled_cost or 0)
        whale_mtm = None if mid is None else whale_contracts * mid - whale_cost
        our_mtm = 0.0 if filled_contracts <= 0 else (None if mid is None else filled_contracts * mid - filled_cost)
        if whale_mtm is not None:
            summary[str(row.local_day)]["whale_mtm"] += whale_mtm
        if our_mtm is not None:
            summary[str(row.local_day)]["our_mtm"] += our_mtm
        enriched.append((row, mid, whale_mtm, our_mtm))

    lines.append("\n## Summary")
    lines.append("| Day | Positions | Ordered | Filled | Whale MTM | Our MTM |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for day, data in sorted(summary.items()):
        lines.append(
            f"| {day} | {data['positions']} | {data['ordered']} | {data['filled']} | "
            f"{_money(data['whale_mtm'])} | {_money(data['our_mtm'])} |"
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
                source = row.latest_decision_source or "inferred"
                if row.latest_latency_seconds is not None:
                    source = f"{source}/{float(row.latest_latency_seconds):.1f}s"
                lines.append(
                    "| "
                    + " | ".join(
                        [
                            _short_market(row.question, row.condition_id),
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

    for day in sorted({str(row.local_day) for row in rows}):
        lines.append(f"\n## {day}")
        for wallet in sorted(
            {row.wallet_address for row in rows if str(row.local_day) == day},
            key=lambda value: wallet_order.get(value, 999),
        ):
            wallet_rows = [
                row for row in rows
                if str(row.local_day) == day and row.wallet_address == wallet
            ]
            if not wallet_rows:
                continue
            lines.append(f"\n### Whale {names.get(wallet, wallet[:10])}")
            lines.append(
                "| Market | Side | Whale buy fills | Whale contracts | Whale cost | Bot attempts | "
                "Bot filled | Unfilled gap | Copy % | Status | Reason | Source |"
            )
            lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|")
            for row in wallet_rows:
                whale_contracts = float(row.whale_contracts or 0)
                filled_contracts = float(row.filled_contracts or 0)
                unfilled_gap = max(whale_contracts - filled_contracts, 0.0)
                status = _position_copy_status(whale_contracts, filled_contracts, row.order_attempts)
                source = row.latest_decision_source or "inferred"
                if row.latest_latency_seconds is not None:
                    source = f"{source}/{float(row.latest_latency_seconds):.1f}s"
                lines.append(
                    "| "
                    + " | ".join(
                        [
                            _short_market(row.question, row.condition_id),
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
                            source,
                        ]
                    )
                    + " |"
                )
    return "\n".join(lines)


def _source_label(row) -> str:
    source = row.latest_decision_source or "inferred"
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

    summary_table = Table(title="Summary", box=box.SIMPLE, show_edge=True)
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

    for day in sorted({str(row.local_day) for row in rows}):
        console.print()
        console.print(f"{day}")
        for wallet in sorted(
            {row.wallet_address for row in rows if str(row.local_day) == day},
            key=lambda value: wallet_order.get(value, 999),
        ):
            wallet_rows = [
                row for row in rows
                if str(row.local_day) == day and row.wallet_address == wallet
            ]
            if not wallet_rows:
                continue
            detail = Table(title=f"Whale {names.get(wallet, wallet[:10])}", box=box.SIMPLE, show_edge=True)
            for column, justify in (
                ("Market", "left"),
                ("Side", "center"),
                ("W fills", "right"),
                ("W ct", "right"),
                ("W cost", "right"),
                ("Attempts", "right"),
                ("Bot ct", "right"),
                ("Gap", "right"),
                ("Copy", "right"),
                ("Status", "left"),
                ("Reason", "left"),
                ("Source", "left"),
            ):
                detail.add_column(column, justify=justify, no_wrap=(column != "Market"))

            for row in wallet_rows:
                whale_contracts = float(row.whale_contracts or 0)
                filled_contracts = float(row.filled_contracts or 0)
                unfilled_gap = max(whale_contracts - filled_contracts, 0.0)
                detail.add_row(
                    _short_market(row.question, row.condition_id),
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
            console.print(detail)
    return console.export_text(styles=False).rstrip()


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=2)
    parser.add_argument("--timezone", default="America/Montreal")
    parser.add_argument("--midpoint-timeout", type=float, default=2.5)
    parser.add_argument("--no-live-midpoints", action="store_true")
    parser.add_argument("--position-breakdown", action="store_true")
    parser.add_argument("--format", choices=("markdown", "text"), default="text")
    args = parser.parse_args()

    start_utc, end_utc = _local_window(args.days, args.timezone)
    if args.position_breakdown:
        rows = await _fetch_position_breakdown_rows(start_utc, end_utc, args.timezone)
        if args.format == "markdown":
            print(_render_position_breakdown_markdown(rows, args.timezone))
        else:
            print(_render_position_breakdown_text(rows, args.timezone))
        return

    rows = await _fetch_rows(start_utc, end_utc, args.timezone)
    mids = {} if args.no_live_midpoints else await _midpoints(rows, args.midpoint_timeout)
    print(_render_markdown(rows, mids, args.timezone))


if __name__ == "__main__":
    asyncio.run(main())
