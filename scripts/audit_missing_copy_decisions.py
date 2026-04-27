"""Audit whale BUY aggregates that have no durable copy decision."""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from rich import box
from rich.console import Console
from rich.table import Table
from sqlalchemy import text

from src.config import settings
from src.db import async_session
from src.signals.weather_resolution import parse_dt, weather_local_resolution_cutoff


def _local_window(days: int, tz_name: str) -> tuple[datetime, datetime]:
    tz = ZoneInfo(tz_name)
    today = datetime.now(tz).date()
    start_local = datetime.combine(today - timedelta(days=days - 1), datetime.min.time(), tzinfo=tz)
    end_local = datetime.combine(today + timedelta(days=1), datetime.min.time(), tzinfo=tz)
    return start_local.astimezone(ZoneInfo("UTC")), end_local.astimezone(ZoneInfo("UTC"))


def _reason(row) -> str:
    if row.category != "weather":
        return f"non_weather:{row.category or 'unknown'}"
    first_whale_ts = parse_dt(row.first_whale_ts)
    last_whale_ts = parse_dt(row.last_whale_ts) or first_whale_ts
    cutoff = weather_local_resolution_cutoff(
        title=row.question,
        category=row.category,
        resolution_time=row.resolution_time,
        now_utc=first_whale_ts,
    )
    if cutoff is not None and first_whale_ts is not None:
        if first_whale_ts >= cutoff:
            return "past_city_cutoff"
        if last_whale_ts is not None and last_whale_ts >= cutoff:
            return "mixed_city_cutoff"
    return "missed_before_decision"


async def _fetch_missing(start_utc: datetime, end_utc: datetime, tz_name: str, limit: int):
    sql = text(
        """
        WITH wb AS (
            SELECT wt.wallet_address,
                   wt.condition_id,
                   wt.outcome,
                   (wt.timestamp AT TIME ZONE :tz)::date AS local_day,
                   min(wt.id) AS first_whale_trade_id,
                   min(wt.timestamp) AS first_whale_ts,
                   max(wt.timestamp) AS last_whale_ts,
                   count(*) AS whale_fills,
                   sum(wt.num_contracts)::float AS whale_contracts,
                   sum(wt.size_usdc)::float AS whale_cost
            FROM whale_trades wt
            WHERE wt.side = 'BUY'
              AND wt.timestamp >= :start_utc
              AND wt.timestamp < :end_utc
              AND wt.wallet_address = ANY(:wallets)
            GROUP BY 1, 2, 3, 4
        )
        SELECT wb.*,
               coalesce(m.question, '') AS question,
               coalesce(m.category_override, m.category, '') AS category,
               m.resolution_time
        FROM wb
        LEFT JOIN markets m ON m.condition_id = wb.condition_id
        WHERE NOT EXISTS (
            SELECT 1
            FROM whale_trades wt
            JOIN copy_decisions cd ON cd.whale_trade_id = wt.id
            WHERE wt.wallet_address = wb.wallet_address
              AND wt.condition_id = wb.condition_id
              AND wt.outcome = wb.outcome
              AND (wt.timestamp AT TIME ZONE :tz)::date = wb.local_day
              AND cd.decision_code <> 'SELL_IGNORED'
        )
          AND NOT EXISTS (
            SELECT 1
            FROM copy_decisions cd
            WHERE cd.whale_trade_id IS NULL
              AND cd.wallet_address = wb.wallet_address
              AND cd.condition_id = wb.condition_id
              AND cd.outcome = wb.outcome
              AND (cd.decided_at AT TIME ZONE :tz)::date = wb.local_day
              AND cd.decision_code <> 'SELL_IGNORED'
        )
          AND NOT EXISTS (
            SELECT 1
            FROM my_trades t
            CROSS JOIN LATERAL unnest(t.source_wallets) AS sw(wallet_address)
            WHERE lower(sw.wallet_address) = wb.wallet_address
              AND t.condition_id = wb.condition_id
              AND t.outcome = wb.outcome
              AND (t.entry_timestamp AT TIME ZONE :tz)::date = wb.local_day
        )
        ORDER BY wb.local_day DESC, wb.whale_cost DESC
        LIMIT :limit
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
                "limit": limit,
            },
        )
        return result.fetchall()


async def _apply_repairs(rows) -> int:
    sql = text(
        """
        INSERT INTO copy_decisions (
            whale_trade_id, wallet_address, condition_id, outcome,
            decision_code, decision_reason, decision_source,
            event_timestamp, decided_at, requested_contracts, context
        )
        VALUES (
            :whale_trade_id, :wallet_address, :condition_id, :outcome,
            :decision_code, :decision_reason, 'audit_repair',
            :event_timestamp, now(), :requested_contracts, CAST(:context AS jsonb)
        )
        """
    )
    inserted = 0
    async with async_session() as session:
        for row in rows:
            reason = _reason(row)
            await session.execute(
                sql,
                {
                    "whale_trade_id": row.first_whale_trade_id,
                    "wallet_address": row.wallet_address,
                    "condition_id": row.condition_id,
                    "outcome": row.outcome,
                    "decision_code": reason,
                    "decision_reason": reason,
                    "event_timestamp": row.first_whale_ts,
                    "requested_contracts": Decimal(str(row.whale_contracts or 0)),
                    "context": (
                        '{"note":"historical audit repair only; no order was placed",'
                        f'"local_day":"{row.local_day}",'
                        f'"whale_fills":{int(row.whale_fills or 0)}}}'
                    ),
                },
            )
            inserted += 1
        await session.commit()
    return inserted


def _render(rows) -> None:
    table = Table(title="Missing Copy Decisions", box=box.ROUNDED, show_lines=True)
    for column, justify in (
        ("Day", "left"),
        ("Whale", "left"),
        ("Market", "left"),
        ("Side", "center"),
        ("Fills", "right"),
        ("Contracts", "right"),
        ("Cost", "right"),
        ("First Buy", "left"),
        ("Reason", "left"),
    ):
        table.add_column(column, justify=justify, no_wrap=(column != "Market"))
    for row in rows:
        table.add_row(
            str(row.local_day),
            row.wallet_address[:10],
            (row.question or row.condition_id)[:70],
            row.outcome,
            str(row.whale_fills),
            f"{float(row.whale_contracts or 0):,.2f}",
            f"${float(row.whale_cost or 0):,.2f}",
            str(row.first_whale_ts),
            _reason(row),
        )
    Console(width=180).print(table)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=2)
    parser.add_argument("--timezone", default="America/Montreal")
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    start_utc, end_utc = _local_window(args.days, args.timezone)
    rows = await _fetch_missing(start_utc, end_utc, args.timezone, args.limit)
    _render(rows)
    if args.apply:
        inserted = await _apply_repairs(rows)
        print(f"Inserted {inserted} audit_repair copy_decisions")
    else:
        print("Dry run only. Re-run with --apply to insert audit_repair copy_decisions.")


if __name__ == "__main__":
    asyncio.run(main())
