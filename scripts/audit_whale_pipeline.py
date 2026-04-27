"""Read-only operational audit for the whale copy bot pipeline."""

import argparse
import asyncio
import json
import re
import subprocess
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from sqlalchemy import text

from src.config import settings
from src.db import async_session
from src.events import cache_get


LOG_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")


def _parse_log_time(line: str, tz_name: str) -> datetime | None:
    match = LOG_TS_RE.match(line)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo(tz_name))
    except ValueError:
        return None


def _classify_log_line(line: str) -> str | None:
    lower = line.lower()
    if "failed to fetch" in lower and "temporary failure in name resolution" in lower:
        return "network_dns_failure"
    if "timed out during opening handshake" in lower and "polynode" in lower:
        return "polynode_handshake_timeout"
    if "price (0.0)" in lower or "@ 0.0000" in lower:
        return "zero_price_order_attempt"
    if "resolved as unfilled" in lower and "fill_status=cancelled" in lower:
        return "cancelled_unfilled_resolution_noise"
    if "raised an exception" in lower:
        return "scheduler_exception"
    if "traceback" in lower:
        return "traceback"
    if "order placement failed" in lower or "returned no id" in lower:
        return "order_placement_failed"
    if "get_positions failed" in lower:
        return "data_api_positions_failed"
    if "balance" in lower and "failed" in lower:
        return "balance_fetch_failed"
    if "warning" in lower or "error" in lower or "critical" in lower:
        return "other_warning_error"
    return None


def _summarize_logs(log_path: Path, days: int, tz_name: str) -> tuple[Counter, dict[str, list[str]], datetime]:
    tz = ZoneInfo(tz_name)
    cutoff = datetime.now(tz) - timedelta(days=days)
    counts: Counter[str] = Counter()
    samples: dict[str, list[str]] = defaultdict(list)
    if not log_path.exists():
        return counts, samples, cutoff
    current_ts: datetime | None = None
    with log_path.open("r", errors="replace") as fh:
        for raw in fh:
            line = raw.rstrip()
            parsed = _parse_log_time(line, tz_name)
            if parsed is not None:
                current_ts = parsed
            if current_ts is None or current_ts < cutoff:
                continue
            category = _classify_log_line(line)
            if not category:
                continue
            counts[category] += 1
            if len(samples[category]) < 3:
                samples[category].append(line[:220])
    return counts, samples, cutoff


def _print_log_summary(log_path: Path, days: int, tz_name: str) -> None:
    counts, samples, cutoff = _summarize_logs(log_path, days, tz_name)
    print(f"\n== Log Summary ({log_path}, since {cutoff.isoformat()}) ==")
    if not counts:
        print("(none)")
        return
    for category, count in counts.most_common():
        print(f"{category} | {count}")
        for sample in samples.get(category, []):
            print(f"  sample: {sample}")


async def _rows(sql: str, params: dict | None = None):
    async with async_session() as session:
        result = await session.execute(text(sql), params or {})
        return result.fetchall()


async def _scalar(sql: str, params: dict | None = None):
    rows = await _rows(sql, params)
    return rows[0][0] if rows else None


async def _cache_get_with_timeout(key: str):
    return await asyncio.wait_for(cache_get(key), timeout=1.0)


def _print_rows(title: str, rows) -> None:
    print(f"\n== {title} ==")
    if not rows:
        print("(none)")
        return
    for row in rows:
        print(" | ".join("" if value is None else str(value) for value in row))


def _process_rows() -> list[str]:
    try:
        proc = subprocess.run(
            ["pgrep", "-af", r"\.venv/bin/python -m src\.main|python -m src\.main"],
            check=False,
            text=True,
            capture_output=True,
        )
    except Exception as exc:
        return [f"process check failed: {exc}"]
    return [line for line in proc.stdout.splitlines() if line.strip()]


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=2)
    parser.add_argument("--timezone", default="America/Montreal")
    parser.add_argument("--log", default="bot.log")
    args = parser.parse_args()

    print("# Whale Pipeline Audit")
    print(f"generated_at={datetime.now(timezone.utc).isoformat()}")
    print(f"live_execution_enabled={settings.live_execution_enabled}")
    print(f"polynode_enabled={settings.polynode_enabled}")
    print(f"polynode_key_present={bool(settings.polynode_api_key)}")
    print(f"fallback_interval_seconds={settings.wallet_sync_fallback_interval_seconds}")
    print(f"audit_days={args.days}")
    print(f"audit_timezone={args.timezone}")

    _print_log_summary(Path(args.log), args.days, args.timezone)

    _print_rows("Process", [(line,) for line in _process_rows()])

    try:
        health = await _cache_get_with_timeout("polynode:wallet_stream:health")
    except Exception as exc:
        health = {"error": str(exc)}
    print("\n== PolyNode Health ==")
    print(json.dumps(health or {}, sort_keys=True, default=str))

    _print_rows(
        "Alembic",
        await _rows("SELECT version_num FROM alembic_version ORDER BY version_num"),
    )

    _print_rows(
        "Pipeline Counts",
        await _rows(
            """
            SELECT 'unresolved_past_resolution', count(*)::text
            FROM markets
            WHERE resolved = false
              AND resolution_time IS NOT NULL
              AND resolution_time < now()
            UNION ALL
            SELECT 'pending_my_trades', count(*)::text
            FROM my_trades
            WHERE fill_status = 'PENDING'
            UNION ALL
            SELECT 'open_whale_positions_past_resolution', count(*)::text
            FROM whale_positions wp
            JOIN markets m ON m.condition_id = wp.condition_id
            WHERE wp.is_open = true
              AND m.resolution_time < now()
            UNION ALL
            SELECT 'whale_trades_missing_market', count(*)::text
            FROM whale_trades wt
            LEFT JOIN markets m ON m.condition_id = wt.condition_id
            WHERE m.condition_id IS NULL
            UNION ALL
            SELECT 'whale_trades_missing_token', count(*)::text
            FROM whale_trades wt
            LEFT JOIN market_tokens mt ON mt.token_id = wt.token_id
            WHERE mt.token_id IS NULL
            UNION ALL
            SELECT 'open_backlog_events', count(*)::text
            FROM whale_event_backlog
            WHERE resolved_at IS NULL
            UNION ALL
            SELECT 'copy_decisions_24h', count(*)::text
            FROM copy_decisions
            WHERE decided_at >= now() - interval '24 hours'
            """
        ),
    )

    _print_rows(
        f"Copy Decisions {args.days}d",
        await _rows(
            """
            SELECT decision_source,
                   decision_code,
                   count(*) AS rows,
                   round(avg(latency_seconds), 3) AS avg_latency_seconds,
                   max(decided_at) AS last_decided_at
            FROM copy_decisions
            WHERE decided_at >= now() - make_interval(days => :days)
            GROUP BY 1, 2
            ORDER BY rows DESC, decision_source, decision_code
            """,
            {"days": args.days},
        ),
    )

    _print_rows(
        f"Websocket/Data API BUY Coverage {args.days}d",
        await _rows(
            """
            WITH recent_buys AS (
                SELECT wt.id,
                       wt.wallet_address,
                       wt.condition_id,
                       wt.outcome,
                       wt.timestamp,
                       wt.detected_at,
                       wt.num_contracts,
                       wt.size_usdc
                FROM whale_trades wt
                WHERE wt.side = 'BUY'
                  AND wt.wallet_address = ANY(:wallets)
                  AND wt.timestamp >= now() - make_interval(days => :days)
            ),
            classified AS (
                SELECT rb.*,
                       EXISTS (
                           SELECT 1
                           FROM copy_decisions cd
                           WHERE cd.decision_source = 'websocket'
                             AND (
                                 cd.whale_trade_id = rb.id
                                 OR (
                                     cd.wallet_address = rb.wallet_address
                                     AND cd.condition_id = rb.condition_id
                                     AND cd.outcome = rb.outcome
                                     AND cd.event_timestamp BETWEEN rb.timestamp - interval '30 seconds'
                                                                AND rb.timestamp + interval '30 seconds'
                                 )
                             )
                       ) AS has_websocket_decision,
                       EXISTS (
                           SELECT 1
                           FROM my_trades t
                           WHERE t.condition_id = rb.condition_id
                             AND t.outcome = rb.outcome
                             AND rb.wallet_address = ANY(t.source_wallets)
                             AND t.entry_timestamp BETWEEN rb.timestamp - interval '10 minutes'
                                                       AND rb.timestamp + interval '10 minutes'
                       ) AS has_nearby_bot_order
                FROM recent_buys rb
            )
            SELECT left(wallet_address, 12) AS wallet,
                   count(*) AS buy_fills,
                   count(*) FILTER (WHERE has_websocket_decision) AS websocket_seen,
                   count(*) FILTER (WHERE NOT has_websocket_decision) AS websocket_missing,
                   count(*) FILTER (WHERE has_nearby_bot_order) AS nearby_bot_order,
                   round(sum(size_usdc), 2) AS whale_cost
            FROM classified
            GROUP BY 1
            ORDER BY 1
            """,
            {"wallets": settings.watch_whales, "days": args.days},
        ),
    )

    _print_rows(
        f"Recent Possible Missed BUYs {args.days}d",
        await _rows(
            """
            SELECT wt.timestamp,
                   left(wt.wallet_address, 12) AS wallet,
                   left(coalesce(m.question, ''), 70) AS market,
                   wt.outcome,
                   round(wt.num_contracts, 2) AS contracts,
                   round(wt.size_usdc, 2) AS cost
            FROM whale_trades wt
            LEFT JOIN markets m ON m.condition_id = wt.condition_id
            WHERE wt.side = 'BUY'
              AND wt.wallet_address = ANY(:wallets)
              AND wt.timestamp >= now() - make_interval(days => :days)
              AND NOT EXISTS (
                  SELECT 1
                  FROM copy_decisions cd
                  WHERE cd.decision_source = 'websocket'
                    AND (
                        cd.whale_trade_id = wt.id
                        OR (
                            cd.wallet_address = wt.wallet_address
                            AND cd.condition_id = wt.condition_id
                            AND cd.outcome = wt.outcome
                            AND cd.event_timestamp BETWEEN wt.timestamp - interval '30 seconds'
                                                       AND wt.timestamp + interval '30 seconds'
                        )
                    )
              )
            ORDER BY wt.timestamp DESC
            LIMIT 30
            """,
            {"wallets": settings.watch_whales, "days": args.days},
        ),
    )

    _print_rows(
        f"Order Failure Context {args.days}d",
        await _rows(
            """
            SELECT decision_code,
                   left(coalesce(context->>'market_title', ''), 70) AS market,
                   context->>'midpoint' AS midpoint,
                   context->>'bid_price' AS bid_price,
                   context->>'order_price' AS order_price,
                   context->>'best_bid' AS best_bid,
                   context->>'best_ask' AS best_ask,
                   count(*) AS rows,
                   max(decided_at) AS last_at
            FROM copy_decisions
            WHERE decided_at >= now() - make_interval(days => :days)
              AND decision_code IN (
                  'place_failed',
                  'price_below_tick',
                  'no_book',
                  'under_min',
                  'slippage_above',
                  'stale_below',
                  'position_cap'
              )
            GROUP BY 1,2,3,4,5,6,7
            ORDER BY rows DESC, last_at DESC
            LIMIT 40
            """,
            {"days": args.days},
        ),
    )

    _print_rows(
        "Recent Copy Decisions",
        await _rows(
            """
            SELECT decided_at,
                   decision_source,
                   left(coalesce(wallet_address, ''), 12) AS wallet,
                   left(coalesce(condition_id, ''), 12) AS cid,
                   outcome,
                   decision_code,
                   decision_reason,
                   latency_seconds
            FROM copy_decisions
            ORDER BY decided_at DESC
            LIMIT 30
            """
        ),
    )

    _print_rows(
        "Order Ledger",
        await _rows(
            """
            SELECT fill_status,
                   resolved,
                   count(*) AS rows,
                   round(coalesce(sum(size_usdc), 0), 2) AS notional,
                   round(coalesce(sum(pnl_usdc), 0), 2) AS pnl
            FROM my_trades
            GROUP BY 1, 2
            ORDER BY 1, 2
            """
        ),
    )

    _print_rows(
        "Watched Wallet Ingestion",
        await _rows(
            """
            SELECT address, last_ingested_ts, total_trades, last_active
            FROM wallets
            WHERE address = ANY(:wallets)
            ORDER BY address
            """,
            {"wallets": settings.watch_whales},
        ),
    )

    _print_rows(
        "Schema Constraints",
        await _rows(
            """
            SELECT conrelid::regclass::text AS table_name,
                   conname,
                   contype,
                   convalidated,
                   pg_get_constraintdef(oid)
            FROM pg_constraint
            WHERE conrelid::regclass::text IN (
                'whale_trades',
                'market_tokens',
                'whale_positions',
                'my_trades',
                'whale_event_backlog',
                'copy_decisions'
            )
            ORDER BY 1, 2
            """
        ),
    )

    _print_rows(
        "Top Open Past-Resolution Exposure",
        await _rows(
            """
            SELECT left(t.condition_id, 12) AS cid,
                   left(coalesce(m.question, ''), 80) AS market,
                   t.outcome,
                   m.resolution_time,
                   count(*) AS rows,
                   round(sum(t.size_usdc), 2) AS exposure
            FROM my_trades t
            LEFT JOIN markets m ON m.condition_id = t.condition_id
            WHERE NOT t.resolved
              AND t.fill_status IN ('FILLED','PARTIAL','PAPER')
              AND m.resolution_time < now()
            GROUP BY 1, 2, 3, 4
            ORDER BY exposure DESC
            LIMIT 20
            """
        ),
    )

    _print_rows(
        "Copy Target Overages",
        await _rows(
            """
            WITH bot AS (
                SELECT t.condition_id,
                       t.outcome,
                       lower(coalesce(t.source_wallets[1], t.attribution->>'source_wallet', t.attribution->>'wallet_address', '')) AS wallet_address,
                       sum(CASE WHEN t.fill_status IN ('FILLED','PARTIAL') THEN coalesce(t.num_contracts, 0) ELSE 0 END)::numeric AS filled_contracts,
                       sum(CASE WHEN t.fill_status = 'PENDING' THEN coalesce(nullif(t.attribution->>'requested_contracts', '')::numeric, t.num_contracts::numeric, 0) ELSE 0 END)::numeric AS reserved_contracts,
                       round(sum(CASE WHEN t.fill_status IN ('FILLED','PARTIAL') THEN coalesce(t.size_usdc, 0) ELSE 0 END), 2) AS filled_usdc,
                       round(sum(CASE WHEN t.fill_status = 'PENDING' THEN coalesce(nullif(t.attribution->>'requested_size_usdc', '')::numeric, t.size_usdc::numeric, 0) ELSE 0 END), 2) AS reserved_usdc
                FROM my_trades t
                WHERE t.fill_status IN ('FILLED','PARTIAL','PENDING')
                GROUP BY 1, 2, 3
            ),
            joined AS (
                SELECT b.wallet_address,
                       b.condition_id,
                       b.outcome,
                       b.filled_contracts,
                       b.reserved_contracts,
                       b.filled_contracts + b.reserved_contracts AS effective_contracts,
                       coalesce(wp.num_contracts, 0)::numeric AS whale_contracts,
                       floor(coalesce(wp.num_contracts, 0)::numeric * 1.05) AS buffered_max,
                       b.filled_usdc,
                       b.reserved_usdc,
                       m.question
                FROM bot b
                JOIN whale_positions wp
                  ON wp.wallet_address = b.wallet_address
                 AND wp.condition_id = b.condition_id
                 AND wp.outcome = b.outcome
                 AND wp.is_open = true
                LEFT JOIN markets m ON m.condition_id = b.condition_id
                WHERE b.wallet_address <> ''
            )
            SELECT left(wallet_address, 12) AS wallet,
                   left(condition_id, 12) AS cid,
                   left(coalesce(question, ''), 70) AS market,
                   outcome,
                   effective_contracts,
                   whale_contracts,
                   buffered_max,
                   effective_contracts - buffered_max AS over_buffer_contracts,
                   filled_contracts,
                   reserved_contracts,
                   filled_usdc,
                   reserved_usdc
            FROM joined
            WHERE effective_contracts > buffered_max
            ORDER BY over_buffer_contracts DESC
            LIMIT 40
            """
        ),
    )


if __name__ == "__main__":
    asyncio.run(main())
