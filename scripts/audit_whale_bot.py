"""Read-only whale bot trade audit report.

Usage:
    .venv/bin/python scripts/audit_whale_bot.py
"""

import asyncio

from sqlalchemy import text

from src.db import async_session


async def _rows(sql: str):
    async with async_session() as session:
        result = await session.execute(text(sql))
        return result.fetchall()


def _print_section(title: str, rows) -> None:
    print(f"\n== {title} ==")
    if not rows:
        print("(none)")
        return
    for row in rows:
        print(" | ".join("" if value is None else str(value) for value in row))


async def main() -> None:
    _print_section(
        "Order Ledger By Status",
        await _rows(
            """
            SELECT fill_status, resolved, count(*) AS rows,
                   round(coalesce(sum(size_usdc), 0), 2) AS logged_notional,
                   round(coalesce(sum(pnl_usdc), 0), 2) AS pnl
            FROM my_trades
            GROUP BY fill_status, resolved
            ORDER BY fill_status, resolved
            """
        ),
    )

    _print_section(
        "Economic Performance By Source Wallet",
        await _rows(
            """
            SELECT coalesce(sw, 'unknown') AS wallet,
                   count(*) FILTER (WHERE fill_status IN ('FILLED','PARTIAL','PAPER')) AS economic_rows,
                   round(coalesce(sum(size_usdc) FILTER (WHERE fill_status IN ('FILLED','PARTIAL','PAPER')), 0), 2) AS exposure,
                   count(*) FILTER (WHERE fill_status = 'CANCELLED') AS cancelled_attempts,
                   round(coalesce(sum(pnl_usdc) FILTER (
                       WHERE resolved AND fill_status IN ('FILLED','PARTIAL','PAPER')
                   ), 0), 2) AS realized_pnl
            FROM my_trades
            LEFT JOIN LATERAL unnest(source_wallets) sw ON true
            GROUP BY 1
            ORDER BY realized_pnl ASC
            """
        ),
    )

    _print_section(
        "Worst Resolved Economic Markets",
        await _rows(
            """
            SELECT left(t.condition_id, 12) AS cid,
                   left(coalesce(m.question, ''), 80) AS market,
                   t.outcome,
                   count(*) AS rows,
                   round(sum(t.size_usdc), 2) AS exposure,
                   round(coalesce(sum(t.pnl_usdc), 0), 2) AS pnl
            FROM my_trades t
            LEFT JOIN markets m ON m.condition_id = t.condition_id
            WHERE t.resolved
              AND t.fill_status IN ('FILLED','PARTIAL','PAPER')
            GROUP BY 1, 2, 3
            ORDER BY pnl ASC
            LIMIT 20
            """
        ),
    )

    _print_section(
        "Open/Past-Resolution Exposure",
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
            LIMIT 30
            """
        ),
    )

    _print_section(
        "Resolution Anomalies",
        await _rows(
            """
            SELECT
                count(*) FILTER (WHERE NOT resolved AND resolution_time < now()) AS unresolved_past_resolution,
                count(*) FILTER (WHERE NOT resolved AND outcome IS NOT NULL) AS unresolved_with_outcome,
                count(*) FILTER (WHERE resolved) AS resolved_markets,
                count(*) AS total_markets
            FROM markets
            """
        ),
    )


if __name__ == "__main__":
    asyncio.run(main())
