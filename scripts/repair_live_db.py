"""One-shot live DB repair for websocket rollout prerequisites.

Dry-run by default. Use `--apply` to mutate rows and optionally run resolution
and wallet reconciliation passes until the past-resolution backlog is under the
configured halt threshold.
"""

import argparse
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from sqlalchemy import text, select

from src.config import settings
from src.db import async_session
from src.models import MyTrade

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


AUDIT_QUERIES = {
    "Order Ledger By Status": """
        SELECT fill_status, resolved, count(*) AS rows,
               round(coalesce(sum(size_usdc), 0), 2) AS logged_notional,
               round(coalesce(sum(pnl_usdc), 0), 2) AS pnl
        FROM my_trades
        GROUP BY fill_status, resolved
        ORDER BY fill_status, resolved
    """,
    "Resolution Anomalies": """
        SELECT
            count(*) FILTER (WHERE NOT resolved AND resolution_time < now()) AS unresolved_past_resolution,
            count(*) FILTER (WHERE NOT resolved AND outcome IS NOT NULL) AS unresolved_with_outcome,
            count(*) FILTER (WHERE resolved) AS resolved_markets,
            count(*) AS total_markets
        FROM markets
    """,
    "Cancelled/Failed Economic Leakage": """
        SELECT fill_status,
               count(*) AS rows,
               round(coalesce(sum(size_usdc), 0), 2) AS notional,
               round(coalesce(sum(pnl_usdc), 0), 2) AS pnl,
               round(coalesce(sum(num_contracts), 0), 2) AS contracts
        FROM my_trades
        WHERE fill_status IN ('CANCELLED', 'FAILED')
          AND (
              coalesce(size_usdc, 0) != 0
              OR coalesce(num_contracts, 0) != 0
              OR coalesce(pnl_usdc, 0) != 0
              OR coalesce(trade_outcome, '') NOT IN ('', 'UNFILLED')
          )
        GROUP BY fill_status
        ORDER BY fill_status
    """,
    "Stale Pending Orders": """
        SELECT count(*) AS rows,
               round(coalesce(sum(size_usdc), 0), 2) AS notional,
               min(entry_timestamp) AS oldest,
               max(entry_timestamp) AS newest
        FROM my_trades
        WHERE fill_status = 'PENDING'
          AND entry_timestamp < now() - interval '2 minutes'
    """,
}


async def _print_audit(title: str) -> None:
    print(f"\n# {title}")
    async with async_session() as session:
        for section, sql in AUDIT_QUERIES.items():
            print(f"\n== {section} ==")
            rows = (await session.execute(text(sql))).fetchall()
            if not rows:
                print("(none)")
                continue
            for row in rows:
                print(" | ".join("" if value is None else str(value) for value in row))


async def _repair_cancelled_failed(apply: bool) -> int:
    sql = text(
        """
        UPDATE my_trades
        SET size_usdc = 0,
            num_contracts = 0,
            pnl_usdc = 0,
            trade_outcome = 'UNFILLED'
        WHERE fill_status IN ('CANCELLED', 'FAILED')
          AND (
              coalesce(size_usdc, 0) != 0
              OR coalesce(num_contracts, 0) != 0
              OR coalesce(pnl_usdc, 0) != 0
              OR coalesce(trade_outcome, '') NOT IN ('', 'UNFILLED')
          )
        """
    )
    async with async_session() as session:
        result = await session.execute(sql)
        count = int(result.rowcount or 0)
        if apply:
            await session.commit()
        else:
            await session.rollback()
    return count


def _mark_unfilled(trade: MyTrade) -> None:
    attribution = dict(trade.attribution or {})
    if "requested_contracts" not in attribution and trade.num_contracts is not None:
        attribution["requested_contracts"] = int(trade.num_contracts or 0)
    if "requested_size_usdc" not in attribution and trade.size_usdc is not None:
        attribution["requested_size_usdc"] = str(trade.size_usdc)
    attribution["repair_reason"] = "stale_pending_unfilled"
    trade.attribution = attribution
    trade.fill_status = "CANCELLED"
    trade.size_usdc = Decimal("0")
    trade.num_contracts = 0
    trade.pnl_usdc = Decimal("0")
    trade.trade_outcome = "UNFILLED"


async def _repair_stale_pending(apply: bool, older_than_minutes: int) -> dict[str, int]:
    """Refresh/cancel old PENDING rows and zero the ones with no matched fill."""
    from src.execution.order_manager import cancel_order, get_order_status

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=older_than_minutes)
    stats = {
        "checked": 0,
        "cancel_requested": 0,
        "marked_cancelled": 0,
        "partial": 0,
        "unchanged": 0,
    }

    async with async_session() as session:
        result = await session.execute(
            select(MyTrade)
            .where(MyTrade.fill_status == "PENDING", MyTrade.entry_timestamp < cutoff)
            .order_by(MyTrade.entry_timestamp)
        )
        trades = list(result.scalars().all())

        for trade in trades:
            stats["checked"] += 1
            status = await get_order_status(trade.order_id) if trade.order_id else None
            clob_status = str((status or {}).get("status") or "").upper()
            try:
                matched = Decimal(str((status or {}).get("size_matched") or 0))
            except Exception:
                matched = Decimal("0")

            if clob_status == "LIVE" and trade.order_id:
                stats["cancel_requested"] += 1
                await cancel_order(trade.order_id, tag=f"repair#{trade.id}")
                status = await get_order_status(trade.order_id)
                clob_status = str((status or {}).get("status") or "").upper()
                try:
                    matched = Decimal(str((status or {}).get("size_matched") or 0))
                except Exception:
                    matched = Decimal("0")

            if matched > 0:
                trade.fill_status = "PARTIAL"
                trade.num_contracts = matched
                if trade.entry_price and Decimal(str(trade.entry_price)) > 0:
                    trade.size_usdc = matched * Decimal(str(trade.entry_price))
                stats["partial"] += 1
            elif status is None or clob_status in {"", "CANCELLED", "CANCELED", "EXPIRED"}:
                _mark_unfilled(trade)
                stats["marked_cancelled"] += 1
            else:
                stats["unchanged"] += 1

        if apply:
            await session.commit()
        else:
            await session.rollback()
    return stats


async def _past_resolution_count() -> int:
    async with async_session() as session:
        result = await session.execute(
            text(
                """
                SELECT count(*)
                FROM markets
                WHERE NOT resolved
                  AND resolution_time IS NOT NULL
                  AND resolution_time < now()
                """
            )
        )
        return int(result.scalar() or 0)


async def _run_resolution_reconcile(max_rounds: int) -> None:
    from src.execution.reconcile import reconcile_positions
    from src.tracking.resolution import check_resolutions

    threshold = int(settings.max_unresolved_past_resolution_markets)
    for round_idx in range(1, max_rounds + 1):
        backlog = await _past_resolution_count()
        logger.info(
            "Resolution backlog before round %d: %d (threshold=%d)",
            round_idx,
            backlog,
            threshold,
        )
        if backlog <= threshold:
            return
        resolved = await check_resolutions()
        recon = await reconcile_positions()
        logger.info("Round %d complete: resolved=%s reconcile=%s", round_idx, resolved, recon)
    backlog = await _past_resolution_count()
    logger.warning("Resolution backlog after %d rounds: %d", max_rounds, backlog)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="mutate the DB")
    parser.add_argument(
        "--run-resolution",
        action="store_true",
        help="run resolution/reconcile passes after non-economic row repair",
    )
    parser.add_argument("--max-rounds", type=int, default=3)
    parser.add_argument(
        "--cancel-stale-pending",
        action="store_true",
        help="refresh/cancel PENDING rows older than --pending-minutes",
    )
    parser.add_argument("--pending-minutes", type=int, default=2)
    args = parser.parse_args()

    await _print_audit("Before")
    repaired = await _repair_cancelled_failed(apply=args.apply)
    action = "repaired" if args.apply else "would repair"
    logger.info("%s %d CANCELLED/FAILED economic leakage rows", action, repaired)

    if args.cancel_stale_pending:
        pending_stats = await _repair_stale_pending(args.apply, args.pending_minutes)
        logger.info(
            "%s stale PENDING repair stats: %s",
            "applied" if args.apply else "would apply",
            pending_stats,
        )

    if args.run_resolution:
        if not args.apply:
            logger.info("--run-resolution ignored in dry-run mode")
        else:
            await _run_resolution_reconcile(args.max_rounds)

    await _print_audit("After" if args.apply else "Dry-run After (rolled back)")


if __name__ == "__main__":
    asyncio.run(main())
