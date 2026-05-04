"""Reconcile bot's DB against Polymarket wallet.

Polymarket is the source of truth. Updates PnL and resolution status
on existing trades. Imports new positions. Never deletes trades.
"""

import logging
from datetime import datetime, timezone
from decimal import Decimal
from collections.abc import Sequence

from sqlalchemy import select, and_, desc

from src.config import settings
from src.db import async_session
from src.models import MyTrade, WhalePosition
from src.polymarket.data_api import DataAPIClient
from src.indexer.market_ingester import ensure_market

logger = logging.getLogger(__name__)

ECONOMIC_FILL_STATUSES = {"FILLED", "PARTIAL", "PAPER"}
UNFILLED_FILL_STATUSES = {"PENDING", "FAILED", "CANCELLED"}


async def reconcile_positions() -> dict:
    """Sync DB with Polymarket wallet. Update-only — never deletes trades."""
    wallet = settings.polymarket_wallet_address
    if not wallet:
        logger.warning("RECONCILE: no wallet address configured, skipping")
        return {"skipped": True}

    client = DataAPIClient()
    try:
        try:
            active_positions = await client.get_positions(wallet)
        except Exception as e:
            logger.warning("RECONCILE: get_positions failed: %s", e)
            active_positions = []
        try:
            closed_positions = await client.get_closed_positions(wallet)
        except Exception as e:
            logger.warning("RECONCILE: get_closed_positions failed: %s", e)
            closed_positions = []
        try:
            all_activity = await client.get_all_activity(wallet)
        except Exception as e:
            logger.warning("RECONCILE: get_all_activity failed: %s", e)
            all_activity = []
    finally:
        try:
            await client.close()
        except Exception:
            pass

    # Build one entry per (condition_id, outcome) from all sources
    # Priority: positions > closed-positions > activity sells
    pm_data: dict[tuple[str, str], dict] = {}

    # 1. Active positions
    for p in (active_positions or []):
        if float(p.size or 0) <= 0:
            continue
        outcome = (p.outcome or "YES").upper()
        key = (p.condition_id, outcome)
        cur_price = float(p.cur_price or 0)
        avg_entry = float(p.avg_price or 0)
        size = float(p.size or 0)
        api_pnl = float(p.realized_pnl or 0)

        if cur_price >= 0.99 or cur_price <= 0.01:
            # Terminal price — effectively resolved even if not yet settled
            # on-chain. Polymarket's realized_pnl is 0 for active positions
            # (only filled on sell/settlement), so compute implied PnL from
            # the terminal cur_price ourselves: (cur_price - avg_entry) * size.
            # This captures the locked-in MTM correctly. When the market
            # actually settles, resolution.py overwrites with PM truth.
            implied_pnl = (cur_price - avg_entry) * size
            pm_data[key] = {
                "entry_price": avg_entry,
                "num_contracts": size,
                "cur_price": cur_price,
                "realized_pnl": implied_pnl,
                "resolved": True,
                "slug": p.slug or "",
            }
        else:
            pm_data[key] = {
                "entry_price": avg_entry,
                "num_contracts": size,
                "cur_price": cur_price,
                "realized_pnl": api_pnl,
                "resolved": False,
                "slug": p.slug or "",
            }

    # 2. Closed positions
    for p in (closed_positions or []):
        outcome = (p.outcome or "YES").upper()
        key = (p.condition_id, outcome)
        if key in pm_data:
            continue
        pnl = float(p.realized_pnl or 0)
        pm_data[key] = {
            "entry_price": float(p.avg_price or 0),
            "num_contracts": max(float(p.size or 0), 1),
            "cur_price": float(p.cur_price or 0),
            "realized_pnl": pnl,
            "resolved": True,
            "slug": p.slug or "",
        }

    # 3. Activity sells not already covered
    sell_agg: dict[tuple[str, str], dict] = {}
    buy_agg: dict[tuple[str, str], dict] = {}
    for a in (all_activity or []):
        if a.type != "TRADE" or not a.condition_id:
            continue
        outcome = (a.outcome or "YES").upper()
        key = (a.condition_id, outcome)
        price = float(a.price or 0)
        size = float(a.size or 0)
        if (a.side or "").upper() == "SELL":
            agg = sell_agg.setdefault(key, {"total_value": 0, "total_size": 0, "slug": a.slug or ""})
            agg["total_value"] += price * size
            agg["total_size"] += size
        elif (a.side or "").upper() == "BUY":
            agg = buy_agg.setdefault(key, {"total_value": 0, "total_size": 0})
            agg["total_value"] += price * size
            agg["total_size"] += size

    for key, sell in sell_agg.items():
        if key in pm_data:
            continue
        avg_sell = sell["total_value"] / sell["total_size"] if sell["total_size"] > 0 else 0
        buy = buy_agg.get(key, {"total_value": 0, "total_size": 0})
        avg_buy = buy["total_value"] / buy["total_size"] if buy["total_size"] > 0 else 0
        pnl = (avg_sell - avg_buy) * sell["total_size"]
        pm_data[key] = {
            "entry_price": avg_buy,
            "num_contracts": sell["total_size"],
            "cur_price": avg_sell,
            "realized_pnl": pnl,
            "resolved": True,
            "slug": sell["slug"],
        }

    logger.info("RECONCILE: %d positions from Polymarket", len(pm_data))

    imported = 0
    updated = 0
    matched = 0

    async with async_session() as session:
        for (cid, outcome), data in pm_data.items():
            # Reconcile the whole local position. Sync-loop entries are often split
            # across many nibbles, so mutating only the most recent row corrupts PnL.
            result = await session.execute(
                select(MyTrade)
                .where(and_(MyTrade.condition_id == cid, MyTrade.outcome == outcome))
                .order_by(desc(MyTrade.id))
            )
            trades = list(result.scalars().all())
            trade = trades[0] if trades else None

            if trade is None:
                # Import new position
                market = await ensure_market(cid, session, slug=data.get("slug"))
                if not market:
                    logger.warning("RECONCILE: skipping %s %s — market not found", cid[:12], outcome)
                    continue
                entry_price = data["entry_price"]
                num_contracts = int(data["num_contracts"])
                now = datetime.now(timezone.utc)

                trade = MyTrade(
                    signal_id=None,
                    condition_id=cid,
                    outcome=outcome,
                    entry_price=Decimal(str(round(entry_price, 6))),
                    size_usdc=Decimal(str(round(num_contracts * entry_price, 2))),
                    num_contracts=num_contracts,
                    fill_status="FILLED",
                    entry_timestamp=now,
                    resolved=data["resolved"],
                    source_wallets=[],
                    attribution={"source": "reconciled"},
                )
                if data["resolved"]:
                    pnl = data["realized_pnl"]
                    trade.exit_price = Decimal(str(round(data["cur_price"], 6)))
                    trade.pnl_usdc = Decimal(str(round(pnl, 2)))
                    trade.trade_outcome = "WIN" if pnl > 0 else ("LOSS" if pnl < 0 else "WIN")
                    trade.exit_timestamp = now

                session.add(trade)
                imported += 1
                continue

            # Trade exists — update PnL/resolution from Polymarket, preserve metadata.
            changed = False

            if data["resolved"]:
                changed = _apply_resolved_position_to_trades(
                    trades, data, datetime.now(timezone.utc)
                )
            else:
                changed = _apply_active_position_to_trades(trades, data)
                # If local nibbles diverge from PM truth and no reconciled
                # placeholder exists yet, import a new "reconciled" row for
                # the delta. Captures positions silently acquired by orders
                # the bot wrote off as CANCELLED (e.g. legacy DELAYED FAKs).
                economic = [t for t in trades if t.fill_status in ECONOMIC_FILL_STATUSES]
                has_reconciled = any(
                    (t.attribution or {}).get("source") == "reconciled"
                    for t in economic
                )
                local_sum = sum(int(t.num_contracts or 0) for t in economic)
                delta = int(data["num_contracts"]) - local_sum
                if not has_reconciled and delta >= 1:
                    market = await ensure_market(cid, session, slug=data.get("slug"))
                    if market:
                        avg = float(data["entry_price"])
                        # Attribute the delta to the watched whale that held the
                        # largest open position on this (cid, outcome). Without
                        # this, _trade_is_attributed_to skips the row and the
                        # whale's exposure is undercounted on the next cycle.
                        attributed_wallet = await _dominant_watched_whale(
                            session, cid, outcome
                        )
                        new_trade = MyTrade(
                            signal_id=None,
                            condition_id=cid,
                            outcome=outcome,
                            entry_price=Decimal(str(round(avg, 6))),
                            size_usdc=Decimal(str(round(delta * avg, 2))),
                            num_contracts=delta,
                            fill_status="FILLED",
                            entry_timestamp=datetime.now(timezone.utc),
                            resolved=False,
                            source_wallets=[attributed_wallet] if attributed_wallet else [],
                            attribution={
                                "source": "reconciled",
                                "delta_absorber": True,
                                "source_wallet": attributed_wallet or "",
                            },
                        )
                        session.add(new_trade)
                        logger.info(
                            "RECONCILE: imported delta %d contracts on %s %s (local=%d, pm=%d, attr=%s)",
                            delta, cid[:12], outcome, local_sum, int(data["num_contracts"]),
                            (attributed_wallet or "none")[:12],
                        )
                        imported += 1
                        changed = False  # already accounted via imported counter

            if changed:
                updated += 1
            else:
                matched += 1

        await session.commit()

    total_pnl = sum(d["realized_pnl"] for d in pm_data.values() if d["resolved"])
    summary = {
        "total": len(pm_data),
        "imported": imported,
        "updated": updated,
        "matched": matched,
        "total_realized_pnl": round(total_pnl, 2),
    }
    logger.info(
        "RECONCILE COMPLETE: %d positions | %d imported, %d updated, %d matched | PnL: $%.2f",
        len(pm_data), imported, updated, matched, total_pnl,
    )
    return summary


def _trade_weight(trade: MyTrade) -> float:
    contracts = float(trade.num_contracts or 0)
    if contracts > 0:
        return contracts
    return float(trade.size_usdc or 0)


def _apply_resolved_position_to_trades(
    trades: Sequence[MyTrade],
    data: dict,
    now: datetime,
) -> bool:
    """Apply aggregate Polymarket realized PnL across all economic local fills."""
    changed = False
    economic = [t for t in trades if t.fill_status in ECONOMIC_FILL_STATUSES]
    unfilled = [t for t in trades if t.fill_status in UNFILLED_FILL_STATUSES]

    total_weight = sum(_trade_weight(t) for t in economic)
    total_pnl = float(data["realized_pnl"])
    exit_price = Decimal(str(round(float(data["cur_price"]), 6)))

    for trade in economic:
        weight = _trade_weight(trade)
        share = (weight / total_weight) if total_weight > 0 else (1 / len(economic))
        pnl = Decimal(str(round(total_pnl * share, 2)))
        outcome = "WIN" if pnl > 0 else "LOSS"
        if (
            not trade.resolved
            or trade.pnl_usdc != pnl
            or trade.exit_price != exit_price
            or trade.trade_outcome != outcome
        ):
            trade.resolved = True
            trade.exit_price = exit_price
            trade.pnl_usdc = pnl
            trade.trade_outcome = outcome
            trade.exit_timestamp = trade.exit_timestamp or now
            changed = True

    for trade in unfilled:
        if (
            not trade.resolved
            or trade.pnl_usdc != Decimal("0")
            or trade.trade_outcome != "UNFILLED"
        ):
            trade.resolved = True
            trade.exit_price = None
            trade.pnl_usdc = Decimal("0")
            trade.trade_outcome = "UNFILLED"
            trade.exit_timestamp = trade.exit_timestamp or now
            changed = True

    return changed


def _apply_active_position_to_trades(trades: Sequence[MyTrade], data: dict) -> bool:
    """Reconcile local economic fills against PM truth for an open position.

    Local original-source rows record actual nibble-level fills; we keep their
    per-row entry prices intact. PM truth (size, avg) gets absorbed into a
    reconciled placeholder row for any delta between local sum and PM size, so
    nibbled positions whose local total drifts from PM still surface the gap.
    """
    changed = False
    economic = [t for t in trades if t.fill_status in ECONOMIC_FILL_STATUSES]
    reconciled = [t for t in economic if (t.attribution or {}).get("source") == "reconciled"]

    pm_shares = int(data["num_contracts"])
    pm_avg = float(data["entry_price"])
    local_sum = sum(int(t.num_contracts or 0) for t in economic)
    delta = pm_shares - local_sum

    if reconciled and abs(delta) >= 1:
        # Use the most recent reconciled row to absorb the local-vs-PM gap.
        absorber = reconciled[-1]
        target = max(0, int(absorber.num_contracts or 0) + delta)
        if abs(int(absorber.num_contracts or 0) - target) >= 1:
            absorber.num_contracts = target
            absorber.entry_price = Decimal(str(round(pm_avg, 6)))
            absorber.size_usdc = Decimal(str(round(target * pm_avg, 2)))
            changed = True

    for trade in trades:
        if trade.resolved and trade.fill_status in ECONOMIC_FILL_STATUSES:
            trade.resolved = False
            trade.trade_outcome = None
            trade.exit_price = None
            trade.pnl_usdc = None
            trade.exit_timestamp = None
            changed = True

    return changed


async def _dominant_watched_whale(session, condition_id: str, outcome: str) -> str | None:
    """Return the watched whale with the largest open position on (cid, outcome)."""
    watch = [w.lower() for w in (settings.watch_whales or [])]
    if not watch:
        return None
    result = await session.execute(
        select(WhalePosition.wallet_address, WhalePosition.num_contracts)
        .where(
            and_(
                WhalePosition.condition_id == condition_id,
                WhalePosition.outcome == outcome,
                WhalePosition.is_open == True,
                WhalePosition.wallet_address.in_(watch),
            )
        )
        .order_by(desc(WhalePosition.num_contracts))
        .limit(1)
    )
    row = result.first()
    return row[0] if row else None
