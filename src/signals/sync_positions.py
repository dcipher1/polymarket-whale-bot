"""Unified 30s copy-trader loop.

For each watch-whale's open weather position, every 30 seconds:
  1. Refresh status of any in-flight orders (capture recent fills).
  2. Cancel any remaining PENDING orders (active 30s expiry — no stale bids).
  3. Compute target_contracts = whale.size × position_size_fraction.
  4. Treat filled plus live pending requests as copied exposure.
  5. If exposure is already at target, hold; if it is over the 5% buffer, block more buys.
  6. If the remaining gap is below the CLOB minimum, skip as under_min_notional.
  7. Fetch book (bid + ask). Compute midpoint.
  8. Gate on midpoint vs whale_avg:
     - midpoint > whale_avg × 1.05  → SKIP slippage_above_5pct
     - midpoint < whale_avg × 0.80  → SKIP stale_below_20pct
  9. In-band pricing: submit a real FAK taker BUY if ask liquidity is available
     within whale_avg × 1.05.
 10. Never leave whale-copy BUYs resting on the book; FAK fills immediately or
     cancels the unfilled remainder."""

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import select, and_, func, update, tuple_
from sqlalchemy.dialects.postgresql import insert

from src.config import settings
from src.db import async_session
from src.execution.order_manager import (
    MIN_ORDER_PRICE,
    cancel_order,
    get_order_status,
    place_order,
    place_taker_buy,
    quantize_order_price,
)
from src.indexer.market_ingester import ensure_market
from src.indexer.wallet_ingester import ingest_wallet
from src.models import CopyDecision, Market, MarketToken, MyTrade, WhalePosition, WhaleTrade
from src.polymarket.clob_client import CLOBClient
from src.polymarket.data_api import DataAPIClient, Position
from src.polymarket.polynode_wallet_ws import WhaleTradeEvent, persist_whale_trade_event
from src.signals.weather_resolution import (
    MONTH_ABBR,
    effective_market_category,
    extract_weather_city,
    extract_weather_date,
    is_generic_weather_end_time,
    is_weather_market,
    parse_dt,
    weather_local_date_gate,
    weather_local_resolution_cutoff,
)

logger = logging.getLogger(__name__)

MIN_NOTIONAL_USDC = 1.05  # CLOB rejects BUY orders under $1; pad a cent for rounding.
MIN_ORDER_CONTRACTS = 5    # CLOB rejects orders smaller than 5 contracts.
FLOOR_FRAC = 0.80          # Skip if midpoint < whale_avg * FLOOR_FRAC (20% floor — thesis broken).
CEILING_FRAC = 1.05        # Allow bidding up to whale_avg * CEILING_FRAC (5% above — capture winners).
MAX_POSITION_FRAC_OF_WALLET = 0.10  # No single position > 10% of wallet USDC.
MAX_ORDER_USDC = 50.0      # Nibble cap — no single order larger than $50 notional.
LOW_PRICE_OBSERVATION = 0.10  # Log low average entries, but do not hard-skip real aggregate positions.
COPY_TARGET_BUFFER_FRAC = 1.05  # Allow only 5% above the watched whale's visible size.
TERMINAL_FILL = {"FILLED", "PARTIAL"}
STILL_OPEN = {"PENDING"}

_CITY_ABBR = {
    "New York City": "NYC", "New York": "NYC",
    "San Francisco": "SF", "Los Angeles": "LA",
    "Washington D.C.": "DC", "Washington DC": "DC",
}
def _parse_dt(value) -> datetime | None:
    return parse_dt(value)


def _candidate_resolution_time(market: Market | None, pos: Position) -> datetime | None:
    """Best known UTC resolution timestamp for this candidate market."""
    weather_cutoff = _weather_local_resolution_cutoff(market, pos)
    if weather_cutoff is not None:
        return weather_cutoff
    return _parse_dt(getattr(market, "resolution_time", None)) or _parse_dt(pos.end_date)


def _candidate_resolution_gate(
    market: Market | None,
    pos: Position,
    now_utc: datetime | None = None,
) -> tuple[bool, str, datetime | None]:
    """Return whether a candidate can still be bought based on its own timestamp."""
    weather_gate = _weather_local_date_gate(market, pos, now_utc)
    if weather_gate is not None:
        return weather_gate

    resolution_time = _candidate_resolution_time(market, pos)
    if _is_weather_market(market) and _is_generic_weather_end_time(resolution_time):
        return True, "weather_time_unknown", resolution_time
    if resolution_time is None:
        return False, "missing_resolution_time", None
    now = _parse_dt(now_utc) or datetime.now(timezone.utc)
    if resolution_time <= now:
        return False, "past_resolution_time", resolution_time
    return True, "before_resolution_time", resolution_time


def _mkt_tag(title: str | None, outcome: str, end_date: str | None) -> str:
    """Compact label like 'NYC 70-71 NO Apr23' for log lines."""
    t = title or ""
    city = "?"
    m = re.search(r"temperature in ([^?]+?) be ", t)
    if m:
        city = m.group(1).strip()
        city = _CITY_ABBR.get(city, city)
    temp = "?"
    m = re.search(r"(\d+-\d+|\d+)°[FC]", t)
    if m:
        temp = m.group(1)
    # Prefer end_date ISO (e.g. "2026-04-23") → "Apr23"
    date_tag = ""
    ed = (end_date or "")[:10]
    if len(ed) == 10:
        try:
            dt = datetime.fromisoformat(ed)
            date_tag = f"{MONTH_ABBR.get(dt.strftime('%B'), dt.strftime('%b'))}{dt.day}"
        except Exception:
            date_tag = ed
    return f"{city} {temp} {outcome} {date_tag}".strip()


def _weather_local_resolution_cutoff(market: Market | None, pos: Position) -> datetime | None:
    return weather_local_resolution_cutoff(market, pos)


def _weather_local_date_gate(
    market: Market | None,
    pos: Position,
    now_utc: datetime | None = None,
) -> tuple[bool, str, datetime | None] | None:
    return weather_local_date_gate(market, pos, now_utc)


def _is_weather_market(market: Market | None) -> bool:
    return is_weather_market(market)


def _effective_market_category(market: Market | None, fallback_title: str | None = None) -> str | None:
    return effective_market_category(market, fallback_title)


def _is_generic_weather_end_time(dt: datetime | None) -> bool:
    return is_generic_weather_end_time(dt)


def _extract_weather_city(title: str) -> str | None:
    return extract_weather_city(title)


def _extract_weather_date(title: str, pos: Position, market: Market | None):
    return extract_weather_date(title, pos, market)


def _whale_position_notional(whale_size: int, whale_avg: float) -> float:
    return max(0.0, whale_size * whale_avg)


async def _upsert_open_whale_position_from_api(
    session,
    addr: str,
    pos: Position,
) -> bool:
    """Mirror the Data API open-position snapshot into whale_positions."""
    outcome = (pos.outcome or "").upper()
    if outcome not in ("YES", "NO") or not pos.condition_id:
        return False

    contracts = Decimal(str(_f(pos.size)))
    avg_entry = Decimal(str(_f(pos.avg_price)))
    if contracts <= 0 or avg_entry <= 0:
        return False

    existing = await session.get(WhalePosition, (addr, pos.condition_id, outcome))
    if not existing or not existing.is_open:
        last_event_type = "OPEN"
    else:
        old_contracts = Decimal(str(existing.num_contracts or 0))
        if contracts > old_contracts:
            last_event_type = "ADD"
        elif contracts < old_contracts:
            last_event_type = "REDUCE"
        else:
            last_event_type = existing.last_event_type or "OPEN"

    total_size = contracts * avg_entry
    stmt = insert(WhalePosition).values(
        wallet_address=addr,
        condition_id=pos.condition_id,
        outcome=outcome,
        avg_entry_price=avg_entry,
        total_size_usdc=total_size,
        num_contracts=contracts,
        first_entry=existing.first_entry if existing else None,
        last_updated=datetime.now(timezone.utc),
        is_open=True,
        last_event_type=last_event_type,
        slug=pos.slug or (existing.slug if existing else None),
    ).on_conflict_do_update(
        constraint="whale_positions_pkey",
        set_={
            "avg_entry_price": avg_entry,
            "total_size_usdc": total_size,
            "num_contracts": contracts,
            "last_updated": datetime.now(timezone.utc),
            "is_open": True,
            "last_event_type": last_event_type,
            "slug": pos.slug or (existing.slug if existing else None),
        },
    )
    await session.execute(stmt)
    return True


async def _close_absent_whale_positions(
    session,
    addr: str,
    open_keys: set[tuple[str, str]],
) -> int:
    """Close DB whale positions absent from the latest Data API open snapshot."""
    stmt = update(WhalePosition).where(
        WhalePosition.wallet_address == addr,
        WhalePosition.is_open == True,
    )
    if open_keys:
        stmt = stmt.where(
            tuple_(WhalePosition.condition_id, WhalePosition.outcome).notin_(open_keys)
        )
    stmt = stmt.values(
        is_open=False,
        last_event_type="CLOSE",
        last_updated=datetime.now(timezone.utc),
    )
    result = await session.execute(stmt)
    return int(result.rowcount or 0)


async def _mark_whale_position_closed(
    session,
    addr: str,
    condition_id: str,
    outcome: str,
) -> None:
    await session.execute(
        update(WhalePosition)
        .where(
            WhalePosition.wallet_address == addr,
            WhalePosition.condition_id == condition_id,
            WhalePosition.outcome == outcome,
            WhalePosition.is_open == True,
        )
        .values(
            is_open=False,
            last_event_type="CLOSE",
            last_updated=datetime.now(timezone.utc),
        )
    )

# Suppress repeat logging of the same decision on the same (addr, cid, outcome).
_LAST_DECISION: dict[tuple[str, str, str], str] = {}
_LAST_HEALTH_ALERT_COUNT: int | None = None
_LAST_STALE_DECISION_RECORD: dict[tuple[str, str, str, str], datetime] = {}
STALE_DECISION_RECORD_INTERVAL = timedelta(hours=1)


@dataclass(frozen=True)
class CopyExposure:
    filled_contracts: int
    reserved_pending_contracts: int
    effective_contracts: int
    filled_usdc: float
    reserved_pending_usdc: float
    effective_usdc: float


@dataclass(frozen=True)
class CopyTarget:
    target_contracts: int
    max_target_contracts: int
    gap_contracts: int
    buffered_gap_contracts: int


def _copy_target(whale_size: float, effective_contracts: int) -> CopyTarget:
    target_contracts = int(round(whale_size * settings.position_size_fraction))
    max_target_contracts = int(whale_size * settings.position_size_fraction * COPY_TARGET_BUFFER_FRAC)
    max_target_contracts = max(target_contracts, max_target_contracts)
    return CopyTarget(
        target_contracts=target_contracts,
        max_target_contracts=max_target_contracts,
        gap_contracts=target_contracts - effective_contracts,
        buffered_gap_contracts=max_target_contracts - effective_contracts,
    )


def _note_decision(addr: str, cid: str, outcome: str, signature: str) -> bool:
    key = (addr, cid, outcome)
    prev = _LAST_DECISION.get(key)
    if prev == signature:
        return False
    _LAST_DECISION[key] = signature
    return True


def _should_record_copy_decision(
    decision_source: str,
    addr: str,
    cid: str,
    outcome: str,
    code: str,
    now: datetime | None = None,
) -> bool:
    """Throttle repetitive fallback decisions for stale historical positions."""
    if decision_source != "fallback_poll" or code not in {"stale_past_resolution", "past_resolution_time"}:
        return True
    now = now or datetime.now(timezone.utc)
    key = (addr, cid, outcome, code)
    last = _LAST_STALE_DECISION_RECORD.get(key)
    if last is not None and now - last < STALE_DECISION_RECORD_INTERVAL:
        return False
    _LAST_STALE_DECISION_RECORD[key] = now
    return True


def _f(val) -> float:
    try:
        return float(val or 0)
    except Exception:
        return 0.0


def _trade_is_attributed_to(trade: MyTrade, addr: str) -> bool:
    addr = addr.lower()
    source_wallets = [w.lower() for w in (trade.source_wallets or [])]
    if addr in source_wallets:
        return True
    attribution = trade.attribution or {}
    source = str(attribution.get("source_wallet") or attribution.get("wallet_address") or "").lower()
    return source == addr


def _trade_requested_contracts(trade: MyTrade) -> int:
    attribution = trade.attribution or {}
    requested = attribution.get("requested_contracts")
    if requested not in (None, ""):
        return int(_f(requested))
    return int(_f(trade.num_contracts))


def _trade_requested_usdc(trade: MyTrade) -> float:
    attribution = trade.attribution or {}
    requested = attribution.get("requested_size_usdc")
    if requested not in (None, ""):
        return _f(requested)
    price = _f(trade.entry_price)
    contracts = _trade_requested_contracts(trade)
    if price > 0 and contracts > 0:
        return contracts * price
    return _f(trade.size_usdc)


def _copy_exposure(trades: list[MyTrade]) -> CopyExposure:
    filled_contracts = 0
    reserved_pending_contracts = 0
    filled_usdc = 0.0
    reserved_pending_usdc = 0.0

    for trade in trades:
        if trade.fill_status in TERMINAL_FILL:
            filled_contracts += int(_f(trade.num_contracts))
            filled_usdc += _f(trade.size_usdc)
        elif trade.fill_status == "PENDING":
            reserved_pending_contracts += _trade_requested_contracts(trade)
            reserved_pending_usdc += _trade_requested_usdc(trade)

    return CopyExposure(
        filled_contracts=filled_contracts,
        reserved_pending_contracts=reserved_pending_contracts,
        effective_contracts=filled_contracts + reserved_pending_contracts,
        filled_usdc=filled_usdc,
        reserved_pending_usdc=reserved_pending_usdc,
        effective_usdc=filled_usdc + reserved_pending_usdc,
    )


def _jsonable(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return value


async def _find_whale_trade_by_event(session, event: WhaleTradeEvent) -> WhaleTrade | None:
    result = await session.execute(
        select(WhaleTrade).where(
            and_(
                WhaleTrade.wallet_address == event.wallet_address,
                WhaleTrade.tx_hash == event.tx_hash,
                WhaleTrade.token_id == event.token_id,
            )
        )
    )
    return result.scalar_one_or_none()


async def _record_copy_decision(
    session,
    *,
    wallet_address: str | None,
    condition_id: str | None,
    outcome: str | None,
    decision_code: str,
    decision_source: str,
    decision_reason: str | None = None,
    whale_trade_id: int | None = None,
    event_timestamp: datetime | None = None,
    detected_at: datetime | None = None,
    requested_contracts: int | float | Decimal | None = None,
    filled_contracts: int | float | Decimal | None = None,
    order_ids: list[str] | None = None,
    context: dict | None = None,
) -> CopyDecision:
    decided_at = datetime.now(timezone.utc)
    latency_seconds = None
    if detected_at is not None:
        latency_seconds = Decimal(str(round((decided_at - detected_at).total_seconds(), 3)))
    decision = CopyDecision(
        whale_trade_id=whale_trade_id,
        wallet_address=wallet_address.lower() if wallet_address else None,
        condition_id=condition_id,
        outcome=outcome,
        decision_code=decision_code,
        decision_reason=decision_reason or decision_code,
        decision_source=decision_source,
        event_timestamp=event_timestamp,
        detected_at=detected_at,
        decided_at=decided_at,
        latency_seconds=latency_seconds,
        requested_contracts=Decimal(str(requested_contracts)) if requested_contracts is not None else None,
        filled_contracts=Decimal(str(filled_contracts)) if filled_contracts is not None else None,
        order_ids=order_ids or None,
        context=_jsonable(context or {}),
    )
    session.add(decision)
    return decision


async def _get_book_prices(clob: CLOBClient, token_id: str) -> tuple[float | None, float | None, float | None]:
    """Return (best_bid, best_ask, midpoint). Any component may be None if unavailable."""
    bid = None
    ask = None
    try:
        book = await clob.get_orderbook(token_id)
        if book:
            if book.get("bids"):
                bid = max(float(b["price"]) for b in book["bids"])
            if book.get("asks"):
                ask = min(float(a["price"]) for a in book["asks"])
    except Exception as e:
        logger.debug("book fetch failed for %s: %s", token_id[:16], e)

    if bid is not None and ask is not None:
        return bid, ask, (bid + ask) / 2

    # Fall back to midpoint API when one side of the book is missing.
    try:
        mid = await clob.get_midpoint(token_id)
        mid = float(mid) if mid is not None else None
    except Exception:
        mid = None
    return bid, ask, mid


async def _refresh_inflight(session, trade: MyTrade) -> None:
    """Poll CLOB for a PENDING trade. Update status + num_contracts based on fills."""
    if not trade.order_id:
        return
    attribution = dict(trade.attribution or {})
    if "requested_contracts" not in attribution and trade.num_contracts is not None:
        attribution["requested_contracts"] = int(trade.num_contracts or 0)
    if "requested_size_usdc" not in attribution and trade.size_usdc is not None:
        attribution["requested_size_usdc"] = str(trade.size_usdc)
    trade.attribution = attribution

    status = await get_order_status(trade.order_id)
    if not status:
        _mark_cancelled_zero_fill(trade)
        logger.info("Trade %d: CLOB has no record of %s — marking CANCELLED",
                    trade.id, trade.order_id[:16])
        return
    clob_status = str(status.get("status", "")).upper()
    matched = int(_f(status.get("size_matched")))
    requested = int(trade.num_contracts or 0)

    if clob_status == "MATCHED" and matched > 0:
        trade.fill_status = "FILLED"
        trade.num_contracts = matched
        if trade.entry_price and float(trade.entry_price) > 0:
            trade.size_usdc = Decimal(str(round(matched * float(trade.entry_price), 2)))
        logger.info("Trade %d: FILLED — %d contracts", trade.id, matched)
    elif clob_status in ("CANCELLED", "CANCELED", "EXPIRED"):
        if matched > 0:
            trade.fill_status = "PARTIAL"
            trade.num_contracts = matched
            if trade.entry_price and float(trade.entry_price) > 0:
                trade.size_usdc = Decimal(str(round(matched * float(trade.entry_price), 2)))
            logger.info("Trade %d: PARTIAL — %d of %s matched before %s",
                        trade.id, matched, requested or "?", clob_status)
        else:
            _mark_cancelled_zero_fill(trade)
            logger.info("Trade %d: CANCELLED (no fills)", trade.id)
    elif clob_status == "LIVE":
        if matched > 0:
            attribution = dict(trade.attribution or {})
            attribution["live_matched_contracts"] = matched
            trade.attribution = attribution
            logger.info(
                "Trade %d: LIVE with %d matched; keeping requested size reserved",
                trade.id,
                matched,
            )


def _mark_cancelled_zero_fill(trade: MyTrade) -> None:
    """Mark an unfilled order attempt as non-economic while preserving request metadata."""
    attribution = dict(trade.attribution or {})
    if "requested_contracts" not in attribution and trade.num_contracts is not None:
        attribution["requested_contracts"] = int(trade.num_contracts or 0)
    if "requested_size_usdc" not in attribution and trade.size_usdc is not None:
        attribution["requested_size_usdc"] = str(trade.size_usdc)
    trade.attribution = attribution
    trade.fill_status = "CANCELLED"
    trade.num_contracts = 0
    trade.size_usdc = Decimal("0")


async def _cancel_inflight(session, trade: MyTrade, reason: str, tag: str | None = None) -> None:
    if not trade.order_id or trade.fill_status != "PENDING":
        return
    label = tag or (trade.attribution or {}).get("mkt_tag") or f"trade#{trade.id}"
    logger.info("Cancelling %s (%s)", label, reason)
    ok = await cancel_order(trade.order_id, tag=label)
    if not ok:
        logger.warning("Cancel returned False: %s", label)
    # Re-poll to capture any last-moment fills before we remove it from in-flight.
    await _refresh_inflight(session, trade)


async def _evaluate_position(
    session,
    addr: str,
    pos: Position,
    clob: CLOBClient,
    wallet_usdc: float,
    allow_new_buys: bool = True,
    decision_source: str = "fallback_poll",
    whale_trade_id: int | None = None,
    event_timestamp: datetime | None = None,
    detected_at: datetime | None = None,
) -> str:
    """Handle one whale open position end-to-end for this cycle.

    Returns a short decision code (e.g. 'BUY', 'HOLD', 'slippage_above', 'tiny_whale')
    so the caller can tally a per-cycle summary.
    """
    outcome = (pos.outcome or "").upper()
    decision_context = {
        "wallet_usdc": round(wallet_usdc, 6),
        "allow_new_buys": allow_new_buys,
        "position_size_fraction": settings.position_size_fraction,
        "max_order_usdc": MAX_ORDER_USDC,
    }

    async def finish(
        code: str,
        reason: str | None = None,
        *,
        requested_contracts: int | float | Decimal | None = None,
        filled_contracts: int | float | Decimal | None = None,
        order_ids: list[str] | None = None,
        **extra,
    ) -> str:
        context = dict(decision_context)
        context.update(extra)
        if _should_record_copy_decision(decision_source, addr, pos.condition_id, outcome, code):
            await _record_copy_decision(
                session,
                wallet_address=addr,
                condition_id=pos.condition_id,
                outcome=outcome if outcome in {"YES", "NO"} else None,
                decision_code=code,
                decision_reason=reason,
                decision_source=decision_source,
                whale_trade_id=whale_trade_id,
                event_timestamp=event_timestamp,
                detected_at=detected_at,
                requested_contracts=requested_contracts,
                filled_contracts=filled_contracts,
                order_ids=order_ids,
                context=context,
            )
        return code

    if outcome not in ("YES", "NO") or not pos.condition_id:
        return await finish("invalid", "invalid_outcome_or_condition")
    whale_size = int(_f(pos.size))
    whale_avg = _f(pos.avg_price)
    decision_context.update(
        {
            "whale_size": whale_size,
            "whale_avg_price": round(whale_avg, 6),
        }
    )
    if whale_size <= 0 or whale_avg <= 0:
        return await finish("whale_empty")

    # Only weather.
    market = await ensure_market(pos.condition_id, session, slug=pos.slug or None)
    if not market:
        logger.debug("SYNC: %s | %s | SKIP no_market_metadata", addr[:10], pos.condition_id[:10])
        return await finish("no_market_metadata")
    category = _effective_market_category(market, pos.title)
    decision_context.update(
        {
            "market_title": market.question or pos.title,
            "market_slug": market.slug or pos.slug,
            "category": category,
            "resolution_time": market.resolution_time,
        }
    )
    if category != "weather":
        logger.debug("SYNC: %s | %s | SKIP non_weather:%s",
                     addr[:10], pos.condition_id[:10], category or "None")
        return await finish("non_weather", f"non_weather:{category or 'None'}")

    # Compute the readable tag early so cancel logs can use it too.
    mkt_tag = _mkt_tag(market.question or pos.title, outcome, pos.end_date)

    can_buy_by_time, time_code, resolution_time = _candidate_resolution_gate(market, pos)
    decision_context.update(
        {
            "candidate_resolution_code": time_code,
            "candidate_resolution_time": resolution_time,
        }
    )
    if not can_buy_by_time:
        if time_code == "stale_past_resolution":
            await _mark_whale_position_closed(session, addr, pos.condition_id, outcome)
        if decision_source == "fallback_poll":
            logger.debug(
                "SYNC: %s | fallback quiet %s%s",
                mkt_tag,
                time_code,
                f":{resolution_time.isoformat()}" if resolution_time else "",
            )
            return await finish(time_code)
        signature = f"SKIP:{time_code}:{resolution_time.isoformat() if resolution_time else 'unknown'}"
        if _note_decision(addr, pos.condition_id, outcome, signature):
            logger.info(
                "SYNC: %s | SKIP %s%s",
                mkt_tag,
                time_code,
                f":{resolution_time.isoformat()}" if resolution_time else "",
        )
        return await finish(time_code)

    await _upsert_open_whale_position_from_api(session, addr, pos)

    whale_notional = _whale_position_notional(whale_size, whale_avg)
    decision_context["whale_position_notional_usdc"] = round(whale_notional, 2)
    if whale_avg < LOW_PRICE_OBSERVATION and _note_decision(
        addr, pos.condition_id, outcome, f"OBSERVE:low_avg_price:{whale_avg:.3f}:{whale_notional:.2f}"
    ):
        logger.info(
            "SYNC: %s | low_avg_price %.3f but aggregate whale position $%.2f; evaluating against CLOB min",
            mkt_tag,
            whale_avg,
            whale_notional,
        )

    # Pull our trades on this (cid, outcome).
    our_result = await session.execute(
        select(MyTrade).where(
            and_(MyTrade.condition_id == pos.condition_id, MyTrade.outcome == outcome)
        )
    )
    our_trades: list[MyTrade] = list(our_result.scalars().all())
    source_trades = [t for t in our_trades if _trade_is_attributed_to(t, addr)]

    # Refresh fill status on any in-flight orders so we don't cancel what just filled.
    in_flight = [t for t in source_trades if t.fill_status in STILL_OPEN]
    for t in in_flight:
        await _refresh_inflight(session, t)

    # Cancel any still-PENDING orders — active 30s expiry, no stale bids.
    in_flight = [t for t in source_trades if t.fill_status == "PENDING"]
    for t in in_flight:
        await _cancel_inflight(session, t, reason="cycle_expiry", tag=mkt_tag)

    exposure = _copy_exposure(source_trades)
    filled_contracts = exposure.filled_contracts
    reserved_contracts = exposure.reserved_pending_contracts
    effective_contracts = exposure.effective_contracts
    copy_target = _copy_target(whale_size, effective_contracts)
    target_contracts = copy_target.target_contracts
    max_target_contracts = copy_target.max_target_contracts
    gap_contracts = copy_target.gap_contracts
    buffered_gap_contracts = copy_target.buffered_gap_contracts
    decision_context.update(
        {
            "filled_contracts": filled_contracts,
            "reserved_pending_contracts": reserved_contracts,
            "effective_copied_contracts": effective_contracts,
            "target_contracts": target_contracts,
            "max_target_contracts": max_target_contracts,
            "gap_contracts": gap_contracts,
            "buffered_gap_contracts": buffered_gap_contracts,
            "current_exposure_usdc": round(exposure.effective_usdc, 2),
        }
    )

    if buffered_gap_contracts <= 0:
        if _note_decision(
            addr,
            pos.condition_id,
            outcome,
            f"SKIP:copy_target_buffer:{effective_contracts}:{max_target_contracts}",
        ):
            logger.info(
                "SYNC: %s | filled %d reserved %d effective %d >= buffered max %d → SKIP copy_target_buffer",
                mkt_tag,
                filled_contracts,
                reserved_contracts,
                effective_contracts,
                max_target_contracts,
            )
        return await finish("copy_target_buffer", filled_contracts=filled_contracts)

    if gap_contracts <= 0:
        if _note_decision(
            addr,
            pos.condition_id,
            outcome,
            f"HOLD:target_met:{effective_contracts}:{target_contracts}:{max_target_contracts}",
        ):
            logger.info(
                "SYNC: %s | filled %d reserved %d effective %d >= target %d (buffer max %d) → HOLD",
                mkt_tag,
                filled_contracts,
                reserved_contracts,
                effective_contracts,
                target_contracts,
                max_target_contracts,
            )
        return await finish("HOLD", "target_met", filled_contracts=filled_contracts)

    max_allowed_notional = gap_contracts * whale_avg * CEILING_FRAC
    decision_context["max_allowed_notional"] = round(max_allowed_notional, 4)
    if max_allowed_notional < MIN_NOTIONAL_USDC:
        if _note_decision(
            addr,
            pos.condition_id,
            outcome,
            f"SKIP:under_min_notional:{gap_contracts}:{filled_contracts}:{reserved_contracts}:{target_contracts}:{max_target_contracts}",
        ):
            logger.info(
                "SYNC: %s | gap %d contracts × max %.3f = $%.2f < $%.2f CLOB min | filled %d reserved %d effective %d / target %d max %d → SKIP under_min_notional",
                mkt_tag,
                gap_contracts,
                whale_avg * CEILING_FRAC,
                max_allowed_notional,
                MIN_NOTIONAL_USDC,
                filled_contracts,
                reserved_contracts,
                effective_contracts,
                target_contracts,
                max_target_contracts,
            )
        return await finish("under_min", "under_min_notional", filled_contracts=filled_contracts)

    if not allow_new_buys:
        if _note_decision(addr, pos.condition_id, outcome, "SKIP:buy_health_halt"):
            logger.warning(
                "SYNC: %s | gap %d contracts | SKIP buy_health_halt",
                mkt_tag, gap_contracts,
            )
        return await finish("buy_health_halt", filled_contracts=filled_contracts)

    # Token lookup.
    tok = await session.execute(
        select(MarketToken).where(
            and_(MarketToken.condition_id == pos.condition_id, MarketToken.outcome == outcome)
        )
    )
    token = tok.scalar_one_or_none()
    if not token:
        if _note_decision(addr, pos.condition_id, outcome, "SKIP:no_token"):
            logger.info("SYNC: %s | gap %d contracts | SKIP no_token", mkt_tag, gap_contracts)
        return await finish("no_token", filled_contracts=filled_contracts)
    decision_context["token_id"] = token.token_id

    # Book fetch — need midpoint.
    bid, ask, mid = await _get_book_prices(clob, token.token_id)
    decision_context.update({"best_bid": bid, "best_ask": ask, "midpoint": mid})
    if mid is None:
        if _note_decision(addr, pos.condition_id, outcome, "SKIP:no_book"):
            logger.info("SYNC: %s | gap %d contracts | SKIP no_book", mkt_tag, gap_contracts)
        return await finish("no_book", filled_contracts=filled_contracts)

    # Price band anchored to whale_avg — 5% ceiling above, 20% floor below.
    max_bid = whale_avg * CEILING_FRAC
    min_bid = whale_avg * FLOOR_FRAC
    decision_context.update({"max_bid": round(max_bid, 6), "min_bid": round(min_bid, 6)})

    if mid < min_bid:
        if _note_decision(addr, pos.condition_id, outcome, f"SKIP:stale_below_20pct:{mid:.3f}"):
            logger.info(
                "SYNC: %s | gap %d contracts | mid %.3f < %.3f × 0.80 = %.3f → SKIP stale_below_20pct",
                mkt_tag, gap_contracts, mid, whale_avg, min_bid,
            )
        return await finish("stale_below", "stale_below_20pct", filled_contracts=filled_contracts)
    if mid > max_bid:
        if _note_decision(addr, pos.condition_id, outcome, f"SKIP:slippage_above_5pct:{mid:.3f}"):
            logger.info(
                "SYNC: %s | gap %d contracts | mid %.3f > %.3f × 1.05 = %.3f → SKIP slippage_above_5pct",
                mkt_tag, gap_contracts, mid, whale_avg, max_bid,
            )
        return await finish("slippage_above", "slippage_above_5pct", filled_contracts=filled_contracts)

    # In band: use a real FAK taker. If no ask exists inside the cap, do not
    # leave a passive whale-copy order resting on the book.
    if ask is None or ask <= 0:
        if _note_decision(addr, pos.condition_id, outcome, "SKIP:no_valid_price:no_ask"):
            logger.info("SYNC: %s | gap %d contracts | SKIP no_valid_price:no_ask", mkt_tag, gap_contracts)
        return await finish("NO_VALID_PRICE", "no_ask", filled_contracts=filled_contracts)
    if ask > max_bid:
        if _note_decision(addr, pos.condition_id, outcome, f"SKIP:slippage_above_ask:{ask:.3f}"):
            logger.info(
                "SYNC: %s | gap %d contracts | ask %.3f > %.3f × 1.05 = %.3f → SKIP slippage_above_ask",
                mkt_tag, gap_contracts, ask, whale_avg, max_bid,
            )
        return await finish("slippage_above", "ask_above_slippage_cap", filled_contracts=filled_contracts)

    bid_price = ask
    price_reason = "FAK_TAKER"
    order_price = quantize_order_price(bid_price, "BUY")
    max_price = quantize_order_price(max_bid, "BUY")
    decision_context.update(
        {
            "bid_price": round(bid_price, 6),
            "order_price": round(order_price, 6),
            "max_price": round(max_price, 6),
            "price_reason": price_reason,
            "order_type": "FAK",
        }
    )
    if order_price < MIN_ORDER_PRICE or max_price < MIN_ORDER_PRICE:
        if _note_decision(addr, pos.condition_id, outcome, f"SKIP:no_valid_price:{bid_price:.6f}"):
            logger.info(
                "SYNC: %s | taker price %.6f rounds below CLOB min tick %.3f → SKIP no_valid_price",
                mkt_tag, bid_price, MIN_ORDER_PRICE,
            )
        return await finish("NO_VALID_PRICE", "price_below_tick", requested_contracts=gap_contracts, filled_contracts=filled_contracts)

    # Per-market concentration cap — size down the order if placing it would push
    # this position beyond MAX_POSITION_FRAC_OF_WALLET of wallet USDC.
    contracts = min(gap_contracts, buffered_gap_contracts)
    position_cap_usdc = wallet_usdc * MAX_POSITION_FRAC_OF_WALLET
    current_exposure_usdc = exposure.effective_usdc
    allowed_additional_usdc = max(0.0, position_cap_usdc - current_exposure_usdc)
    gap_usdc = contracts * order_price
    if gap_usdc > allowed_additional_usdc:
        capped_contracts = int(allowed_additional_usdc / order_price)
        if capped_contracts * order_price < MIN_NOTIONAL_USDC:
            if _note_decision(addr, pos.condition_id, outcome, "SKIP:position_cap"):
                logger.info(
                    "SYNC: %s | gap %d @ $%.2f = $%.2f | at/over %.0f%% cap $%.2f (current $%.2f) → SKIP position_cap",
                    mkt_tag, contracts, order_price, gap_usdc,
                    MAX_POSITION_FRAC_OF_WALLET * 100, position_cap_usdc, current_exposure_usdc,
                )
            return await finish("position_cap", filled_contracts=filled_contracts)
        logger.info(
            "SYNC: %s | sizing down %d → %d contracts (position cap $%.2f)",
            mkt_tag, contracts, capped_contracts, position_cap_usdc,
        )
        contracts = capped_contracts
    decision_context.update(
        {
            "position_cap_usdc": round(position_cap_usdc, 2),
            "allowed_additional_usdc": round(allowed_additional_usdc, 2),
        }
    )

    # Per-order nibble cap: never place > $50 notional in one order.
    per_order_cap_contracts = int(MAX_ORDER_USDC / order_price)
    if contracts > per_order_cap_contracts:
        logger.info(
            "SYNC: %s | nibbling: gap %d → %d contracts ($%.0f cap @ %.4f)",
            mkt_tag, contracts, per_order_cap_contracts, MAX_ORDER_USDC, order_price,
        )
        contracts = per_order_cap_contracts
    decision_context["final_contracts"] = contracts

    # CLOB has a 5-contract minimum per order. Skip if our final size is below that.
    if contracts < MIN_ORDER_CONTRACTS:
        if _note_decision(addr, pos.condition_id, outcome, f"SKIP:under_min_contracts:{contracts}"):
            logger.info(
                "SYNC: %s | order size %d < CLOB min %d → SKIP",
                mkt_tag, contracts, MIN_ORDER_CONTRACTS,
            )
        return await finish("under_min", "under_min_contracts", requested_contracts=contracts, filled_contracts=filled_contracts)
    if contracts * order_price < MIN_NOTIONAL_USDC:
        if _note_decision(addr, pos.condition_id, outcome, "SKIP:under_min_notional"):
            logger.info("SYNC: %s | order notional under CLOB $1 min → SKIP", mkt_tag)
        return await finish("under_min", "under_min_notional", requested_contracts=contracts, filled_contracts=filled_contracts)

    taker_amount_usdc = round(min(contracts * order_price, MAX_ORDER_USDC, allowed_additional_usdc), 2)
    decision_context["taker_amount_usdc"] = taker_amount_usdc
    if taker_amount_usdc < MIN_NOTIONAL_USDC:
        if _note_decision(addr, pos.condition_id, outcome, "SKIP:under_min_notional"):
            logger.info("SYNC: %s | FAK amount $%.2f under CLOB $1 min → SKIP", mkt_tag, taker_amount_usdc)
        return await finish("under_min", "under_min_notional", requested_contracts=contracts, filled_contracts=filled_contracts)

    if settings.live_execution_enabled:
        taker_result = await place_taker_buy(
            token_id=token.token_id,
            amount_usdc=taker_amount_usdc,
            max_price=max_price,
            tag=mkt_tag,
        )
        mode = "LIVE"
        order_id = taker_result.order_id
        immediate_fill_contracts = taker_result.filled_contracts
        avg_fill_price = taker_result.avg_fill_price or order_price
        decision_context.update(
            {
                "fak_status": taker_result.status,
                "fak_error": taker_result.error,
                "fak_response": taker_result.raw_response,
                "fak_order_status": taker_result.raw_status,
                "fak_filled_contracts": immediate_fill_contracts,
            }
        )
        if taker_result.error and not taker_result.accepted:
            if taker_result.error == "place_failed":
                if _note_decision(addr, pos.condition_id, outcome, "SKIP:place_failed"):
                    logger.warning("SYNC: %s | gap %d contracts | FAK placement failed", mkt_tag, contracts)
                return await finish("place_failed", requested_contracts=contracts, filled_contracts=0)
            if _note_decision(addr, pos.condition_id, outcome, f"SKIP:FAK_REJECTED:{taker_result.error}"):
                logger.warning("SYNC: %s | FAK rejected: %s", mkt_tag, taker_result.error)
            return await finish("FAK_REJECTED", taker_result.error, requested_contracts=contracts, filled_contracts=0)
    else:
        order_id = f"paper_fak_{int(datetime.now(timezone.utc).timestamp() * 1000)}_{pos.condition_id[:8]}"
        mode = "PAPER"
        immediate_fill_contracts = contracts
        avg_fill_price = order_price

    if not order_id:
        if _note_decision(addr, pos.condition_id, outcome, "SKIP:place_failed"):
            logger.warning("SYNC: %s | gap %d contracts | FAK placement failed", mkt_tag, contracts)
        return await finish("place_failed", requested_contracts=contracts, filled_contracts=0)

    fill_status = "FILLED"
    if immediate_fill_contracts <= 0:
        fill_status = "CANCELLED"
        actual_contracts = 0
        size_usdc = 0.0
    else:
        actual_contracts = immediate_fill_contracts
        fill_status = "FILLED" if immediate_fill_contracts >= contracts else "PARTIAL"
        size_usdc = round(actual_contracts * avg_fill_price, 2)

    trade = MyTrade(
        condition_id=pos.condition_id,
        outcome=outcome,
        entry_price=Decimal(str(round(avg_fill_price, 6))),
        size_usdc=Decimal(str(size_usdc)),
        num_contracts=actual_contracts,
        order_id=order_id,
        fill_status=fill_status,
        entry_timestamp=datetime.now(timezone.utc),
        source_wallets=[addr],
        attribution={
            "mkt_tag": mkt_tag,
            "whale_avg_price": str(round(whale_avg, 6)),
            "whale_position_notional_usdc": str(round(whale_notional, 2)),
            "low_avg_price": whale_avg < LOW_PRICE_OBSERVATION,
            "bid_price": str(round(bid_price, 6)),
            "order_price": str(round(order_price, 6)),
            "max_price": str(round(max_price, 6)),
            "midpoint": str(round(mid, 6)),
            "price_reason": price_reason,
            "order_type": "FAK",
            "whale_size": whale_size,
            "target_contracts": target_contracts,
            "max_target_contracts": max_target_contracts,
            "filled_contracts": filled_contracts,
            "reserved_pending_contracts": reserved_contracts,
            "effective_copied_contracts": effective_contracts,
            "requested_contracts": contracts,
            "requested_size_usdc": str(taker_amount_usdc),
            "immediate_fill_contracts": immediate_fill_contracts,
            "size_fraction": settings.position_size_fraction,
            "category": "weather",
            "execution_mode": mode,
            "source": "sync_positions",
        },
    )
    session.add(trade)
    await session.flush()
    _LAST_DECISION.pop((addr, pos.condition_id, outcome), None)
    logger.info(
        "SYNC: %s | whale_avg %.3f, mid %.3f | filled %d reserved %d effective %d / target %d max %d | gap %d → FAK BUY $%.2f max %.4f filled %d/%d [%s]",
        mkt_tag, whale_avg, mid, filled_contracts, reserved_contracts, effective_contracts, target_contracts, max_target_contracts, gap_contracts,
        taker_amount_usdc, max_price, immediate_fill_contracts, contracts, mode,
    )
    if immediate_fill_contracts <= 0:
        return await finish(
            "FAK_NO_FILL",
            "fak_no_immediate_fill",
            requested_contracts=contracts,
            filled_contracts=0,
            order_ids=[order_id],
            my_trade_id=trade.id,
            order_notional_usdc=taker_amount_usdc,
            execution_mode=mode,
        )
    return await finish(
        "BUY",
        "fak_filled" if fill_status == "FILLED" else "fak_partial",
        requested_contracts=contracts,
        filled_contracts=immediate_fill_contracts,
        order_ids=[order_id],
        my_trade_id=trade.id,
        order_notional_usdc=size_usdc,
        execution_mode=mode,
    )


async def _resolution_backlog_count() -> int:
    async with async_session() as session:
        result = await session.execute(
            select(func.count(Market.condition_id)).where(
                and_(
                    Market.resolved == False,
                    Market.resolution_time.isnot(None),
                    Market.resolution_time < datetime.now(timezone.utc),
                )
            )
        )
        return int(result.scalar() or 0)


async def _allow_new_buys() -> bool:
    """Raw DB resolution backlog is audit-only; live buys use per-candidate gates."""
    return True


async def sync_whale_positions_once() -> int:
    """One cycle of the copy-trader loop."""
    orders_placed = 0
    data_client = DataAPIClient()
    clob = CLOBClient()
    allow_new_buys = await _allow_new_buys()

    # Fetch wallet USDC once per cycle for the per-market concentration cap.
    try:
        from src.polymarket.clob_auth import get_auth_client
        wallet_usdc = await get_auth_client().get_balance()
    except Exception as e:
        if settings.live_execution_enabled:
            wallet_usdc = 0.0
            allow_new_buys = False
            logger.warning("SYNC: balance fetch failed in LIVE mode; new buys disabled: %s", e)
        else:
            # Paper mode can still use static capital for sizing diagnostics.
            wallet_usdc = float(settings.starting_capital)
            logger.warning("SYNC: balance fetch failed (using starting_capital $%.0f as paper fallback): %s",
                           wallet_usdc, e)

    try:
        for addr in settings.watch_whales:
            addr = addr.lower()
            try:
                new = await ingest_wallet(addr, client=data_client)
                if new > 0:
                    logger.info("SYNC: %s +%d new whale trades", addr[:10], new)
            except Exception as e:
                logger.warning("SYNC: ingest_wallet failed for %s: %s", addr[:10], e)

            try:
                positions = await data_client.get_positions(addr)
            except Exception as e:
                logger.warning("SYNC: get_positions failed for %s: %s", addr[:10], e)
                continue

            logger.info("SYNC: %s polling — %d open positions", addr[:10], len(positions))
            open_position_keys = {
                (p.condition_id, (p.outcome or "").upper())
                for p in positions
                if p.condition_id and (p.outcome or "").upper() in {"YES", "NO"}
            }
            counts: dict[str, int] = {}
            for pos in positions:
                async with async_session() as session:
                    try:
                        code = await _evaluate_position(
                            session, addr, pos, clob, wallet_usdc,
                            allow_new_buys=allow_new_buys,
                        ) or "unknown"
                        counts[code] = counts.get(code, 0) + 1
                        if code == "BUY":
                            orders_placed += 1
                        await session.commit()
                    except Exception as e:
                        counts["error"] = counts.get("error", 0) + 1
                        logger.error(
                            "SYNC: evaluate failed for %s: %s",
                            pos.condition_id[:10] if pos.condition_id else "?", e,
                        )
                        await session.rollback()
            async with async_session() as session:
                try:
                    closed = await _close_absent_whale_positions(session, addr, open_position_keys)
                    await session.commit()
                    if closed:
                        logger.info("SYNC: %s closed %d absent whale position rows", addr[:10], closed)
                except Exception as e:
                    logger.warning("SYNC: close absent whale positions failed for %s: %s", addr[:10], e)
                    await session.rollback()
            summary_parts = [f"{k}={v}" for k, v in sorted(counts.items(), key=lambda kv: -kv[1])]
            logger.info(
                "SYNC COMPLETE %s: %d scanned | %s",
                addr[:10], sum(counts.values()), " ".join(summary_parts),
            )
    finally:
        await data_client.close()
        try:
            await clob.close()
        except Exception:
            pass
    return orders_placed


async def _get_wallet_usdc_for_sizing() -> float:
    try:
        from src.polymarket.clob_auth import get_auth_client
        return await get_auth_client().get_balance()
    except Exception as e:
        if settings.live_execution_enabled:
            logger.warning("SYNC: balance fetch failed in LIVE mode; websocket BUY copy disabled: %s", e)
            return 0.0
        wallet_usdc = float(settings.starting_capital)
        logger.warning(
            "SYNC: balance fetch failed (using starting_capital $%.0f as paper fallback): %s",
            wallet_usdc,
            e,
        )
        return wallet_usdc


async def handle_polynode_wallet_event(event: WhaleTradeEvent) -> str:
    """Persist and handle one normalized PolyNode wallet event.

    BUY events reuse the normal copy evaluator. SELL events are persisted as
    whale activity but deliberately ignored for execution.
    """
    async with async_session() as session:
        result = await persist_whale_trade_event(session, event)
        whale_trade = await _find_whale_trade_by_event(session, event)
        whale_trade_id = whale_trade.id if whale_trade else None
        if result == "backlog":
            await _record_copy_decision(
                session,
                wallet_address=event.wallet_address,
                condition_id=event.condition_id,
                outcome=event.outcome,
                decision_code="backlog",
                decision_reason="market_or_token_hydration_backlog",
                decision_source="websocket",
                whale_trade_id=whale_trade_id,
                event_timestamp=event.timestamp,
                detected_at=event.provider_timestamp,
                requested_contracts=event.contracts,
                context={"event_type": event.event_type, "tx_hash": event.tx_hash},
            )
            await session.commit()
            logger.warning(
                "POLYNODE: event backlogged pending market hydration: %s %s %s",
                event.wallet_address[:10],
                event.condition_id[:10],
                event.token_id[:16],
            )
            return "backlog"
        inserted = result == "inserted" or result is True
        if not inserted:
            await _record_copy_decision(
                session,
                wallet_address=event.wallet_address,
                condition_id=event.condition_id,
                outcome=event.outcome,
                decision_code="duplicate",
                decision_reason="duplicate_whale_trade_event",
                decision_source="websocket",
                whale_trade_id=whale_trade_id,
                event_timestamp=event.timestamp,
                detected_at=event.provider_timestamp,
                requested_contracts=event.contracts,
                context={"event_type": event.event_type, "tx_hash": event.tx_hash},
            )
            await session.commit()
            logger.debug(
                "PolyNode duplicate ignored: %s %s %s",
                event.wallet_address[:10],
                event.tx_hash[:16],
                event.token_id[:16],
            )
            return "duplicate"

        if event.side == "SELL":
            await _record_copy_decision(
                session,
                wallet_address=event.wallet_address,
                condition_id=event.condition_id,
                outcome=event.outcome,
                decision_code="SELL_IGNORED",
                decision_reason="sell_events_do_not_auto_execute",
                decision_source="websocket",
                whale_trade_id=whale_trade_id,
                event_timestamp=event.timestamp,
                detected_at=event.provider_timestamp,
                requested_contracts=event.contracts,
                context={"event_type": event.event_type, "tx_hash": event.tx_hash},
            )
            await session.commit()
            logger.info(
                "POLYNODE: watched whale SELL ignored for execution: %s %s %s %.2f @ %.3f",
                event.wallet_address[:10],
                event.condition_id[:10],
                event.outcome,
                float(event.contracts),
                float(event.price),
            )
            return "SELL_IGNORED"

        position = await session.get(
            WhalePosition,
            (event.wallet_address, event.condition_id, event.outcome),
        )
        if not position:
            await _record_copy_decision(
                session,
                wallet_address=event.wallet_address,
                condition_id=event.condition_id,
                outcome=event.outcome,
                decision_code="missing_position",
                decision_reason="whale_position_missing_after_event",
                decision_source="websocket",
                whale_trade_id=whale_trade_id,
                event_timestamp=event.timestamp,
                detected_at=event.provider_timestamp,
                requested_contracts=event.contracts,
                context={"event_type": event.event_type, "tx_hash": event.tx_hash},
            )
            await session.commit()
            return "missing_position"

        await session.commit()

    if not await _allow_new_buys():
        async with async_session() as session:
            await _record_copy_decision(
                session,
                wallet_address=event.wallet_address,
                condition_id=event.condition_id,
                outcome=event.outcome,
                decision_code="buy_health_halt",
                decision_reason="buy_health_halt",
                decision_source="websocket",
                whale_trade_id=whale_trade_id,
                event_timestamp=event.timestamp,
                detected_at=event.provider_timestamp,
                requested_contracts=event.contracts,
                context={"event_type": event.event_type, "tx_hash": event.tx_hash},
            )
            await session.commit()
        logger.warning(
            "POLYNODE: BUY persisted but copy skipped due to buy_health_halt: %s %s",
            event.wallet_address[:10],
            event.condition_id[:10],
        )
        return "buy_health_halt"

    pos = Position(
        conditionId=event.condition_id,
        tokenId=event.token_id,
        size=str(position.num_contracts or event.contracts),
        avgPrice=str(position.avg_entry_price or event.price),
        title=event.market_title,
        slug=event.market_slug,
        outcome=event.outcome,
    )
    clob = CLOBClient()
    try:
        wallet_usdc = await _get_wallet_usdc_for_sizing()
        if settings.live_execution_enabled and wallet_usdc <= 0:
            async with async_session() as session:
                await _record_copy_decision(
                    session,
                    wallet_address=event.wallet_address,
                    condition_id=event.condition_id,
                    outcome=event.outcome,
                    decision_code="balance_unavailable",
                    decision_reason="missing_live_wallet_balance",
                    decision_source="websocket",
                    whale_trade_id=whale_trade_id,
                    event_timestamp=event.timestamp,
                    detected_at=event.provider_timestamp,
                    requested_contracts=event.contracts,
                    context={"event_type": event.event_type, "tx_hash": event.tx_hash},
                )
                await session.commit()
            logger.warning(
                "POLYNODE: BUY persisted but copy skipped due to missing live wallet balance: %s %s",
                event.wallet_address[:10],
                event.condition_id[:10],
            )
            return "balance_unavailable"
        async with async_session() as session:
            code = await _evaluate_position(
                session,
                event.wallet_address,
                pos,
                clob,
                wallet_usdc,
                allow_new_buys=True,
                decision_source="websocket",
                whale_trade_id=whale_trade_id,
                event_timestamp=event.timestamp,
                detected_at=event.provider_timestamp,
            )
            await session.commit()
            logger.info(
                "POLYNODE: handled %s %s %s -> %s",
                event.wallet_address[:10],
                event.side,
                event.condition_id[:10],
                code,
            )
            return code
    finally:
        try:
            await clob.close()
        except Exception:
            pass


async def run_sync_loop(interval_seconds: float = 30):
    logger.info("Sync loop started (interval=%ds)", interval_seconds)
    while True:
        try:
            await sync_whale_positions_once()
        except Exception as e:
            logger.error("Sync loop error: %s", e, exc_info=True)
        await asyncio.sleep(interval_seconds)
