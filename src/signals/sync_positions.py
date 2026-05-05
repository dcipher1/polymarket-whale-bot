"""Copy-trader loop for the watch-whales.

The PolyNode wallet websocket drives the hot path; this fallback poll runs
every wallet_sync_fallback_interval_seconds (currently 300s / 5 min) to
catch BUYs the websocket missed during reconnects or outages. For each
watch-whale's open weather position the loop:

  1. Refreshes status of any in-flight orders (capture recent fills).
  2. Cancels any remaining PENDING orders.
  3. Computes target_contracts = whale.size × position_size_fraction.
  4. Treats filled plus live pending requests as copied exposure.
  5. If exposure is already at target, holds; if over the 5% buffer, blocks more buys.
  6. If the remaining gap is below the CLOB minimum, skips as under_min_notional.
  7. Fetches book (bid + ask) and computes midpoint.
  8. Gates on midpoint vs whale_avg:
     - midpoint > whale_avg × 1.05  → SKIP slippage_above_5pct
     - midpoint < whale_avg × 0.80  → SKIP stale_below_20pct
  9. In-band pricing: submits a real FAK taker BUY if ask liquidity is
     available within whale_avg × 1.05.
 10. Never leaves whale-copy BUYs resting on the book; FAK fills immediately
     or cancels the unfilled remainder."""

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select, and_, func, update, tuple_
from sqlalchemy.dialects.postgresql import insert

from src.config import settings
from src.db import async_session
from src.execution.order_manager import (
    MAX_ORDER_PRICE,
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
from src.signals.event_pick import implied_yes_bucket
from src.signals.whale_belief import (
    edge_per_bucket,
    trade_decisions,
    whale_implied_distribution,
)
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
MAX_ORDER_USDC = 200.0     # Nibble cap — no single order larger than $200 notional.
MAX_POSITION_USDC = 400.0  # Per-position cap — total exposure on a single condition_id+outcome ≤ $400.
FAST_MAX_PRICE = 0.90      # Fast-execution absolute price ceiling (settings.fast_execution_whales).
FAST_MAX_ORDER_USDC = 200.0  # Fast-execution per-order nibble (matches default).
MIN_WALLET_USDC = 50.0     # Halt new BUYs when pUSD balance falls to this floor.
EVENT_PICK_MIN_CONTRACTS = 100  # Lower bound on event-pick targets (margin → 0).
EVENT_PICK_MAX_CONTRACTS = 400  # Upper bound on event-pick targets (margin → 1).
EDGE_TRADER_MAX_DIRECTION_USDC = 150.0  # Global per-cycle, per-direction (YES/NO) cap.
EDGE_TRADER_MAX_PER_WHALE_DIRECTION_USDC = 50.0  # Per-whale per-cycle, per-direction cap.
WHALE_TRADE_STALENESS_HOURS = 2  # Skip initiating a copy if whale's latest BUY is older than this.
LOW_PRICE_OBSERVATION = 0.10  # Log low average entries, but do not hard-skip real aggregate positions.
COPY_TARGET_BUFFER_FRAC = 1.05  # Allow only 5% above the watched whale's visible size.
TERMINAL_FILL = {"FILLED", "PARTIAL"}
STILL_OPEN = {"PENDING"}

_CITY_ABBR = {
    "New York City": "NYC", "New York": "NYC",
    "San Francisco": "SF", "Los Angeles": "LA",
    "Washington D.C.": "DC", "Washington DC": "DC",
}

_CITY_FROM_SLUG_RE = re.compile(r"^(?:highest|lowest)-temperature-in-(.+?)-on-")


def _city_from_slug(slug: str) -> str | None:
    """Extract canonical city name from a Polymarket weather market slug.
    Mirrors /home/pi/whale_finder/filters.py:_city_from_slug — keep in sync if
    Polymarket's slug shape evolves."""
    m = _CITY_FROM_SLUG_RE.match(slug or "")
    if not m:
        return None
    raw = m.group(1).replace("-", " ")
    return raw.upper() if len(raw) <= 3 else raw.title()


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


def _whale_position_notional(whale_size: float, whale_avg: float) -> float:
    return max(0.0, whale_size * whale_avg)


async def _latest_whale_buy_age_seconds(
    session, wallet: str, condition_id: str, outcome: str
) -> int | None:
    """Seconds since the most recent BUY by this whale on (condition_id, outcome).
    None if no whale_trade record exists for this combination."""
    result = await session.execute(
        select(WhaleTrade.timestamp)
        .where(
            and_(
                WhaleTrade.wallet_address == wallet.lower(),
                WhaleTrade.condition_id == condition_id,
                WhaleTrade.outcome == outcome,
                WhaleTrade.side == "BUY",
            )
        )
        .order_by(WhaleTrade.timestamp.desc())
        .limit(1)
    )
    ts = result.scalar_one_or_none()
    if ts is None:
        return None
    return int((datetime.now(timezone.utc) - ts).total_seconds())


async def _opposite_side_avg_cost(session, condition_id: str, outcome: str) -> float | None:
    """Weighted average entry price of our FILLED contracts on the OPPOSITE
    outcome of the same market. Used by the hedge-cost gate."""
    opposite = "NO" if outcome == "YES" else "YES"
    result = await session.execute(
        select(MyTrade).where(
            and_(
                MyTrade.condition_id == condition_id,
                MyTrade.outcome == opposite,
                MyTrade.fill_status.in_(("FILLED", "PARTIAL")),
            )
        )
    )
    total_size = 0
    weighted = 0.0
    for t in result.scalars().all():
        n = int(t.num_contracts or 0)
        p = float(t.entry_price or 0)
        if n > 0 and p > 0:
            total_size += n
            weighted += n * p
    return (weighted / total_size) if total_size > 0 else None


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


# Per-whale counter of consecutive empty Data API responses. We only mass-close
# absent positions after the second empty cycle, so a single transient API
# outage does not silently mark every tracked position CLOSE.
_CONSECUTIVE_EMPTY_POLLS: dict[str, int] = {}
EMPTY_POLL_CONFIRMATION_CYCLES = 2


async def _close_absent_whale_positions(
    session,
    addr: str,
    open_keys: set[tuple[str, str]],
) -> int:
    """Close DB whale positions absent from the latest Data API open snapshot."""
    if not open_keys:
        # Treat empty as suspect (likely a transient API outage). Require
        # EMPTY_POLL_CONFIRMATION_CYCLES consecutive empty polls before closing.
        count = _CONSECUTIVE_EMPTY_POLLS.get(addr, 0) + 1
        _CONSECUTIVE_EMPTY_POLLS[addr] = count
        if count < EMPTY_POLL_CONFIRMATION_CYCLES:
            logger.info(
                "SYNC: %s empty positions response (%d/%d) — deferring mass-close",
                addr[:10], count, EMPTY_POLL_CONFIRMATION_CYCLES,
            )
            return 0
        # WARN once on the cycle that mass-closes; then heartbeat every ~60h
        # (720 cycles × ~5 min). Intermediate cycles are DEBUG to keep the log clean.
        if count == EMPTY_POLL_CONFIRMATION_CYCLES or count % 720 == 0:
            logger.warning(
                "SYNC: %s %d consecutive empty positions responses — closing all DB rows",
                addr[:10], count,
            )
        else:
            logger.debug(
                "SYNC: %s %d consecutive empty positions responses (still empty)",
                addr[:10], count,
            )
    else:
        _CONSECUTIVE_EMPTY_POLLS.pop(addr, None)

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


def _copy_target(
    whale_size: float,
    effective_contracts: int,
    override_target: int | None = None,
) -> CopyTarget:
    if override_target is not None:
        target_contracts = override_target
        max_target_contracts = max(target_contracts, int(target_contracts * COPY_TARGET_BUFFER_FRAC))
    else:
        scaled = whale_size * settings.position_size_fraction
        target_contracts = int(round(scaled))
        max_target_contracts = max(target_contracts, int(scaled * COPY_TARGET_BUFFER_FRAC))
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
        # Freshly-placed FAKs may not be indexed by Polymarket yet. Don't
        # mark CANCELLED until the trade is old enough that a missing order
        # is genuine (rejected/purged) rather than just slow propagation.
        age = datetime.now(timezone.utc) - (trade.entry_timestamp or datetime.now(timezone.utc))
        if age.total_seconds() < 60:
            logger.info(
                "Trade %d: CLOB has no record of %s yet (age %.0fs) — leaving PENDING",
                trade.id, trade.order_id[:16], age.total_seconds(),
            )
            return
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
    fast_mode = (
        decision_source == "websocket"
        and addr.lower() in {w.lower() for w in (settings.fast_execution_whales or [])}
    )
    nibble_cap_usdc = FAST_MAX_ORDER_USDC if fast_mode else MAX_ORDER_USDC
    decision_context = {
        "wallet_usdc": round(wallet_usdc, 6),
        "allow_new_buys": allow_new_buys,
        "position_size_fraction": settings.position_size_fraction,
        "max_order_usdc": nibble_cap_usdc,
        "fast_mode": fast_mode,
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
    # When edge trader is enabled, all execution flows through the per-event
    # poll dispatch (_dispatch_edge_trader_for_whale). The websocket path still
    # persists the whale's trade but does not trigger 1:1 copy.
    if settings.edge_trader_enabled and addr.lower() in settings.edge_trader_whales:
        return await finish("edge_trader_handled_via_poll", "edge_trader_enabled")
    # Keep whale_size as float — fractional positions (e.g. 0.28 contracts) are
    # legitimate; downstream gates (under_min_contracts) will skip too-small
    # whales with a clearer reason than "whale_empty".
    whale_size = _f(pos.size)
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

    # Bucketed-event gate: for multi-bucket neg-risk events, only buy YES on the
    # single bucket whose YES-resolution would pay the whale the most given their
    # current holdings. See src/signals/event_pick.py.
    event_pick_target: int | None = None
    if settings.event_pick_enabled and market.neg_risk and market.event_id:
        pick_cid, pick_ctx = await implied_yes_bucket(session, addr, market.event_id)
        decision_context["event_pick"] = pick_ctx
        if pick_cid is None:
            logger.info("SYNC: %s | SKIP event_no_pick reason=%s",
                        mkt_tag, pick_ctx.get("reason", "unknown"))
            return await finish("event_no_pick", pick_ctx.get("reason", "unknown"))
        if pick_cid != pos.condition_id or outcome != "YES":
            logger.info("SYNC: %s | SKIP event_not_picked picked=%s_YES this=%s_%s",
                        mkt_tag, pick_cid[:10], pos.condition_id[:10], outcome)
            return await finish(
                "event_not_picked_bucket",
                f"picked={pick_cid[:10]}_YES; this={pos.condition_id[:10]}_{outcome}",
            )
        # Pick survived → size the trade by the picker's margin (conviction).
        margin = float(pick_ctx.get("margin", 0.0))
        pick_payout = float(pick_ctx.get("pick_payout", 0.0)) or 1.0
        margin_ratio = max(0.0, min(1.0, margin / pick_payout))
        span = EVENT_PICK_MAX_CONTRACTS - EVENT_PICK_MIN_CONTRACTS
        event_pick_target = int(round(EVENT_PICK_MIN_CONTRACTS + span * margin_ratio))
        decision_context["event_pick_margin_ratio"] = round(margin_ratio, 4)
        decision_context["event_pick_target"] = event_pick_target

    # Per-whale city allowlist gate. Whales not listed in watch_whale_cities
    # bypass this filter (default: any weather city).
    allowed_cities = settings.watch_whale_cities.get(addr.lower())
    if allowed_cities is not None:
        city = _city_from_slug(market.slug or pos.slug or "")
        if not city or city not in allowed_cities:
            logger.info("SYNC: %s | SKIP city_not_allowed:%s (allowed: %s)",
                        mkt_tag, city or "unknown", ", ".join(allowed_cities))
            return await finish("city_not_allowed", f"city_not_allowed:{city or 'unknown'}")

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

    # Staleness gate — don't INITIATE a copy on a whale buy older than the
    # threshold. Once we already have exposure, the whale's old action is
    # moot; we manage existing copies as usual (they're our own commitment).
    if filled_contracts == 0 and reserved_contracts == 0:
        age_s = await _latest_whale_buy_age_seconds(
            session, addr, pos.condition_id, outcome
        )
        threshold_s = WHALE_TRADE_STALENESS_HOURS * 3600
        if age_s is None or age_s > threshold_s:
            age_label = f"{age_s/3600:.1f}h" if age_s is not None else "no_record"
            if _note_decision(addr, pos.condition_id, outcome, f"SKIP:stale_signal:{age_label}"):
                logger.info(
                    "SYNC: %s | latest whale BUY age %s > %dh threshold → SKIP stale_signal",
                    mkt_tag, age_label, WHALE_TRADE_STALENESS_HOURS,
                )
            return await finish("stale_signal", f"stale_signal:{age_label}")
    copy_target = _copy_target(
        whale_size,
        effective_contracts,
        override_target=event_pick_target,
    )
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
    # Skip the gap-based notional floor for fast-track events: we override the
    # contract count below to always fill the $100 nibble, so the real notional
    # check happens post-override and an early skip here would short-circuit it.
    if max_allowed_notional < MIN_NOTIONAL_USDC and not fast_mode:
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

    # Price band. Default: anchor to whale_avg (5% above / 20% below).
    # Fast mode (websocket BUY from a fast_execution_whales wallet): bid up
    # to FAST_MAX_PRICE regardless of whale_avg slippage. Skips the band and
    # ask-cap gates so we chase whales whose own buy moves the book.
    if fast_mode:
        max_bid = FAST_MAX_PRICE
        min_bid = 0.0
    else:
        # Hard ceiling at settings.max_entry_price — buying above this leaves
        # too little upside vs. resolution risk regardless of the whale's avg.
        max_bid = min(whale_avg * CEILING_FRAC, settings.max_entry_price)
        min_bid = whale_avg * FLOOR_FRAC
    decision_context.update({"max_bid": round(max_bid, 6), "min_bid": round(min_bid, 6)})

    if not fast_mode and mid < min_bid:
        if _note_decision(addr, pos.condition_id, outcome, f"SKIP:stale_below_20pct:{mid:.3f}"):
            logger.info(
                "SYNC: %s | gap %d contracts | mid %.3f < %.3f × 0.80 = %.3f → SKIP stale_below_20pct",
                mkt_tag, gap_contracts, mid, whale_avg, min_bid,
            )
        return await finish("stale_below", "stale_below_20pct", filled_contracts=filled_contracts)
    if not fast_mode and mid > max_bid:
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
                "SYNC: %s | gap %d contracts | ask %.3f > cap %.3f → SKIP slippage_above_ask%s",
                mkt_tag, gap_contracts, ask, max_bid, " [FAST]" if fast_mode else "",
            )
        return await finish("slippage_above", "ask_above_slippage_cap", filled_contracts=filled_contracts)

    bid_price = ask
    price_reason = "FAK_TAKER"
    order_price = quantize_order_price(bid_price, "BUY")
    # max_price is a ceiling — round outward so a between-ticks cap admits the
    # adjacent tick instead of rejecting fills inside the caller's intended cap.
    max_price = quantize_order_price(max_bid, "BUY", direction="outside")
    # Clamp to CLOB upper bound. whale_avg×1.05 can exceed 0.999 when whale_avg
    # is near 1.0 (a near-resolved market) — Polymarket rejects such orders as
    # invalid_price. Cap the ceiling; if order_price itself is above the bound,
    # we'll skip below.
    if max_price > MAX_ORDER_PRICE:
        max_price = MAX_ORDER_PRICE
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
    if order_price > MAX_ORDER_PRICE:
        if _note_decision(addr, pos.condition_id, outcome, f"SKIP:near_resolution:{bid_price:.6f}"):
            logger.info(
                "SYNC: %s | ask %.4f at/above CLOB max %.3f — market near resolution → SKIP near_resolution",
                mkt_tag, bid_price, MAX_ORDER_PRICE,
            )
        return await finish("NO_VALID_PRICE", "price_above_max", requested_contracts=gap_contracts, filled_contracts=filled_contracts)

    # Hedge-cost gate: if we already hold the opposite side, skip when the
    # matched-pair cost (this BUY price + opposite-side avg cost) would exceed
    # $1.00 — a guaranteed loss at resolution. Whales hedge their own
    # positions; at our scale that just bleeds slippage.
    opposite_avg = await _opposite_side_avg_cost(session, pos.condition_id, outcome)
    if opposite_avg is not None and (order_price + opposite_avg) > 1.0:
        opp = "NO" if outcome == "YES" else "YES"
        if _note_decision(addr, pos.condition_id, outcome, "SKIP:hedge_cost_above_1"):
            logger.info(
                "SYNC: %s | hedge-cost: this %s @ $%.3f + existing %s avg $%.3f = $%.3f > $1 → SKIP hedge_cost_above_1",
                mkt_tag, outcome, order_price, opp, opposite_avg, order_price + opposite_avg,
            )
        return await finish(
            "hedge_cost_above_1",
            f"hedge_cost_above_1:{order_price + opposite_avg:.3f}",
            filled_contracts=filled_contracts,
        )

    contracts = min(gap_contracts, buffered_gap_contracts)
    # Fast-track whale gets a flat $200 nibble per websocket event regardless of
    # how small his actual buy was. Wallet floor (MIN_WALLET_USDC, gated upstream
    # via allow_new_buys), the per-position cap below, and the per-order nibble
    # cap further down are the only bounds.
    if fast_mode:
        contracts = max(contracts, int(nibble_cap_usdc / order_price))

    # Per-position cap — total notional on this condition_id+outcome ≤ MAX_POSITION_USDC.
    # Size down (or skip) if this order would push exposure past the cap.
    current_exposure_usdc = exposure.effective_usdc
    allowed_additional_usdc = max(0.0, MAX_POSITION_USDC - current_exposure_usdc)
    gap_usdc = contracts * order_price
    if gap_usdc > allowed_additional_usdc:
        capped_contracts = int(allowed_additional_usdc / order_price)
        if capped_contracts * order_price < MIN_NOTIONAL_USDC:
            if _note_decision(addr, pos.condition_id, outcome, "SKIP:position_cap"):
                logger.info(
                    "SYNC: %s | gap %d @ $%.2f = $%.2f | at/over position cap $%.2f (current $%.2f) → SKIP position_cap",
                    mkt_tag, contracts, order_price, gap_usdc,
                    MAX_POSITION_USDC, current_exposure_usdc,
                )
            return await finish("position_cap", filled_contracts=filled_contracts)
        logger.info(
            "SYNC: %s | sizing down %d → %d contracts (position cap $%.2f, current $%.2f)",
            mkt_tag, contracts, capped_contracts, MAX_POSITION_USDC, current_exposure_usdc,
        )
        contracts = capped_contracts
    decision_context.update(
        {
            "position_cap_usdc": MAX_POSITION_USDC,
            "allowed_additional_usdc": round(allowed_additional_usdc, 2),
        }
    )

    # Per-order nibble cap: never place > nibble_cap_usdc notional in one order.
    per_order_cap_contracts = int(nibble_cap_usdc / order_price)
    if contracts > per_order_cap_contracts:
        logger.info(
            "SYNC: %s | nibbling: gap %d → %d contracts ($%.0f cap @ %.4f)%s",
            mkt_tag, contracts, per_order_cap_contracts, nibble_cap_usdc, order_price,
            " [FAST]" if fast_mode else "",
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

    # Don't try to spend more than (wallet - floor) on a single order.
    # Without this clamp the bot queues full-nibble FAKs that CLOB rejects
    # with "not enough balance / allowance" when wallet is between $50 floor
    # and the nibble cap.
    available_usdc = max(0.0, wallet_usdc - MIN_WALLET_USDC)
    taker_amount_usdc = round(
        min(contracts * order_price, nibble_cap_usdc, allowed_additional_usdc, available_usdc),
        2,
    )
    decision_context["taker_amount_usdc"] = taker_amount_usdc
    if taker_amount_usdc < MIN_NOTIONAL_USDC:
        # Distinguish "wallet too thin" from "intent too small" for the log/decision tag.
        if available_usdc < MIN_NOTIONAL_USDC:
            tag = "SKIP:insufficient_balance"
            code = "insufficient_balance"
            msg = (
                f"SYNC: %s | FAK amount $%.2f under CLOB $1 min "
                f"(wallet $%.2f, floor $%.2f) → SKIP insufficient_balance"
            )
            args = (mkt_tag, taker_amount_usdc, wallet_usdc, MIN_WALLET_USDC)
        else:
            tag = "SKIP:under_min_notional"
            code = "under_min_notional"
            msg = "SYNC: %s | FAK amount $%.2f under CLOB $1 min → SKIP"
            args = (mkt_tag, taker_amount_usdc)
        if _note_decision(addr, pos.condition_id, outcome, tag):
            logger.info(msg, *args)
        return await finish("under_min" if code == "under_min_notional" else "insufficient_balance",
                            code, requested_contracts=contracts, filled_contracts=filled_contracts)
    # Clamp contract count down so we don't try to buy more shares than we can pay for.
    if taker_amount_usdc < contracts * order_price:
        contracts = int(taker_amount_usdc / order_price)

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

    # Polymarket-side DELAYED means the FAK was queued and will match async.
    # Keep the row PENDING so _refresh_inflight transitions it to FILLED on the
    # next cycle instead of abandoning the on-chain position as CANCELLED/0.
    is_delayed_async = (
        mode == "LIVE"
        and immediate_fill_contracts <= 0
        and (taker_result.status or "").upper() == "DELAYED"
        and taker_result.accepted
    )

    fill_status = "FILLED"
    if is_delayed_async:
        fill_status = "PENDING"
        actual_contracts = contracts
        size_usdc = round(contracts * order_price, 2)
        avg_fill_price = order_price
    elif immediate_fill_contracts <= 0:
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
        "SYNC: %s | whale_avg %.3f, mid %.3f | filled %d reserved %d effective %d / target %d max %d | gap %d → FAK BUY $%.2f max %.4f filled %d/%d [%s%s]",
        mkt_tag, whale_avg, mid, filled_contracts, reserved_contracts, effective_contracts, target_contracts, max_target_contracts, gap_contracts,
        taker_amount_usdc, max_price, immediate_fill_contracts, contracts, mode, " FAST" if fast_mode else "",
    )
    if is_delayed_async:
        return await finish(
            "FAK_DELAYED",
            "fak_queued_delayed",
            requested_contracts=contracts,
            filled_contracts=0,
            order_ids=[order_id],
            my_trade_id=trade.id,
            order_notional_usdc=taker_amount_usdc,
            execution_mode=mode,
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


async def _record_edge_decision(
    session,
    *,
    addr: str | None,
    condition_id: str | None,
    outcome: str | None,
    code: str,
    reason: str | None = None,
    context: dict | None = None,
) -> None:
    """Thin wrapper around _record_copy_decision that pins decision_source for
    the edge_trader path so every skip/exit produces a CopyDecision row."""
    await _record_copy_decision(
        session,
        wallet_address=addr,
        condition_id=condition_id,
        outcome=outcome,
        decision_code=code,
        decision_reason=reason or code,
        decision_source="edge_trader_poll",
        context=context or {},
    )


async def _execute_edge_decision(
    session,
    addr: str,
    event_id: str,
    decision,
    market_yes: float,
    yes_token_id: str | None,
    no_token_id: str | None,
    wallet_usdc: float,
    counts: dict[str, int],
    mode: str,
    cycle_spent: dict[str, float] | None = None,
    whale_spent: dict[str, float] | None = None,
) -> float:
    """Place a single belief-edge trade. Mutates cycle_spent + whale_spent in place. Returns wallet_usdc."""
    if cycle_spent is None:
        cycle_spent = {"YES": 0.0, "NO": 0.0}
    if whale_spent is None:
        whale_spent = {"YES": 0.0, "NO": 0.0}
    cid = decision.condition_id
    side = decision.side
    target_contracts = decision.target_contracts
    edge = decision.edge
    base_ctx = {
        "event_id": event_id,
        "mode": mode,
        "edge": round(edge, 6),
        "market_yes": round(market_yes, 6),
        "target_contracts": target_contracts,
    }
    mkt_tag = f"event{event_id[:8]}_{cid[:10]}_{side}"

    # Per-direction cycle caps (global + per-whale).
    if cycle_spent.get(side, 0.0) >= EDGE_TRADER_MAX_DIRECTION_USDC:
        code = f"{side.lower()}_cycle_cap"
        counts[code] = counts.get(code, 0) + 1
        logger.info("EDGE_SKIP: %s | %s spent=$%.2f >= cap $%.0f",
                    mkt_tag, code, cycle_spent.get(side, 0.0), EDGE_TRADER_MAX_DIRECTION_USDC)
        await _record_edge_decision(session, addr=addr, condition_id=cid, outcome=side,
                                    code=code, context={**base_ctx, "cycle_spent_usdc": round(cycle_spent.get(side, 0.0), 2)})
        return wallet_usdc
    if whale_spent.get(side, 0.0) >= EDGE_TRADER_MAX_PER_WHALE_DIRECTION_USDC:
        code = f"{side.lower()}_whale_cap"
        counts[code] = counts.get(code, 0) + 1
        logger.info("EDGE_SKIP: %s | %s spent=$%.2f >= cap $%.0f",
                    mkt_tag, code, whale_spent.get(side, 0.0), EDGE_TRADER_MAX_PER_WHALE_DIRECTION_USDC)
        await _record_edge_decision(session, addr=addr, condition_id=cid, outcome=side,
                                    code=code, context={**base_ctx, "whale_spent_usdc": round(whale_spent.get(side, 0.0), 2)})
        return wallet_usdc

    token_id = yes_token_id if side == "YES" else no_token_id
    if not token_id:
        counts["no_token_id"] = counts.get("no_token_id", 0) + 1
        logger.info("EDGE_SKIP: %s | no_token_id", mkt_tag)
        await _record_edge_decision(session, addr=addr, condition_id=cid, outcome=side,
                                    code="no_token_id", context=base_ctx)
        return wallet_usdc

    if side == "YES":
        mid = market_yes
    else:
        mid = max(0.001, 1.0 - market_yes)
    # Don't trade outside the 10c–90c band — extreme prices are either dust
    # (low edge × low price = no real PnL) or near-resolved (you're paying for
    # the consensus, not the edge).
    if mid < 0.10 or mid > 0.90:
        counts["price_out_of_band"] = counts.get("price_out_of_band", 0) + 1
        logger.info("EDGE_SKIP: %s | price_out_of_band mid=%.3f", mkt_tag, mid)
        await _record_edge_decision(session, addr=addr, condition_id=cid, outcome=side,
                                    code="price_out_of_band", context={**base_ctx, "mid": round(mid, 6)})
        return wallet_usdc
    max_price = min(MAX_ORDER_PRICE, mid + 0.02)
    if max_price < MIN_ORDER_PRICE or mid <= 0:
        counts["price_below_min"] = counts.get("price_below_min", 0) + 1
        logger.info("EDGE_SKIP: %s | price_below_min mid=%.5f max_price=%.5f", mkt_tag, mid, max_price)
        await _record_edge_decision(session, addr=addr, condition_id=cid, outcome=side,
                                    code="price_below_min", context={**base_ctx, "mid": round(mid, 6), "max_price": round(max_price, 6)})
        return wallet_usdc

    existing_trades = (await session.execute(
        select(MyTrade).where(MyTrade.condition_id == cid).where(MyTrade.outcome == side)
    )).scalars().all()
    exposure = _copy_exposure(list(existing_trades))
    remaining_position_usdc = max(0.0, MAX_POSITION_USDC - exposure.effective_usdc)
    if remaining_position_usdc < 1.0:
        counts["position_cap"] = counts.get("position_cap", 0) + 1
        logger.info("EDGE_SKIP: %s | position_cap exposure=$%.2f / cap $%.0f",
                    mkt_tag, exposure.effective_usdc, MAX_POSITION_USDC)
        await _record_edge_decision(session, addr=addr, condition_id=cid, outcome=side,
                                    code="position_cap",
                                    context={**base_ctx, "current_exposure_usdc": round(exposure.effective_usdc, 2)})
        return wallet_usdc

    # Sizing: enforce a minimum $1.50 nibble (above CLOB $1 floor) — for cheap
    # markets we scale contract count up to hit the floor, capped at 5× target
    # so we don't buy thousands of "lottery tickets" on near-resolved buckets.
    MIN_NIBBLE_USDC = 1.5
    MAX_CONTRACT_SCALE = 5
    global_remaining = max(0.0, EDGE_TRADER_MAX_DIRECTION_USDC - cycle_spent.get(side, 0.0))
    whale_remaining = max(0.0, EDGE_TRADER_MAX_PER_WHALE_DIRECTION_USDC - whale_spent.get(side, 0.0))
    cycle_remaining_usdc = min(global_remaining, whale_remaining)
    desired_usdc = target_contracts * mid
    nibble_usdc = max(desired_usdc, MIN_NIBBLE_USDC)
    cap_usdc = min(
        nibble_usdc, MAX_ORDER_USDC, remaining_position_usdc,
        max(0.0, wallet_usdc - MIN_WALLET_USDC),
        cycle_remaining_usdc,
    )
    if cap_usdc < MIN_NIBBLE_USDC:
        counts["floor_or_cap"] = counts.get("floor_or_cap", 0) + 1
        logger.info("EDGE_SKIP: %s | floor_or_cap cap_usdc=$%.2f cycle_rem=$%.2f wallet_rem=$%.2f",
                    mkt_tag, cap_usdc, cycle_remaining_usdc, max(0.0, wallet_usdc - MIN_WALLET_USDC))
        await _record_edge_decision(session, addr=addr, condition_id=cid, outcome=side,
                                    code="floor_or_cap",
                                    context={**base_ctx, "cap_usdc": round(cap_usdc, 2),
                                             "cycle_remaining_usdc": round(cycle_remaining_usdc, 2)})
        return wallet_usdc
    actual_contracts = max(MIN_ORDER_CONTRACTS, int(cap_usdc / mid))
    actual_contracts = min(actual_contracts, target_contracts * MAX_CONTRACT_SCALE)
    actual_usdc = round(actual_contracts * mid, 2)
    if actual_usdc < 1.0:
        counts["mid_too_low"] = counts.get("mid_too_low", 0) + 1
        logger.info(
            "EDGE_SKIP: %s | mid_too_low mid=%.5f target=%d capped_c=%d actual_$=%.2f",
            mkt_tag, mid, target_contracts, actual_contracts, actual_usdc,
        )
        await _record_edge_decision(session, addr=addr, condition_id=cid, outcome=side,
                                    code="mid_too_low",
                                    context={**base_ctx, "mid": round(mid, 6),
                                             "actual_contracts": actual_contracts,
                                             "actual_usdc": actual_usdc})
        return wallet_usdc

    if settings.live_execution_enabled:
        result = await place_taker_buy(
            token_id=token_id, amount_usdc=actual_usdc,
            max_price=max_price, tag=mkt_tag,
        )
        order_id = result.order_id
        immediate_fill = result.filled_contracts
        avg_fill_price = result.avg_fill_price or mid
        if not result.accepted or not order_id:
            counts["FAK_REJECTED"] = counts.get("FAK_REJECTED", 0) + 1
            logger.warning("EDGE_TRADE: %s | FAK rejected: %s", mkt_tag, result.error)
            await _record_edge_decision(session, addr=addr, condition_id=cid, outcome=side,
                                        code="FAK_REJECTED", reason=str(result.error or "rejected"),
                                        context={**base_ctx, "actual_usdc": actual_usdc,
                                                 "max_price": round(max_price, 6)})
            return wallet_usdc
    else:
        result = None
        order_id = f"paper_edge_{int(datetime.now(timezone.utc).timestamp() * 1000)}_{cid[:8]}"
        immediate_fill = actual_contracts
        avg_fill_price = mid

    actual_filled = immediate_fill if immediate_fill > 0 else 0
    # Polymarket DELAYED = FAK queued for async match. Order is STILL ACTIVE on
    # PM and will likely fill — must record as PENDING so the reconciler can
    # track it. Treating DELAYED as cancelled creates orphan positions.
    is_delayed_async = (
        settings.live_execution_enabled
        and actual_filled <= 0
        and (result.status or "").upper() == "DELAYED"
        and result.accepted
    )
    if actual_filled <= 0 and not is_delayed_async:
        counts["FAK_NO_FILL"] = counts.get("FAK_NO_FILL", 0) + 1
        logger.info(
            "EDGE_TRADE: %s mode=%s edge=%+.3f mkt_yes=%.3f → %s 0 @ %.3f → CANCELLED",
            mkt_tag, mode, edge, market_yes, side, mid,
        )
        await _record_edge_decision(session, addr=addr, condition_id=cid, outcome=side,
                                    code="FAK_NO_FILL",
                                    context={**base_ctx, "mid": round(mid, 6),
                                             "actual_usdc": actual_usdc,
                                             "max_price": round(max_price, 6),
                                             "order_id": order_id})
        return wallet_usdc

    if is_delayed_async:
        fill_status = "PENDING"
        actual_filled = actual_contracts  # reserve full requested for tracking
        avg_fill_price = mid
        size_usdc = round(actual_filled * avg_fill_price, 2)
    else:
        fill_status = "FILLED" if immediate_fill >= actual_contracts else "PARTIAL"
        size_usdc = round(actual_filled * avg_fill_price, 2)
    trade = MyTrade(
        condition_id=cid, outcome=side,
        entry_price=Decimal(str(round(avg_fill_price, 6))),
        size_usdc=Decimal(str(size_usdc)),
        num_contracts=actual_filled,
        order_id=order_id, fill_status=fill_status,
        entry_timestamp=datetime.now(timezone.utc),
        source_wallets=[addr],
        attribution={
            "source": "edge_trader",
            "event_id": event_id,
            "mode": mode,
            "edge": round(edge, 6),
            "market_yes": round(market_yes, 6),
            "target_contracts": target_contracts,
            "requested_contracts": actual_contracts,
            "requested_size_usdc": str(actual_usdc),
            "max_price": round(max_price, 6),
        },
    )
    session.add(trade)
    await session.flush()
    counts["BUY" if fill_status != "PENDING" else "FAK_DELAYED"] = (
        counts.get("BUY" if fill_status != "PENDING" else "FAK_DELAYED", 0) + 1
    )
    cycle_spent[side] = cycle_spent.get(side, 0.0) + size_usdc
    whale_spent[side] = whale_spent.get(side, 0.0) + size_usdc
    logger.info(
        "EDGE_TRADE: %s mode=%s edge=%+.3f mkt_yes=%.3f → %s %d @ %.3f → %s spent $%.2f "
        "whale=$%.2f/$%.2f global=$%.2f/$%.2f",
        mkt_tag, mode, edge, market_yes, side, actual_filled, avg_fill_price,
        fill_status, size_usdc,
        whale_spent.get("YES", 0.0), whale_spent.get("NO", 0.0),
        cycle_spent.get("YES", 0.0), cycle_spent.get("NO", 0.0),
    )
    await _record_edge_decision(
        session, addr=addr, condition_id=cid, outcome=side,
        code="FAK_DELAYED" if fill_status == "PENDING" else "BUY",
        reason="fak_queued_delayed" if fill_status == "PENDING" else (
            "fak_filled" if fill_status == "FILLED" else "fak_partial"
        ),
        context={
            **base_ctx,
            "fill_status": fill_status,
            "filled_contracts": actual_filled,
            "avg_fill_price": round(avg_fill_price, 6),
            "size_usdc": size_usdc,
            "max_price": round(max_price, 6),
            "order_id": order_id,
            "my_trade_id": trade.id,
        },
    )
    return wallet_usdc - size_usdc


async def _evaluate_event_edge(
    session,
    addr: str,
    event_id: str,
    whale_positions: list[Position],
    clob: CLOBClient,
    wallet_usdc: float,
    mode: str,
    counts: dict[str, int],
    cycle_spent: dict[str, float] | None = None,
    whale_spent: dict[str, float] | None = None,
) -> float:
    """Run inference + edge + trade for one event. Mutates dicts in place. Returns wallet."""
    if cycle_spent is None:
        cycle_spent = {"YES": 0.0, "NO": 0.0}
    if whale_spent is None:
        whale_spent = {"YES": 0.0, "NO": 0.0}
    result = await session.execute(
        select(Market.condition_id).where(Market.event_id == event_id)
    )
    bucket_cids = [row[0] for row in result.all()]
    if len(bucket_cids) < 2:
        counts["single_bucket"] = counts.get("single_bucket", 0) + 1
        logger.info("EDGE_SKIP: event=%s | single_bucket buckets=%d", event_id, len(bucket_cids))
        await _record_edge_decision(
            session, addr=addr, condition_id=None, outcome=None,
            code="single_bucket",
            context={"event_id": event_id, "mode": mode, "buckets": len(bucket_cids)},
        )
        return wallet_usdc

    yes_avg, no_avg, yes_size, no_size = {}, {}, {}, {}
    for pos in whale_positions:
        side = (pos.outcome or "").upper()
        try:
            sz = float(pos.size or 0)
            ap = float(pos.avg_price or 0)
        except (TypeError, ValueError):
            continue
        if sz <= 0 or ap <= 0:
            continue
        if side == "YES":
            yes_size[pos.condition_id] = sz
            yes_avg[pos.condition_id] = ap
        elif side == "NO":
            no_size[pos.condition_id] = sz
            no_avg[pos.condition_id] = ap

    whale_p = whale_implied_distribution(
        bucket_cids, yes_avg, no_avg, yes_size, no_size, mode=mode,
    )

    token_rows = (await session.execute(
        select(MarketToken.condition_id, MarketToken.outcome, MarketToken.token_id)
        .where(MarketToken.condition_id.in_(bucket_cids))
    )).all()
    yes_tokens: dict[str, str] = {}
    no_tokens: dict[str, str] = {}
    for cid, out, tid in token_rows:
        if (out or "").upper() == "YES":
            yes_tokens[cid] = tid
        else:
            no_tokens[cid] = tid

    yes_tids = [yes_tokens[c] for c in bucket_cids if c in yes_tokens]
    mids = await clob.get_midpoints(yes_tids) if yes_tids else {}
    market_yes: dict[str, float] = {}
    for cid in bucket_cids:
        tid = yes_tokens.get(cid)
        if not tid:
            continue
        m = mids.get(tid)
        if m is not None and 0 < m < 1:
            market_yes[cid] = m

    edges = edge_per_bucket(whale_p, market_yes)
    decisions = trade_decisions(edges)
    # Pick exactly one bucket per event — the largest |edge|. Trading multiple
    # buckets per event creates self-hedged books that lose in most outcomes.
    if decisions:
        decisions = [max(decisions, key=lambda d: abs(d.edge))]

    logger.info(
        "EDGE_EVAL whale=%s event=%s mode=%s buckets=%d priced=%d decisions=%d top_edges=%s",
        addr[:10], event_id, mode, len(bucket_cids), len(market_yes), len(decisions),
        {c[:10]: round(e, 3) for c, e in sorted(edges.items(), key=lambda kv: -abs(kv[1]))[:3]},
    )

    if not decisions:
        counts["decisions_empty"] = counts.get("decisions_empty", 0) + 1
        top = {c[:10]: round(e, 3) for c, e in sorted(edges.items(), key=lambda kv: -abs(kv[1]))[:3]}
        logger.info(
            "EDGE_SKIP: event=%s | decisions_empty (no bucket above edge threshold) top_edges=%s",
            event_id, top,
        )
        await _record_edge_decision(
            session, addr=addr, condition_id=None, outcome=None,
            code="decisions_empty",
            context={"event_id": event_id, "mode": mode, "top_edges": top,
                     "buckets": len(bucket_cids), "priced": len(market_yes)},
        )
        return wallet_usdc

    for d in decisions:
        if wallet_usdc <= MIN_WALLET_USDC:
            counts["wallet_floor"] = counts.get("wallet_floor", 0) + 1
            logger.info("EDGE_SKIP: event=%s | wallet_floor wallet=$%.2f", event_id, wallet_usdc)
            await _record_edge_decision(
                session, addr=addr, condition_id=d.condition_id, outcome=d.side,
                code="wallet_floor",
                context={"event_id": event_id, "mode": mode, "wallet_usdc": round(wallet_usdc, 2)},
            )
            break
        global_yes_full = cycle_spent.get("YES", 0.0) >= EDGE_TRADER_MAX_DIRECTION_USDC
        global_no_full = cycle_spent.get("NO", 0.0) >= EDGE_TRADER_MAX_DIRECTION_USDC
        whale_yes_full = whale_spent.get("YES", 0.0) >= EDGE_TRADER_MAX_PER_WHALE_DIRECTION_USDC
        whale_no_full = whale_spent.get("NO", 0.0) >= EDGE_TRADER_MAX_PER_WHALE_DIRECTION_USDC
        if (global_yes_full and global_no_full) or (whale_yes_full and whale_no_full):
            code = "global_caps_full" if (global_yes_full and global_no_full) else "whale_caps_full"
            counts[code] = counts.get(code, 0) + 1
            logger.info(
                "EDGE_SKIP: event=%s | %s global=YES$%.2f/NO$%.2f whale=YES$%.2f/NO$%.2f",
                event_id, code,
                cycle_spent.get("YES", 0.0), cycle_spent.get("NO", 0.0),
                whale_spent.get("YES", 0.0), whale_spent.get("NO", 0.0),
            )
            await _record_edge_decision(
                session, addr=addr, condition_id=d.condition_id, outcome=d.side,
                code=code,
                context={
                    "event_id": event_id, "mode": mode,
                    "global_yes": round(cycle_spent.get("YES", 0.0), 2),
                    "global_no": round(cycle_spent.get("NO", 0.0), 2),
                    "whale_yes": round(whale_spent.get("YES", 0.0), 2),
                    "whale_no": round(whale_spent.get("NO", 0.0), 2),
                },
            )
            break
        wallet_usdc = await _execute_edge_decision(
            session, addr, event_id, d, market_yes.get(d.condition_id, 0.0),
            yes_tokens.get(d.condition_id), no_tokens.get(d.condition_id),
            wallet_usdc, counts, mode, cycle_spent, whale_spent,
        )
    return wallet_usdc


async def _dispatch_edge_trader_for_whale(
    addr: str,
    positions: list[Position],
    clob: CLOBClient,
    initial_wallet_usdc: float,
    cycle_spent: dict[str, float] | None = None,
    whale_spent: dict[str, float] | None = None,
) -> tuple[dict[str, int], float]:
    """Run belief-edge trader for one whale. Mutates cycle_spent + whale_spent in place. Returns (counts, wallet)."""
    if cycle_spent is None:
        cycle_spent = {"YES": 0.0, "NO": 0.0}
    if whale_spent is None:
        whale_spent = {"YES": 0.0, "NO": 0.0}
    counts: dict[str, int] = {}
    mode = settings.edge_trader_whales.get(addr.lower(), "directional")
    wallet_usdc = initial_wallet_usdc

    cid_to_market: dict[str, Market] = {}
    # Per-whale city allowlist: each whale is a specialist; only fire signals on
    # events whose city is in their assigned list. Whales not in the dict trade
    # any city (legacy behavior).
    allowed_cities = settings.watch_whale_cities.get(addr.lower())
    events_to_eval: dict[str, list[Position]] = {}
    async with async_session() as session:
        for pos in positions:
            if not pos.condition_id or pos.condition_id in cid_to_market:
                continue
            try:
                m = await ensure_market(pos.condition_id, session, slug=pos.slug or None)
                if m:
                    cid_to_market[pos.condition_id] = m
            except Exception as e:
                logger.debug("EDGE: ensure_market failed for %s: %s", pos.condition_id[:10], e)

        for pos in positions:
            outcome = (pos.outcome or "").upper()
            outcome = outcome if outcome in {"YES", "NO"} else None
            m = cid_to_market.get(pos.condition_id)
            if not m or not m.event_id or not m.neg_risk:
                counts["no_event_metadata"] = counts.get("no_event_metadata", 0) + 1
                if _note_decision(addr, pos.condition_id or "", outcome or "", "SKIP:no_event_metadata"):
                    logger.info("EDGE_SKIP: %s %s | no_event_metadata",
                                addr[:10], (pos.condition_id or "?")[:10])
                await _record_edge_decision(
                    session, addr=addr, condition_id=pos.condition_id, outcome=outcome,
                    code="no_event_metadata",
                    context={"slug": pos.slug, "title": pos.title},
                )
                continue
            if (m.category or "").lower() != "weather":
                counts["non_weather"] = counts.get("non_weather", 0) + 1
                if _note_decision(addr, pos.condition_id, outcome or "", f"SKIP:non_weather:{m.category}"):
                    logger.info("EDGE_SKIP: %s %s | non_weather:%s",
                                addr[:10], pos.condition_id[:10], m.category)
                await _record_edge_decision(
                    session, addr=addr, condition_id=pos.condition_id, outcome=outcome,
                    code="non_weather",
                    context={"category": m.category, "event_id": m.event_id},
                )
                continue
            if allowed_cities is not None:
                city = _city_from_slug(m.slug or pos.slug or "")
                if not city or city not in allowed_cities:
                    counts["city_not_allowed"] = counts.get("city_not_allowed", 0) + 1
                    if _note_decision(addr, pos.condition_id, outcome or "",
                                      f"SKIP:city_not_allowed:{city or 'unknown'}"):
                        logger.info(
                            "EDGE_SKIP: %s %s | city_not_allowed:%s (allowed=%s)",
                            addr[:10], pos.condition_id[:10], city or "unknown",
                            ",".join(allowed_cities),
                        )
                    await _record_edge_decision(
                        session, addr=addr, condition_id=pos.condition_id, outcome=outcome,
                        code="city_not_allowed",
                        context={"city": city, "allowed_cities": list(allowed_cities),
                                 "event_id": m.event_id},
                    )
                    continue
            events_to_eval.setdefault(m.event_id, []).append(pos)
        await session.commit()

    if not events_to_eval:
        return counts, wallet_usdc

    for event_id, evt_positions in events_to_eval.items():
        # Halt conditions: take effect across this whale's remaining events.
        # We open a tiny session just to record the halt reason — guarantees
        # the SYNC COMPLETE counters reconcile against copy_decisions rows.
        if wallet_usdc <= MIN_WALLET_USDC:
            counts["wallet_floor"] = counts.get("wallet_floor", 0) + 1
            logger.info("EDGE_SKIP: %s event=%s | wallet_floor wallet=$%.2f",
                        addr[:10], event_id, wallet_usdc)
            async with async_session() as session:
                await _record_edge_decision(
                    session, addr=addr, condition_id=None, outcome=None,
                    code="wallet_floor",
                    context={"event_id": event_id, "wallet_usdc": round(wallet_usdc, 2)},
                )
                await session.commit()
            break
        global_full = (cycle_spent.get("YES", 0.0) >= EDGE_TRADER_MAX_DIRECTION_USDC
                       and cycle_spent.get("NO", 0.0) >= EDGE_TRADER_MAX_DIRECTION_USDC)
        whale_full = (whale_spent.get("YES", 0.0) >= EDGE_TRADER_MAX_PER_WHALE_DIRECTION_USDC
                      and whale_spent.get("NO", 0.0) >= EDGE_TRADER_MAX_PER_WHALE_DIRECTION_USDC)
        if global_full:
            counts["cycle_cap"] = counts.get("cycle_cap", 0) + 1
            logger.info("EDGE_SKIP: %s event=%s | cycle_cap (both directions full)",
                        addr[:10], event_id)
            async with async_session() as session:
                await _record_edge_decision(
                    session, addr=addr, condition_id=None, outcome=None,
                    code="cycle_cap",
                    context={"event_id": event_id,
                             "global_yes": round(cycle_spent.get("YES", 0.0), 2),
                             "global_no": round(cycle_spent.get("NO", 0.0), 2)},
                )
                await session.commit()
            break
        if whale_full:
            counts["whale_cap"] = counts.get("whale_cap", 0) + 1
            logger.info("EDGE_SKIP: %s event=%s | whale_cap (both directions full)",
                        addr[:10], event_id)
            async with async_session() as session:
                await _record_edge_decision(
                    session, addr=addr, condition_id=None, outcome=None,
                    code="whale_cap",
                    context={"event_id": event_id,
                             "whale_yes": round(whale_spent.get("YES", 0.0), 2),
                             "whale_no": round(whale_spent.get("NO", 0.0), 2)},
                )
                await session.commit()
            break
        async with async_session() as session:
            try:
                wallet_usdc = await _evaluate_event_edge(
                    session, addr, event_id, evt_positions, clob,
                    wallet_usdc, mode, counts, cycle_spent, whale_spent,
                )
                await session.commit()
            except Exception as e:
                counts["error"] = counts.get("error", 0) + 1
                logger.error("EDGE: event %s failed for %s: %s", event_id, addr[:10], e)
                await session.rollback()
                async with async_session() as err_session:
                    await _record_edge_decision(
                        err_session, addr=addr, condition_id=None, outcome=None,
                        code="error", reason=str(e)[:200],
                        context={"event_id": event_id},
                    )
                    await err_session.commit()
    return counts, wallet_usdc


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

    # Fetch wallet USDC once per cycle. Used to gate the floor (MIN_WALLET_USDC).
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
    if settings.live_execution_enabled and wallet_usdc <= MIN_WALLET_USDC:
        if allow_new_buys:
            logger.warning(
                "SYNC: wallet $%.2f at/below floor $%.2f — halting new BUYs",
                wallet_usdc, MIN_WALLET_USDC,
            )
        allow_new_buys = False

    cycle_spent: dict[str, float] = {"YES": 0.0, "NO": 0.0}
    whale_cycle_spent: dict[str, dict[str, float]] = {}

    # Reconcile stale PENDING edge_trader trades — Polymarket auto-cancels FAK
    # orders after a TTL expires, but the bot used to leave the row PENDING
    # forever. Without this refresh, _copy_exposure thinks capital is committed
    # to ghosts; new orders then over-commit and trigger "place_failed".
    if settings.edge_trader_enabled:
        async with async_session() as session:
            pending = (await session.execute(
                select(MyTrade)
                .where(MyTrade.fill_status == "PENDING")
                .where(MyTrade.attribution["source"].astext == "edge_trader")
            )).scalars().all()
            for trade in pending:
                try:
                    await _refresh_inflight(session, trade)
                except Exception as e:
                    logger.warning("EDGE: refresh failed for trade %d: %s", trade.id, e)
            await session.commit()
            still_pending = sum(1 for t in pending if t.fill_status == "PENDING")
            if pending:
                logger.info("EDGE: refreshed %d PENDING edge_trader trades (%d still pending)",
                            len(pending) - still_pending, still_pending)

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

            # Drop positions whose market has resolved (PM still lists them
            # with redeemable=True until the holder sweeps on-chain). They
            # would only generate stale_past_resolution decisions otherwise.
            live_positions = [p for p in positions if not p.redeemable]
            redeemable_skipped = len(positions) - len(live_positions)
            if redeemable_skipped:
                logger.info(
                    "SYNC: %s polling — %d open positions (skipped %d redeemable)",
                    addr[:10], len(live_positions), redeemable_skipped,
                )
            else:
                logger.info(
                    "SYNC: %s polling — %d open positions", addr[:10], len(live_positions),
                )
            open_position_keys = {
                (p.condition_id, (p.outcome or "").upper())
                for p in live_positions
                if p.condition_id and (p.outcome or "").upper() in {"YES", "NO"}
            }
            counts: dict[str, int] = {}
            if settings.edge_trader_enabled and addr in settings.edge_trader_whales:
                whale_spent = whale_cycle_spent.setdefault(addr, {"YES": 0.0, "NO": 0.0})
                counts, wallet_usdc = await _dispatch_edge_trader_for_whale(
                    addr, live_positions, clob, wallet_usdc, cycle_spent, whale_spent,
                )
                orders_placed += counts.get("BUY", 0)
            else:
                for pos in live_positions:
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
        ws_allow_new_buys = True
        if settings.live_execution_enabled and wallet_usdc <= MIN_WALLET_USDC:
            ws_allow_new_buys = False
            logger.warning(
                "POLYNODE: wallet $%.2f at/below floor $%.2f — halting websocket BUY: %s",
                wallet_usdc, MIN_WALLET_USDC, event.wallet_address[:10],
            )
        async with async_session() as session:
            code = await _evaluate_position(
                session,
                event.wallet_address,
                pos,
                clob,
                wallet_usdc,
                allow_new_buys=ws_allow_new_buys,
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
