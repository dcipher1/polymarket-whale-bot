"""Unified 30s copy-trader loop.

For each watch-whale's open weather position, every 30 seconds:
  1. Refresh status of any in-flight orders (capture recent fills).
  2. Cancel any remaining PENDING orders (active 30s expiry — no stale bids).
  3. Compute target_contracts = whale.size × position_size_fraction.
  4. gap = target − filled. If gap * whale_avg < $1.05, HOLD.
  5. Fetch book (bid + ask). Compute midpoint.
  6. Gate on midpoint vs whale_avg:
     - midpoint > whale_avg × 1.05  → SKIP slippage_above_5pct
     - midpoint < whale_avg × 0.80  → SKIP stale_below_20pct
  7. In-band pricing: TAKE the ask if ask ≤ whale_avg × 1.05 (marketable limit).
     Otherwise fall back to passive mid+1 tick, capped at whale_avg × 1.05.
     The taker path exists because passive posts never fill in wide-spread markets
     (every cycle cancels after 30s, producing lots of attempts but no fills).
  8. Place a limit BUY at that price for `gap` contracts. Order lives at most 30s —
     next cycle will cancel it if still unfilled and re-decide with fresh book data."""

import asyncio
import logging
import re
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select, and_, func

from src.config import settings
from src.db import async_session
from src.execution.order_manager import cancel_order, get_order_status, place_order
from src.indexer.market_ingester import ensure_market
from src.indexer.wallet_ingester import ingest_wallet
from src.models import Market, MarketToken, MyTrade
from src.polymarket.clob_client import CLOBClient
from src.polymarket.data_api import DataAPIClient, Position

logger = logging.getLogger(__name__)

MIN_NOTIONAL_USDC = 1.05  # CLOB rejects BUY orders under $1; pad a cent for rounding.
MIN_ORDER_CONTRACTS = 5    # CLOB rejects orders smaller than 5 contracts.
FLOOR_FRAC = 0.80          # Skip if midpoint < whale_avg * FLOOR_FRAC (20% floor — thesis broken).
CEILING_FRAC = 1.05        # Allow bidding up to whale_avg * CEILING_FRAC (5% above — capture winners).
MAX_POSITION_FRAC_OF_WALLET = 0.10  # No single position > 10% of wallet USDC.
MAX_ORDER_USDC = 50.0      # Nibble cap — no single order larger than $50 notional.
MIN_TRADE_PRICE = 0.10     # Don't trade markets priced below 10¢ — penny markets, high gamma, often near resolution.
TERMINAL_FILL = {"FILLED", "PARTIAL"}
STILL_OPEN = {"PENDING"}

_CITY_ABBR = {
    "New York City": "NYC", "New York": "NYC",
    "San Francisco": "SF", "Los Angeles": "LA",
    "Washington D.C.": "DC", "Washington DC": "DC",
}
_MONTH_ABBR = {
    "January": "Jan", "February": "Feb", "March": "Mar", "April": "Apr",
    "May": "May", "June": "Jun", "July": "Jul", "August": "Aug",
    "September": "Sep", "October": "Oct", "November": "Nov", "December": "Dec",
}


def _mkt_tag(title: str | None, outcome: str, end_date: str | None) -> str:
    """Compact label like 'NYC 70-71 NO Apr23' for log lines."""
    t = title or ""
    city = "?"
    m = re.search(r"temperature in ([^?]+?) be ", t)
    if m:
        city = m.group(1).strip()
        city = _CITY_ABBR.get(city, city)
    temp = "?"
    m = re.search(r"(\d+-\d+|\d+)°F", t)
    if m:
        temp = m.group(1)
    # Prefer end_date ISO (e.g. "2026-04-23") → "Apr23"
    date_tag = ""
    ed = (end_date or "")[:10]
    if len(ed) == 10:
        try:
            dt = datetime.fromisoformat(ed)
            date_tag = f"{_MONTH_ABBR.get(dt.strftime('%B'), dt.strftime('%b'))}{dt.day}"
        except Exception:
            date_tag = ed
    return f"{city} {temp} {outcome} {date_tag}".strip()

# Suppress repeat logging of the same decision on the same (addr, cid, outcome).
_LAST_DECISION: dict[tuple[str, str, str], str] = {}
_LAST_HEALTH_ALERT_COUNT: int | None = None


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
        pass


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
) -> str:
    """Handle one whale open position end-to-end for this cycle.

    Returns a short decision code (e.g. 'BUY', 'HOLD', 'slippage_above', 'tiny_whale')
    so the caller can tally a per-cycle summary.
    """
    outcome = (pos.outcome or "").upper()
    if outcome not in ("YES", "NO") or not pos.condition_id:
        return "invalid"
    whale_size = int(_f(pos.size))
    whale_avg = _f(pos.avg_price)
    if whale_size <= 0 or whale_avg <= 0:
        return "whale_empty"

    # Skip past-date markets FIRST — their orderbooks are torn down; we can't place or fill.
    # Putting this ahead of the penny filter means the cycle summary reflects past-date count
    # accurately (otherwise past-date penny/tiny positions inflate those buckets).
    end_date_str = (pos.end_date or "")[:10]
    today_iso = datetime.now(timezone.utc).date().isoformat()
    if end_date_str and end_date_str < today_iso:
        logger.debug("SYNC: %s | %s | SKIP past_date:%s",
                     addr[:10], pos.condition_id[:10], end_date_str)
        return "past_date"

    # Penny-market floor: whale_avg under 10¢ means we never want to trade this bucket.
    # (High gamma, often late in the day, low informational value.)
    if whale_avg < MIN_TRADE_PRICE:
        if _note_decision(addr, pos.condition_id, outcome, f"SKIP:penny:{whale_avg:.3f}"):
            logger.info("SYNC: %s | %s | SKIP penny_market whale_avg %.3f < %.2f",
                        addr[:10], pos.condition_id[:10], whale_avg, MIN_TRADE_PRICE)
        return "penny"

    # Only weather.
    market = await ensure_market(pos.condition_id, session, slug=pos.slug or None)
    if not market:
        logger.debug("SYNC: %s | %s | SKIP no_market_metadata", addr[:10], pos.condition_id[:10])
        return "no_market_metadata"
    category = market.category_override or market.category
    if category != "weather":
        logger.debug("SYNC: %s | %s | SKIP non_weather:%s",
                     addr[:10], pos.condition_id[:10], category or "None")
        return "non_weather"

    # Compute the readable tag early so cancel logs can use it too.
    mkt_tag = _mkt_tag(market.question or pos.title, outcome, pos.end_date)

    # Pull our trades on this (cid, outcome).
    our_result = await session.execute(
        select(MyTrade).where(
            and_(MyTrade.condition_id == pos.condition_id, MyTrade.outcome == outcome)
        )
    )
    our_trades: list[MyTrade] = list(our_result.scalars().all())

    # Refresh fill status on any in-flight orders so we don't cancel what just filled.
    in_flight = [t for t in our_trades if t.fill_status in STILL_OPEN]
    for t in in_flight:
        await _refresh_inflight(session, t)

    # Cancel any still-PENDING orders — active 30s expiry, no stale bids.
    in_flight = [t for t in our_trades if t.fill_status == "PENDING"]
    for t in in_flight:
        await _cancel_inflight(session, t, reason="cycle_expiry", tag=mkt_tag)

    # After cancels, in-flight count is effectively zero (anything that slipped through
    # will be picked up next cycle).
    filled_contracts = sum(int(t.num_contracts or 0) for t in our_trades if t.fill_status in TERMINAL_FILL)
    target_contracts = int(round(whale_size * settings.position_size_fraction))
    gap_contracts = target_contracts - filled_contracts

    if gap_contracts * whale_avg < MIN_NOTIONAL_USDC:
        if _note_decision(addr, pos.condition_id, outcome, f"HOLD:{filled_contracts}"):
            logger.info(
                "SYNC: %s | whale %d × %.2f = %d contracts target | filled %d → HOLD",
                mkt_tag, whale_size, settings.position_size_fraction, target_contracts,
                filled_contracts,
            )
        return "HOLD"

    if not allow_new_buys:
        if _note_decision(addr, pos.condition_id, outcome, "SKIP:resolution_backlog_halt"):
            logger.warning(
                "SYNC: %s | gap %d contracts | SKIP resolution_backlog_halt",
                mkt_tag, gap_contracts,
            )
        return "resolution_backlog_halt"

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
        return "no_token"

    # Book fetch — need midpoint.
    bid, ask, mid = await _get_book_prices(clob, token.token_id)
    if mid is None:
        if _note_decision(addr, pos.condition_id, outcome, "SKIP:no_book"):
            logger.info("SYNC: %s | gap %d contracts | SKIP no_book", mkt_tag, gap_contracts)
        return "no_book"

    # Price band anchored to whale_avg — 5% ceiling above, 20% floor below.
    max_bid = whale_avg * CEILING_FRAC
    min_bid = whale_avg * FLOOR_FRAC

    if mid < min_bid:
        if _note_decision(addr, pos.condition_id, outcome, f"SKIP:stale_below_20pct:{mid:.3f}"):
            logger.info(
                "SYNC: %s | gap %d contracts | mid %.3f < %.3f × 0.80 = %.3f → SKIP stale_below_20pct",
                mkt_tag, gap_contracts, mid, whale_avg, min_bid,
            )
        return "stale_below"
    if mid > max_bid:
        if _note_decision(addr, pos.condition_id, outcome, f"SKIP:slippage_above_5pct:{mid:.3f}"):
            logger.info(
                "SYNC: %s | gap %d contracts | mid %.3f > %.3f × 1.05 = %.3f → SKIP slippage_above_5pct",
                mkt_tag, gap_contracts, mid, whale_avg, max_bid,
            )
        return "slippage_above"

    # In the band: prefer to TAKE the ask (cross the spread) when it's within our cap —
    # passive mid+1t posts don't fill in wide-spread markets (all cycle-expire after 30s).
    # Fall back to passive pricing only when the ask is above max_bid.
    if ask is not None and ask <= max_bid:
        bid_price = ask
        price_reason = "ask_taker"
    else:
        bid_price = min(mid + 0.01, max_bid)
        price_reason = "mid+1t" if bid_price < max_bid else "max_bid"

    # Per-market concentration cap — size down the order if placing it would push
    # this position beyond MAX_POSITION_FRAC_OF_WALLET of wallet USDC.
    contracts = gap_contracts
    position_cap_usdc = wallet_usdc * MAX_POSITION_FRAC_OF_WALLET
    current_exposure_usdc = sum(
        float(t.size_usdc or 0) for t in our_trades if t.fill_status in TERMINAL_FILL
    )
    allowed_additional_usdc = max(0.0, position_cap_usdc - current_exposure_usdc)
    gap_usdc = contracts * bid_price
    if gap_usdc > allowed_additional_usdc:
        capped_contracts = int(allowed_additional_usdc / bid_price)
        if capped_contracts * bid_price < MIN_NOTIONAL_USDC:
            if _note_decision(addr, pos.condition_id, outcome, "SKIP:position_cap"):
                logger.info(
                    "SYNC: %s | gap %d @ $%.2f = $%.2f | at/over %.0f%% cap $%.2f (current $%.2f) → SKIP position_cap",
                    mkt_tag, contracts, bid_price, gap_usdc,
                    MAX_POSITION_FRAC_OF_WALLET * 100, position_cap_usdc, current_exposure_usdc,
                )
            return "position_cap"
        logger.info(
            "SYNC: %s | sizing down %d → %d contracts (position cap $%.2f)",
            mkt_tag, contracts, capped_contracts, position_cap_usdc,
        )
        contracts = capped_contracts

    # Per-order nibble cap: never place > $50 notional in one order.
    per_order_cap_contracts = int(MAX_ORDER_USDC / bid_price)
    if contracts > per_order_cap_contracts:
        logger.info(
            "SYNC: %s | nibbling: gap %d → %d contracts ($%.0f cap @ %.4f)",
            mkt_tag, contracts, per_order_cap_contracts, MAX_ORDER_USDC, bid_price,
        )
        contracts = per_order_cap_contracts

    # CLOB has a 5-contract minimum per order. Skip if our final size is below that.
    if contracts < MIN_ORDER_CONTRACTS:
        if _note_decision(addr, pos.condition_id, outcome, f"SKIP:under_min_contracts:{contracts}"):
            logger.info(
                "SYNC: %s | order size %d < CLOB min %d → SKIP",
                mkt_tag, contracts, MIN_ORDER_CONTRACTS,
            )
        return "under_min"
    if contracts * bid_price < MIN_NOTIONAL_USDC:
        if _note_decision(addr, pos.condition_id, outcome, "SKIP:under_min_notional"):
            logger.info("SYNC: %s | order notional under CLOB $1 min → SKIP", mkt_tag)
        return "under_min"

    if settings.live_execution_enabled:
        order_id = await place_order(
            token_id=token.token_id,
            side="BUY",
            price=bid_price,
            size=float(contracts),
            tag=mkt_tag,
        )
        mode = "LIVE"
    else:
        order_id = f"paper_{int(datetime.now(timezone.utc).timestamp() * 1000)}_{pos.condition_id[:8]}"
        mode = "PAPER"

    if not order_id:
        if _note_decision(addr, pos.condition_id, outcome, "SKIP:place_failed"):
            logger.warning("SYNC: %s | gap %d contracts | order placement failed (likely no orderbook)", mkt_tag, contracts)
        return "place_failed"

    size_usdc = round(contracts * bid_price, 2)
    trade = MyTrade(
        condition_id=pos.condition_id,
        outcome=outcome,
        entry_price=Decimal(str(round(bid_price, 6))),
        size_usdc=Decimal(str(size_usdc)),
        num_contracts=contracts,
        order_id=order_id,
        fill_status="FILLED" if mode == "PAPER" else "PENDING",
        entry_timestamp=datetime.now(timezone.utc),
        source_wallets=[addr],
        attribution={
            "mkt_tag": mkt_tag,
            "whale_avg_price": str(round(whale_avg, 6)),
            "bid_price": str(round(bid_price, 6)),
            "midpoint": str(round(mid, 6)),
            "price_reason": price_reason,
            "whale_size": whale_size,
            "target_contracts": target_contracts,
            "requested_contracts": contracts,
            "requested_size_usdc": str(size_usdc),
            "size_fraction": settings.position_size_fraction,
            "category": "weather",
            "execution_mode": mode,
            "source": "sync_positions",
        },
    )
    session.add(trade)
    _LAST_DECISION.pop((addr, pos.condition_id, outcome), None)
    logger.info(
        "SYNC: %s | whale_avg %.3f, mid %.3f | filled %d / target %d | gap %d → BUY %d @ %.4f (%s) [%s]",
        mkt_tag, whale_avg, mid, filled_contracts, target_contracts, gap_contracts,
        contracts, bid_price, price_reason, mode,
    )
    return "BUY"


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
    """Return False when stale unresolved markets make PnL/open exposure unreliable."""
    global _LAST_HEALTH_ALERT_COUNT
    if not settings.halt_on_resolution_backlog:
        return True

    try:
        backlog = await _resolution_backlog_count()
    except Exception as e:
        logger.warning("SYNC: resolution backlog health check failed: %s", e)
        return True

    threshold = int(settings.max_unresolved_past_resolution_markets)
    if backlog <= threshold:
        _LAST_HEALTH_ALERT_COUNT = None
        return True

    logger.warning(
        "SYNC HALT: %d unresolved markets are past resolution_time (threshold=%d); "
        "new buys disabled until resolution/reconcile catches up",
        backlog, threshold,
    )
    if _LAST_HEALTH_ALERT_COUNT != backlog:
        _LAST_HEALTH_ALERT_COUNT = backlog
        try:
            from src.monitoring.telegram import send_alert
            await send_alert(
                "Whale bot buy halt: "
                f"{backlog} unresolved markets are past resolution_time "
                f"(threshold {threshold})."
            )
        except Exception as e:
            logger.debug("Resolution backlog alert failed: %s", e)
    return False


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
        # Fall back to starting_capital so a transient balance-API failure doesn't freeze the bot.
        wallet_usdc = float(settings.starting_capital)
        logger.warning("SYNC: balance fetch failed (using starting_capital $%.0f as fallback): %s",
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


async def run_sync_loop(interval_seconds: float = 30):
    logger.info("Sync loop started (interval=%ds)", interval_seconds)
    while True:
        try:
            await sync_whale_positions_once()
        except Exception as e:
            logger.error("Sync loop error: %s", e, exc_info=True)
        await asyncio.sleep(interval_seconds)
