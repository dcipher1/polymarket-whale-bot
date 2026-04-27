"""PolyNode wallet websocket stream for watched-wallet copy events."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Awaitable, Callable, Literal
from urllib.parse import urlencode

import websockets
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.events import cache_set
from src.indexer.market_ingester import ensure_market
from src.models import MarketToken, Wallet, WhaleEventBacklog, WhaleTrade, WhalePosition

logger = logging.getLogger(__name__)

EventHandler = Callable[["WhaleTradeEvent"], Awaitable[None]]
PersistResult = Literal["inserted", "duplicate", "backlog"]


@dataclass(frozen=True)
class WhaleTradeEvent:
    wallet_address: str
    condition_id: str
    token_id: str
    outcome: str
    side: str
    price: Decimal
    contracts: Decimal
    notional_usdc: Decimal
    tx_hash: str
    market_title: str = ""
    market_slug: str = ""
    timestamp: datetime | None = None
    provider_timestamp: datetime | None = None
    event_type: str = ""
    provider_event_id: str = ""
    raw_event: dict[str, Any] | None = None

    @property
    def dedup_key(self) -> tuple[str, str, str]:
        return (self.wallet_address, self.tx_hash, self.token_id)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _next_utc_midnight(now: datetime | None = None) -> datetime:
    now = now or _utcnow()
    next_day = now.date() + timedelta(days=1)
    return datetime.combine(next_day, time.min, tzinfo=timezone.utc)


class PolyNodeRateLimit(Exception):
    def __init__(self, message: str, resets_at: datetime | None = None):
        super().__init__(message)
        self.resets_at = resets_at


def _parse_ts(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        num = float(value)
        if num > 1_000_000_000_000:
            num /= 1000
        if num > 1_000_000_000:
            return datetime.fromtimestamp(num, tz=timezone.utc)
    except (TypeError, ValueError, OverflowError):
        pass
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _dec(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _norm_outcome(value: Any) -> str:
    outcome = str(value or "").strip().upper()
    if outcome in {"YES", "Y"}:
        return "YES"
    if outcome in {"NO", "N"}:
        return "NO"
    return ""


def _event_id(msg: dict[str, Any], data: dict[str, Any]) -> str:
    for key in ("tx_hash", "transaction_hash", "order_hash", "id", "event_id"):
        value = data.get(key) or msg.get(key)
        if value:
            return str(value)
    return ""


def build_polynode_subscription(last_event_ts_ms: int | None = None) -> dict[str, Any]:
    """Build the PolyNode subscribe message.

    PolyNode documents `dome` as the simplest per-wallet trade stream and
    `wallets` as the broader wallet activity stream. Keep the type configurable
    so ops can flip without code changes.
    """
    filters: dict[str, Any] = {
        "wallets": [addr.lower() for addr in settings.watch_whales],
        "side": "BUY",
        "snapshot_count": settings.polynode_snapshot_count,
    }
    if last_event_ts_ms:
        filters["since"] = last_event_ts_ms
        filters.pop("snapshot_count", None)
    sub_type = settings.polynode_subscription_type or "dome"
    if sub_type not in {"dome", "wallets"}:
        sub_type = "dome"
    return {"action": "subscribe", "type": sub_type, "filters": filters}


def normalize_polynode_message(
    message: dict[str, Any],
    watch_wallets: set[str] | None = None,
) -> list[WhaleTradeEvent]:
    """Normalize a PolyNode message into internal whale trade events."""
    watch = {w.lower() for w in (watch_wallets or settings.watch_whales)}
    msg_type = str(message.get("type", "")).lower()
    if msg_type in {"heartbeat", "pong", "subscribed", "unsubscribed"}:
        return []
    if msg_type == "snapshot":
        events: list[WhaleTradeEvent] = []
        for raw_event in message.get("events") or []:
            if isinstance(raw_event, dict):
                events.extend(normalize_polynode_message(raw_event, watch))
        return events
    if msg_type == "error":
        logger.warning("PolyNode websocket error: %s", message)
        return []

    data = message.get("data")
    if not isinstance(data, dict):
        data = message

    if msg_type == "event" or "shares_normalized" in data or "user" in data:
        event = _normalize_dome_event(message, data, watch)
        return [event] if event else []

    event_type = str(data.get("event_type") or msg_type).lower()
    if event_type == "settlement":
        return _normalize_settlement_event(message, data, watch)
    if event_type == "trade":
        event = _normalize_trade_event(message, data, watch)
        return [event] if event else []
    return []


def _normalize_dome_event(
    msg: dict[str, Any],
    data: dict[str, Any],
    watch: set[str],
) -> WhaleTradeEvent | None:
    wallet = str(data.get("user") or data.get("maker") or "").lower()
    if wallet not in watch:
        return None
    side = str(data.get("side") or "").upper()
    if side not in {"BUY", "SELL"}:
        return None
    price = _dec(data.get("price"))
    contracts = _dec(data.get("shares_normalized") or data.get("size"))
    if contracts is None and data.get("shares") is not None:
        raw = _dec(data.get("shares"))
        contracts = raw / Decimal("1000000") if raw is not None else None
    outcome = _norm_outcome(data.get("token_label") or data.get("outcome"))
    return _make_event(msg, data, wallet, side, price, contracts, outcome, "dome")


def _normalize_trade_event(
    msg: dict[str, Any],
    data: dict[str, Any],
    watch: set[str],
) -> WhaleTradeEvent | None:
    wallet = str(data.get("maker") or data.get("user") or "").lower()
    if wallet not in watch:
        return None
    side = str(data.get("side") or "").upper()
    if side not in {"BUY", "SELL"}:
        return None
    price = _dec(data.get("price"))
    contracts = _dec(data.get("size") or data.get("shares_normalized"))
    outcome = _norm_outcome(data.get("outcome") or data.get("token_label"))
    return _make_event(msg, data, wallet, side, price, contracts, outcome, "trade")


def _normalize_settlement_event(
    msg: dict[str, Any],
    data: dict[str, Any],
    watch: set[str],
) -> list[WhaleTradeEvent]:
    events: list[WhaleTradeEvent] = []
    token_outcomes = data.get("tokens") if isinstance(data.get("tokens"), dict) else {}

    for fill in data.get("trades") or []:
        if not isinstance(fill, dict):
            continue
        wallet = str(fill.get("maker") or fill.get("user") or "").lower()
        if wallet not in watch:
            continue
        token_id = str(fill.get("token_id") or fill.get("asset_id") or "")
        outcome = _norm_outcome(token_outcomes.get(token_id) or fill.get("outcome"))
        events.append(
            _make_event(
                msg,
                {**data, **fill, "token_id": token_id},
                wallet,
                str(fill.get("side") or "").upper(),
                _dec(fill.get("price") or data.get("taker_price")),
                _dec(fill.get("size") or data.get("taker_size")),
                outcome,
                "settlement",
            )
        )

    if events:
        return [event for event in events if event is not None]

    wallet = str(data.get("taker_wallet") or "").lower()
    if wallet not in watch:
        return []
    token_id = str(data.get("taker_token") or "")
    outcome = _norm_outcome(token_outcomes.get(token_id) or data.get("outcome"))
    event = _make_event(
        msg,
        data,
        wallet,
        str(data.get("taker_side") or "").upper(),
        _dec(data.get("taker_price")),
        _dec(data.get("taker_size")),
        outcome,
        "settlement",
    )
    return [event] if event else []


def _make_event(
    msg: dict[str, Any],
    data: dict[str, Any],
    wallet: str,
    side: str,
    price: Decimal | None,
    contracts: Decimal | None,
    outcome: str,
    event_type: str,
) -> WhaleTradeEvent | None:
    if side not in {"BUY", "SELL"} or not wallet:
        return None
    token_id = str(
        data.get("token_id")
        or data.get("asset_id")
        or data.get("taker_token")
        or ""
    )
    condition_id = str(data.get("condition_id") or data.get("market") or "")
    if not token_id or not condition_id or not outcome or price is None or contracts is None:
        return None
    if price <= 0 or contracts <= 0:
        return None

    tx_hash = _event_id(msg, data)
    if not tx_hash:
        tx_hash = f"polynode:{event_type}:{wallet}:{condition_id}:{token_id}:{data.get('timestamp') or msg.get('timestamp')}"
    provider_ts = _parse_ts(msg.get("timestamp") or data.get("detected_at"))
    event_ts = _parse_ts(data.get("timestamp") or data.get("detected_at") or msg.get("timestamp"))
    return WhaleTradeEvent(
        wallet_address=wallet,
        condition_id=condition_id,
        token_id=token_id,
        outcome=outcome,
        side=side,
        price=price,
        contracts=contracts,
        notional_usdc=price * contracts,
        tx_hash=str(tx_hash),
        market_title=str(data.get("market_title") or data.get("title") or data.get("event_title") or ""),
        market_slug=str(data.get("market_slug") or data.get("slug") or ""),
        timestamp=event_ts or _utcnow(),
        provider_timestamp=provider_ts or _utcnow(),
        event_type=event_type,
        provider_event_id=str(data.get("id") or msg.get("id") or ""),
        raw_event={"message": msg, "data": data},
    )


async def persist_whale_trade_event(session: AsyncSession, event: WhaleTradeEvent) -> PersistResult:
    """Insert a normalized event and update wallet/market/position state.

    Returns ``inserted`` only when the whale trade row was newly inserted.
    """
    await _ensure_wallet(session, event.wallet_address)
    market = await ensure_market(
        event.condition_id,
        session,
        slug=event.market_slug or None,
    )
    if not market:
        await record_whale_event_backlog(session, event, "market_hydration_failed")
        return "backlog"
    if not await _ensure_token_mapping(session, event):
        await record_whale_event_backlog(session, event, "token_mapping_conflict")
        return "backlog"

    stmt = insert(WhaleTrade).values(
        wallet_address=event.wallet_address,
        condition_id=event.condition_id,
        token_id=event.token_id,
        side=event.side,
        outcome=event.outcome,
        price=event.price,
        size_usdc=event.notional_usdc,
        num_contracts=event.contracts,
        timestamp=event.timestamp or _utcnow(),
        tx_hash=event.tx_hash,
        detected_at=event.provider_timestamp or _utcnow(),
    ).on_conflict_do_nothing(constraint="uq_whale_trades_dedup")
    result = await session.execute(stmt)
    inserted = result.rowcount > 0
    if inserted:
        await apply_whale_event_to_position(session, event)
        return "inserted"
    return "duplicate"


async def _ensure_wallet(session: AsyncSession, wallet_address: str) -> None:
    stmt = insert(Wallet).values(
        address=wallet_address,
        first_seen=_utcnow(),
    ).on_conflict_do_nothing(index_elements=["address"])
    await session.execute(stmt)


async def _ensure_token_mapping(session: AsyncSession, event: WhaleTradeEvent) -> bool:
    existing = await session.get(MarketToken, event.token_id)
    if existing:
        return existing.condition_id == event.condition_id and existing.outcome == event.outcome
    token_stmt = insert(MarketToken).values(
        token_id=event.token_id,
        condition_id=event.condition_id,
        outcome=event.outcome,
    ).on_conflict_do_nothing(index_elements=["token_id"])
    await session.execute(token_stmt)
    return True


async def record_whale_event_backlog(
    session: AsyncSession,
    event: WhaleTradeEvent,
    reason: str,
) -> None:
    raw = event.raw_event or {
        "wallet_address": event.wallet_address,
        "condition_id": event.condition_id,
        "token_id": event.token_id,
        "outcome": event.outcome,
        "side": event.side,
        "price": str(event.price),
        "contracts": str(event.contracts),
        "tx_hash": event.tx_hash,
    }
    stmt = insert(WhaleEventBacklog).values(
        provider="polynode",
        wallet_address=event.wallet_address,
        condition_id=event.condition_id,
        token_id=event.token_id,
        outcome=event.outcome,
        side=event.side,
        tx_hash=event.tx_hash,
        reason=reason,
        raw_event=raw,
        created_at=_utcnow(),
        last_attempt_at=_utcnow(),
        attempts=1,
    ).on_conflict_do_update(
        constraint="uq_whale_event_backlog_dedup",
        set_={
            "reason": reason,
            "raw_event": raw,
            "last_attempt_at": _utcnow(),
            "attempts": WhaleEventBacklog.attempts + 1,
            "resolved_at": None,
        },
    )
    await session.execute(stmt)


async def apply_whale_event_to_position(session: AsyncSession, event: WhaleTradeEvent) -> str:
    existing = await session.get(
        WhalePosition,
        (event.wallet_address, event.condition_id, event.outcome),
    )
    old_contracts = Decimal(str(existing.num_contracts or 0)) if existing else Decimal("0")
    old_cost = old_contracts * Decimal(str(existing.avg_entry_price or 0)) if existing else Decimal("0")

    if event.side == "BUY":
        new_contracts = old_contracts + event.contracts
        new_cost = old_cost + event.notional_usdc
        avg_entry = new_cost / new_contracts if new_contracts > 0 else Decimal("0")
        last_event_type = "OPEN" if old_contracts <= 0 else "ADD"
        first_entry = existing.first_entry if existing and existing.first_entry else event.timestamp
        is_open = True
    else:
        new_contracts = old_contracts - event.contracts
        if new_contracts <= 0:
            new_contracts = Decimal("0")
            avg_entry = Decimal("0")
            last_event_type = "CLOSE"
            is_open = False
        else:
            avg_entry = Decimal(str(existing.avg_entry_price or event.price or 0))
            last_event_type = "REDUCE"
            is_open = True
        first_entry = existing.first_entry if existing else None

    avg_entry = min(max(avg_entry, Decimal("0")), Decimal("1"))
    total_size = new_contracts * avg_entry
    stmt = insert(WhalePosition).values(
        wallet_address=event.wallet_address,
        condition_id=event.condition_id,
        outcome=event.outcome,
        avg_entry_price=avg_entry,
        total_size_usdc=total_size,
        num_contracts=new_contracts,
        first_entry=first_entry,
        last_updated=_utcnow(),
        is_open=is_open,
        last_event_type=last_event_type,
        slug=event.market_slug or None,
    ).on_conflict_do_update(
        constraint="whale_positions_pkey",
        set_={
            "avg_entry_price": avg_entry,
            "total_size_usdc": total_size,
            "num_contracts": new_contracts,
            "last_updated": _utcnow(),
            "is_open": is_open,
            "last_event_type": last_event_type,
            "slug": event.market_slug or (existing.slug if existing else None),
        },
    )
    await session.execute(stmt)
    return last_event_type


class PolyNodeWalletStream:
    """Reconnectable PolyNode websocket client."""

    def __init__(self, on_event: EventHandler):
        self.on_event = on_event
        self._running = False
        self._last_event_ts_ms: int | None = None
        self._reconnect_delay = 1.0
        self._connect_count = 0
        self._message_count = 0
        self._accepted_event_count = 0
        self._ignored_message_count = 0
        self._last_raw_message_type = ""
        self._last_trade_event_at: datetime | None = None
        self._last_buy_event_at: datetime | None = None
        self._last_sell_event_at: datetime | None = None
        self._last_subscribed_at: datetime | None = None
        self._last_pong_at: datetime | None = None
        self._last_error = ""
        self._provider_error_count = 0
        self._rate_limited_until: datetime | None = None

    async def run(self) -> None:
        if not settings.polynode_enabled:
            logger.info("PolyNode wallet stream disabled")
            return
        if not settings.polynode_api_key:
            logger.warning("PolyNode wallet stream disabled: POLYNODE_API_KEY is not set")
            return

        self._running = True
        while self._running:
            try:
                await self._connect_once()
                self._reconnect_delay = 1.0
            except asyncio.CancelledError:
                raise
            except PolyNodeRateLimit as exc:
                self._last_error = str(exc)
                self._rate_limited_until = exc.resets_at
                await self._record_health("rate_limited")
                sleep_seconds = self._rate_limit_sleep_seconds(exc.resets_at)
                logger.warning(
                    "PolyNode wallet stream rate-limited: %s; sleeping until %s UTC (%.0fs)",
                    exc,
                    exc.resets_at.isoformat() if exc.resets_at else "next retry",
                    sleep_seconds,
                )
                await asyncio.sleep(sleep_seconds)
                self._reconnect_delay = 1.0
            except Exception as exc:
                if "HTTP 429" in str(exc):
                    self._last_error = str(exc)
                    self._rate_limited_until = self._rate_limited_until or _next_utc_midnight()
                    await self._record_health("rate_limited")
                    sleep_seconds = self._rate_limit_sleep_seconds(self._rate_limited_until)
                    logger.warning(
                        "PolyNode wallet stream rate-limited at connect: %s; sleeping until %s UTC (%.0fs)",
                        exc,
                        self._rate_limited_until.isoformat(),
                        sleep_seconds,
                    )
                    await asyncio.sleep(sleep_seconds)
                    self._reconnect_delay = 1.0
                    continue
                self._last_error = str(exc)
                await self._record_health("error")
                logger.warning(
                    "PolyNode wallet stream error: %s; reconnecting in %.1fs",
                    exc,
                    self._reconnect_delay,
                )
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, 30.0)

    async def _connect_once(self) -> None:
        url = self._url()
        async with websockets.connect(url, ping_interval=None, ping_timeout=None, max_queue=None) as ws:
            self._connect_count += 1
            self._last_error = ""
            sub = build_polynode_subscription(self._last_event_ts_ms)
            await ws.send(json.dumps(sub))
            logger.info("PolyNode wallet stream subscribed: %s", sub)
            await self._record_health("connected")
            keepalive = asyncio.create_task(self._keepalive(ws))
            try:
                async for raw in ws:
                    await self._handle_raw(raw)
            finally:
                keepalive.cancel()

    def _url(self) -> str:
        sep = "&" if "?" in settings.polynode_ws_url else "?"
        return f"{settings.polynode_ws_url}{sep}{urlencode({'key': settings.polynode_api_key})}"

    async def _keepalive(self, ws: Any) -> None:
        while self._running:
            await asyncio.sleep(30)
            await ws.send(json.dumps({"action": "ping"}))

    async def _handle_raw(self, raw: str | bytes) -> None:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            message = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Invalid PolyNode JSON: %s", raw[:200])
            return

        self._message_count += 1
        msg_type = str(message.get("type") or "")
        msg_type_lower = msg_type.lower()
        self._last_raw_message_type = msg_type
        if msg_type_lower == "pong":
            self._last_pong_at = _utcnow()
        elif msg_type_lower == "subscribed":
            self._last_subscribed_at = _utcnow()
        if msg_type_lower == "error":
            self._provider_error_count += 1
            code = str(message.get("code") or "")
            if code == "free_tier_limit_reached":
                resets_at = _parse_ts(message.get("resets_at"))
                raise PolyNodeRateLimit(str(message.get("message") or code), resets_at)
        events = normalize_polynode_message(message)
        if not events:
            self._ignored_message_count += 1
            await self._record_health("message")
            return
        await self._record_health("message")
        for event in events:
            ts = event.provider_timestamp or event.timestamp
            if ts:
                self._last_event_ts_ms = max(
                    self._last_event_ts_ms or 0,
                    int(ts.timestamp() * 1000),
                )
            self._accepted_event_count += 1
            self._last_trade_event_at = _utcnow()
            if event.side == "BUY":
                self._last_buy_event_at = self._last_trade_event_at
            elif event.side == "SELL":
                self._last_sell_event_at = self._last_trade_event_at
            await self._record_health("event")
            await self.on_event(event)

    async def stop(self) -> None:
        self._running = False

    def _rate_limit_sleep_seconds(self, resets_at: datetime | None) -> float:
        if resets_at is None:
            return max(self._reconnect_delay, 300.0)
        return max(30.0, (resets_at - _utcnow()).total_seconds() + 5.0)

    async def _record_health(self, status: str) -> None:
        try:
            await cache_set(
                "polynode:wallet_stream:health",
                {
                    "status": status,
                    "updated_at": _utcnow().isoformat(),
                    "last_event_ts_ms": self._last_event_ts_ms,
                    "connect_count": self._connect_count,
                    "message_count": self._message_count,
                    "accepted_event_count": self._accepted_event_count,
                    "ignored_message_count": self._ignored_message_count,
                    "last_raw_message_type": self._last_raw_message_type,
                    "last_trade_event_at": self._last_trade_event_at.isoformat()
                    if self._last_trade_event_at
                    else None,
                    "last_buy_event_at": self._last_buy_event_at.isoformat()
                    if self._last_buy_event_at
                    else None,
                    "last_sell_event_at": self._last_sell_event_at.isoformat()
                    if self._last_sell_event_at
                    else None,
                    "last_subscribed_at": self._last_subscribed_at.isoformat()
                    if self._last_subscribed_at
                    else None,
                    "last_pong_at": self._last_pong_at.isoformat()
                    if self._last_pong_at
                    else None,
                    "last_error": self._last_error,
                    "provider_error_count": self._provider_error_count,
                    "rate_limited_until": self._rate_limited_until.isoformat()
                    if self._rate_limited_until
                    else None,
                },
                ex=600,
            )
        except Exception as exc:
            logger.debug("PolyNode health cache update failed: %s", exc)
