from datetime import datetime, timezone
from decimal import Decimal
import json

import pytest

from src.config import settings
from src.polymarket.polynode_wallet_ws import (
    PolyNodeRateLimit,
    PolyNodeWalletStream,
    WhaleTradeEvent,
    build_polynode_subscription,
    normalize_polynode_message,
)
from src.signals import sync_positions


def _event(side: str = "BUY") -> WhaleTradeEvent:
    return WhaleTradeEvent(
        wallet_address=settings.watch_whales[0].lower(),
        condition_id="0xcondition",
        token_id="token-yes",
        outcome="YES",
        side=side,
        price=Decimal("0.42"),
        contracts=Decimal("12"),
        notional_usdc=Decimal("5.04"),
        tx_hash=f"tx-{side.lower()}",
        market_title="Will it rain in New York City on April 25?",
        market_slug="rain-nyc-apr-25",
        timestamp=datetime.now(timezone.utc),
        provider_timestamp=datetime.now(timezone.utc),
    )


def test_builds_polynode_wallet_subscription(monkeypatch):
    monkeypatch.setattr(settings, "polynode_subscription_type", "dome")
    sub = build_polynode_subscription()

    assert sub["action"] == "subscribe"
    assert sub["type"] == "dome"
    assert sub["filters"]["wallets"] == [w.lower() for w in settings.watch_whales]
    assert sub["filters"]["side"] == "BUY"
    assert sub["filters"]["snapshot_count"] == settings.polynode_snapshot_count


def test_builds_reconnect_subscription_with_since(monkeypatch):
    monkeypatch.setattr(settings, "polynode_subscription_type", "wallets")
    sub = build_polynode_subscription(1770000000000)

    assert sub["type"] == "wallets"
    assert sub["filters"]["since"] == 1770000000000
    assert "snapshot_count" not in sub["filters"]


def test_normalizes_dome_buy_event():
    wallet = settings.watch_whales[0].lower()
    events = normalize_polynode_message(
        {
            "type": "event",
            "timestamp": 1770000000000,
            "data": {
                "user": wallet,
                "condition_id": "0xcondition",
                "token_id": "token-yes",
                "token_label": "YES",
                "side": "BUY",
                "price": "0.42",
                "shares_normalized": "12.5",
                "transaction_hash": "0xtx",
                "market_title": "Will it rain in New York City on April 25?",
                "market_slug": "rain-nyc-apr-25",
                "timestamp": 1770000000000,
            },
        }
    )

    assert len(events) == 1
    event = events[0]
    assert event.wallet_address == wallet
    assert event.side == "BUY"
    assert event.outcome == "YES"
    assert event.price == Decimal("0.42")
    assert event.contracts == Decimal("12.5")
    assert event.notional_usdc == Decimal("5.250")
    assert event.tx_hash == "0xtx"


def test_normalizes_snapshot_and_ignores_duplicates_malformed_and_unwatched():
    wallet = settings.watch_whales[0].lower()
    events = normalize_polynode_message(
        {
            "type": "snapshot",
            "events": [
                {
                    "type": "event",
                    "data": {
                        "user": wallet,
                        "condition_id": "0xcondition",
                        "token_id": "token-yes",
                        "token_label": "YES",
                        "side": "BUY",
                        "price": "0.42",
                        "shares_normalized": "12",
                        "order_hash": "order-1",
                    },
                },
                {"type": "heartbeat"},
                {"type": "event", "data": {"user": "0xnotwatched", "side": "BUY"}},
                {"type": "event", "data": {"user": wallet, "side": "BUY", "price": "bad"}},
            ],
        }
    )

    assert len(events) == 1
    assert events[0].tx_hash == "order-1"


@pytest.mark.asyncio
async def test_stream_health_separates_subscribe_pong_and_buy_events(monkeypatch):
    wallet = settings.watch_whales[0].lower()
    handled = []
    health_updates = []

    async def on_event(event):
        handled.append(event)

    async def fake_cache_set(key, value, ex=None):
        assert key == "polynode:wallet_stream:health"
        health_updates.append(value)

    import src.polymarket.polynode_wallet_ws as ws_mod

    monkeypatch.setattr(ws_mod, "cache_set", fake_cache_set)
    stream = PolyNodeWalletStream(on_event)

    await stream._handle_raw('{"type":"subscribed"}')
    await stream._handle_raw('{"type":"pong"}')
    await stream._handle_raw(
        json.dumps(
            {
                "type": "event",
                "timestamp": 1770000000000,
                "data": {
                    "user": wallet,
                    "condition_id": "0xcondition",
                    "token_id": "token-yes",
                    "token_label": "YES",
                    "side": "BUY",
                    "price": "0.42",
                    "shares_normalized": "12",
                    "transaction_hash": "0xtx",
                    "timestamp": 1770000000000,
                },
            }
        )
    )

    assert len(handled) == 1
    assert handled[0].side == "BUY"
    assert health_updates[-1]["last_subscribed_at"] is not None
    assert health_updates[-1]["last_pong_at"] is not None
    assert health_updates[-1]["last_buy_event_at"] is not None
    assert health_updates[-1]["last_sell_event_at"] is None
    assert health_updates[-1]["accepted_event_count"] == 1


@pytest.mark.asyncio
async def test_stream_free_tier_error_raises_rate_limit(monkeypatch):
    async def on_event(event):
        raise AssertionError("error messages must not emit trade events")

    async def fake_cache_set(*args, **kwargs):
        return None

    import src.polymarket.polynode_wallet_ws as ws_mod

    monkeypatch.setattr(ws_mod, "cache_set", fake_cache_set)
    stream = PolyNodeWalletStream(on_event)

    with pytest.raises(PolyNodeRateLimit):
        await stream._handle_raw(
            json.dumps(
                {
                    "type": "error",
                    "code": "free_tier_limit_reached",
                    "message": "limit",
                    "resets_at": "2026-04-27T00:00:00Z",
                }
            )
        )


@pytest.mark.asyncio
async def test_polynode_buy_event_persists_then_reuses_evaluator(monkeypatch):
    event = _event("BUY")
    calls = {"persist": 0, "evaluate": 0}

    class FakePosition:
        num_contracts = Decimal("12")
        avg_entry_price = Decimal("0.42")

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def get(self, model, key):
            return FakePosition()

        def add(self, value):
            return None

        async def commit(self):
            return None

    async def fake_persist(session, received):
        calls["persist"] += 1
        assert received is event
        return "inserted"

    async def fake_allow():
        return True

    async def fake_wallet_usdc():
        return 1000.0

    async def fake_find_trade(session, received):
        assert received is event
        return type("FakeWhaleTrade", (), {"id": 123})()

    async def fake_evaluate(session, addr, pos, clob, wallet_usdc, allow_new_buys, **kwargs):
        calls["evaluate"] += 1
        assert addr == event.wallet_address
        assert pos.condition_id == event.condition_id
        assert pos.outcome == event.outcome
        assert wallet_usdc == 1000.0
        assert allow_new_buys is True
        assert kwargs["decision_source"] == "websocket"
        assert kwargs["whale_trade_id"] == 123
        return "BUY"

    class FakeClob:
        async def close(self):
            return None

    monkeypatch.setattr(sync_positions, "async_session", lambda: FakeSession())
    monkeypatch.setattr(sync_positions, "persist_whale_trade_event", fake_persist)
    monkeypatch.setattr(sync_positions, "_find_whale_trade_by_event", fake_find_trade)
    monkeypatch.setattr(sync_positions, "_allow_new_buys", fake_allow)
    monkeypatch.setattr(sync_positions, "_get_wallet_usdc_for_sizing", fake_wallet_usdc)
    monkeypatch.setattr(sync_positions, "_evaluate_position", fake_evaluate)
    monkeypatch.setattr(sync_positions, "CLOBClient", FakeClob)

    result = await sync_positions.handle_polynode_wallet_event(event)

    assert result == "BUY"
    assert calls == {"persist": 1, "evaluate": 1}


@pytest.mark.asyncio
async def test_polynode_sell_event_never_evaluates_or_places_order(monkeypatch):
    event = _event("SELL")
    calls = {"evaluate": 0}

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        def add(self, value):
            return None

        async def commit(self):
            return None

    async def fake_persist(session, received):
        return "inserted"

    async def fake_evaluate(*args, **kwargs):
        calls["evaluate"] += 1
        return "BUY"

    async def fake_place_order(*args, **kwargs):
        raise AssertionError("SELL event must not place orders")

    async def fake_find_trade(session, received):
        assert received is event
        return type("FakeWhaleTrade", (), {"id": 124})()

    monkeypatch.setattr(sync_positions, "async_session", lambda: FakeSession())
    monkeypatch.setattr(sync_positions, "persist_whale_trade_event", fake_persist)
    monkeypatch.setattr(sync_positions, "_find_whale_trade_by_event", fake_find_trade)
    monkeypatch.setattr(sync_positions, "_evaluate_position", fake_evaluate)
    monkeypatch.setattr(sync_positions, "place_order", fake_place_order)

    result = await sync_positions.handle_polynode_wallet_event(event)

    assert result == "SELL_IGNORED"
    assert calls["evaluate"] == 0


@pytest.mark.asyncio
async def test_polynode_duplicate_event_does_not_evaluate(monkeypatch):
    event = _event("BUY")
    calls = {"evaluate": 0}

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        def add(self, value):
            return None

        async def commit(self):
            return None

    async def fake_persist(session, received):
        return "duplicate"

    async def fake_evaluate(*args, **kwargs):
        calls["evaluate"] += 1
        return "BUY"

    async def fake_find_trade(session, received):
        assert received is event
        return type("FakeWhaleTrade", (), {"id": 125})()

    monkeypatch.setattr(sync_positions, "async_session", lambda: FakeSession())
    monkeypatch.setattr(sync_positions, "persist_whale_trade_event", fake_persist)
    monkeypatch.setattr(sync_positions, "_find_whale_trade_by_event", fake_find_trade)
    monkeypatch.setattr(sync_positions, "_evaluate_position", fake_evaluate)

    result = await sync_positions.handle_polynode_wallet_event(event)

    assert result == "duplicate"
    assert calls["evaluate"] == 0


@pytest.mark.asyncio
async def test_polynode_backlogged_event_does_not_evaluate(monkeypatch):
    event = _event("BUY")
    calls = {"evaluate": 0}

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        def add(self, value):
            return None

        async def commit(self):
            return None

    async def fake_persist(session, received):
        return "backlog"

    async def fake_evaluate(*args, **kwargs):
        calls["evaluate"] += 1
        return "BUY"

    async def fake_find_trade(session, received):
        assert received is event
        return None

    monkeypatch.setattr(sync_positions, "async_session", lambda: FakeSession())
    monkeypatch.setattr(sync_positions, "persist_whale_trade_event", fake_persist)
    monkeypatch.setattr(sync_positions, "_find_whale_trade_by_event", fake_find_trade)
    monkeypatch.setattr(sync_positions, "_evaluate_position", fake_evaluate)

    result = await sync_positions.handle_polynode_wallet_event(event)

    assert result == "backlog"
    assert calls["evaluate"] == 0
