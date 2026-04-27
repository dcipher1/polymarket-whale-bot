from datetime import datetime, timedelta, timezone
from decimal import Decimal
import importlib.util
from pathlib import Path

import pytest

from src.execution.reconcile import _apply_resolved_position_to_trades
from src.execution.order_manager import quantize_order_price
from src.config import settings
from src.models import MyTrade
from src.signals import sync_positions
from src.tracking.resolution import apply_resolution_to_trade


_AUDIT_SPEC = importlib.util.spec_from_file_location(
    "audit_whale_pipeline",
    Path(__file__).resolve().parents[1] / "scripts" / "audit_whale_pipeline.py",
)
audit_whale_pipeline = importlib.util.module_from_spec(_AUDIT_SPEC)
assert _AUDIT_SPEC.loader is not None
_AUDIT_SPEC.loader.exec_module(audit_whale_pipeline)


@pytest.mark.asyncio
async def test_cancelled_zero_fill_is_non_economic(monkeypatch):
    trade = MyTrade(
        condition_id="0xabc",
        outcome="YES",
        entry_price=Decimal("0.42"),
        size_usdc=Decimal("42.00"),
        num_contracts=100,
        order_id="order-1",
        fill_status="PENDING",
        attribution={},
    )

    async def fake_status(order_id):
        return {"status": "CANCELLED", "size_matched": "0"}

    monkeypatch.setattr(sync_positions, "get_order_status", fake_status)
    await sync_positions._refresh_inflight(None, trade)

    assert trade.fill_status == "CANCELLED"
    assert trade.num_contracts == 0
    assert trade.size_usdc == Decimal("0")
    assert trade.attribution["requested_contracts"] == 100
    assert trade.attribution["requested_size_usdc"] == "42.00"


@pytest.mark.asyncio
async def test_partial_cancel_keeps_only_matched_notional(monkeypatch):
    trade = MyTrade(
        condition_id="0xabc",
        outcome="YES",
        entry_price=Decimal("0.40"),
        size_usdc=Decimal("40.00"),
        num_contracts=100,
        order_id="order-1",
        fill_status="PENDING",
        attribution={},
    )

    async def fake_status(order_id):
        return {"status": "CANCELLED", "size_matched": "25"}

    monkeypatch.setattr(sync_positions, "get_order_status", fake_status)
    await sync_positions._refresh_inflight(None, trade)

    assert trade.fill_status == "PARTIAL"
    assert trade.num_contracts == 25
    assert trade.size_usdc == Decimal("10.0")


@pytest.mark.asyncio
async def test_live_pending_order_keeps_requested_size_reserved(monkeypatch):
    trade = MyTrade(
        condition_id="0xabc",
        outcome="YES",
        entry_price=Decimal("0.40"),
        size_usdc=Decimal("40.00"),
        num_contracts=100,
        order_id="order-1",
        fill_status="PENDING",
        attribution={},
    )

    async def fake_status(order_id):
        return {"status": "LIVE", "size_matched": "25"}

    monkeypatch.setattr(sync_positions, "get_order_status", fake_status)
    await sync_positions._refresh_inflight(None, trade)

    assert trade.fill_status == "PENDING"
    assert trade.num_contracts == 100
    assert trade.attribution["requested_contracts"] == 100
    assert trade.attribution["requested_size_usdc"] == "40.00"
    assert trade.attribution["live_matched_contracts"] == 25


def test_copy_exposure_counts_pending_requested_as_reserved():
    filled = MyTrade(
        condition_id="0xabc",
        outcome="YES",
        fill_status="FILLED",
        num_contracts=40,
        size_usdc=Decimal("16.00"),
    )
    pending = MyTrade(
        condition_id="0xabc",
        outcome="YES",
        fill_status="PENDING",
        num_contracts=60,
        size_usdc=Decimal("24.00"),
        attribution={"requested_contracts": 75, "requested_size_usdc": "30.00"},
    )
    cancelled = MyTrade(
        condition_id="0xabc",
        outcome="YES",
        fill_status="CANCELLED",
        num_contracts=100,
        size_usdc=Decimal("40.00"),
    )

    exposure = sync_positions._copy_exposure([filled, pending, cancelled])

    assert exposure.filled_contracts == 40
    assert exposure.reserved_pending_contracts == 75
    assert exposure.effective_contracts == 115
    assert exposure.filled_usdc == 16.0
    assert exposure.reserved_pending_usdc == 30.0
    assert exposure.effective_usdc == 46.0


def test_copy_target_separates_target_met_from_under_min_notional():
    target = sync_positions._copy_target(100, 102)

    assert target.target_contracts == 100
    assert target.max_target_contracts == 105
    assert target.gap_contracts == -2
    assert target.buffered_gap_contracts == 3


def test_copy_target_detects_over_buffer_before_under_min_notional():
    target = sync_positions._copy_target(198.65, 305)

    assert target.target_contracts == 199
    assert target.max_target_contracts == 208
    assert target.gap_contracts < 0
    assert target.buffered_gap_contracts < 0


def test_resolution_applies_to_partial_trade():
    trade = MyTrade(
        condition_id="0xabc",
        outcome="YES",
        entry_price=Decimal("0.25"),
        size_usdc=Decimal("5.00"),
        num_contracts=20,
        fill_status="PARTIAL",
        resolved=False,
    )

    pnl = apply_resolution_to_trade(trade, "YES")

    assert round(pnl, 2) == 15.00
    assert trade.resolved is True
    assert trade.trade_outcome == "WIN"
    assert trade.pnl_usdc == Decimal("15.0")
    assert trade.exit_price == Decimal("1.0")


def test_reconcile_distributes_pnl_and_marks_cancelled_unfilled():
    filled = MyTrade(
        condition_id="0xabc",
        outcome="NO",
        fill_status="FILLED",
        size_usdc=Decimal("30.00"),
        num_contracts=60,
        resolved=False,
    )
    partial = MyTrade(
        condition_id="0xabc",
        outcome="NO",
        fill_status="PARTIAL",
        size_usdc=Decimal("10.00"),
        num_contracts=20,
        resolved=False,
    )
    cancelled = MyTrade(
        condition_id="0xabc",
        outcome="NO",
        fill_status="CANCELLED",
        size_usdc=Decimal("0"),
        num_contracts=0,
        resolved=False,
    )
    data = {"realized_pnl": -40.0, "cur_price": 0.0}

    changed = _apply_resolved_position_to_trades(
        [filled, partial, cancelled],
        data,
        datetime.now(timezone.utc),
    )

    assert changed is True
    assert filled.pnl_usdc == Decimal("-30.0")
    assert partial.pnl_usdc == Decimal("-10.0")
    assert filled.trade_outcome == "LOSS"
    assert partial.trade_outcome == "LOSS"
    assert cancelled.trade_outcome == "UNFILLED"
    assert cancelled.pnl_usdc == Decimal("0")
    assert cancelled.exit_price is None


def test_runtime_strategy_is_fixed_three_wallet_weather_copying():
    assert len(settings.watch_whales) == 3
    assert settings.valid_categories == {"weather"}
    assert settings.position_size_fraction == 1.0
    assert settings.fixed_position_size_usdc == 50
    assert sync_positions.MAX_ORDER_USDC == 50.0


def test_candidate_resolution_gate_allows_future_market_timestamp():
    now = datetime(2026, 4, 25, 16, 0, tzinfo=timezone.utc)
    market = type("MarketStub", (), {"resolution_time": now + timedelta(minutes=30)})()
    pos = type("PositionStub", (), {"end_date": ""})()

    allowed, code, resolution_time = sync_positions._candidate_resolution_gate(market, pos, now)

    assert allowed is True
    assert code == "before_resolution_time"
    assert resolution_time == now + timedelta(minutes=30)


def test_candidate_resolution_gate_skips_past_market_timestamp():
    now = datetime(2026, 4, 25, 16, 0, tzinfo=timezone.utc)
    market = type("MarketStub", (), {"resolution_time": now - timedelta(seconds=1)})()
    pos = type("PositionStub", (), {"end_date": now + timedelta(hours=1)})()

    allowed, code, resolution_time = sync_positions._candidate_resolution_gate(market, pos, now)

    assert allowed is False
    assert code == "past_resolution_time"
    assert resolution_time == now - timedelta(seconds=1)


def test_candidate_resolution_gate_allows_weather_same_local_day_after_gamma_noon():
    now = datetime(2026, 4, 25, 21, 0, tzinfo=timezone.utc)
    market = type(
        "MarketStub",
        (),
        {
            "category": "weather",
            "category_override": None,
            "question": "Will the highest temperature in San Francisco be between 58-59°F on April 25?",
            "resolution_time": datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc),
        },
    )()
    pos = type("PositionStub", (), {"end_date": "2026-04-25", "title": ""})()

    allowed, code, resolution_time = sync_positions._candidate_resolution_gate(market, pos, now)

    assert allowed is True
    assert code == "weather_local_date_active"
    assert resolution_time == datetime(2026, 4, 26, 7, 0, tzinfo=timezone.utc)


def test_candidate_resolution_gate_allows_nyc_same_local_evening():
    now = datetime(2026, 4, 25, 22, 27, tzinfo=timezone.utc)
    market = type(
        "MarketStub",
        (),
        {
            "category": "weather",
            "category_override": None,
            "question": "Will the highest temperature in New York City be between 50-51°F on April 25?",
            "resolution_time": datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc),
        },
    )()
    pos = type("PositionStub", (), {"end_date": "2026-04-25", "title": ""})()

    allowed, code, resolution_time = sync_positions._candidate_resolution_gate(market, pos, now)

    assert allowed is True
    assert code == "weather_local_date_active"
    assert resolution_time == datetime(2026, 4, 26, 4, 0, tzinfo=timezone.utc)


def test_candidate_resolution_gate_allows_sao_paulo_same_local_day_with_generic_midnight():
    now = datetime(2026, 4, 26, 16, 58, tzinfo=timezone.utc)
    market = type(
        "MarketStub",
        (),
        {
            "category": "weather",
            "category_override": None,
            "question": "Will the highest temperature in Sao Paulo be 31°C on April 26?",
            "resolution_time": datetime(2026, 4, 26, 0, 0, tzinfo=timezone.utc),
        },
    )()
    pos = type("PositionStub", (), {"end_date": "2026-04-26", "title": ""})()

    allowed, code, resolution_time = sync_positions._candidate_resolution_gate(market, pos, now)

    assert allowed is True
    assert code == "weather_local_date_active"
    assert resolution_time == datetime(2026, 4, 27, 3, 0, tzinfo=timezone.utc)


def test_candidate_resolution_gate_quiets_weather_after_city_local_day():
    now = datetime(2026, 4, 25, 16, 1, tzinfo=timezone.utc)
    market = type(
        "MarketStub",
        (),
        {
            "category": "weather",
            "category_override": None,
            "question": "Will the highest temperature in Hong Kong be 30°C on April 25?",
            "resolution_time": datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc),
        },
    )()
    pos = type("PositionStub", (), {"end_date": "2026-04-25", "title": ""})()

    allowed, code, resolution_time = sync_positions._candidate_resolution_gate(market, pos, now)

    assert allowed is False
    assert code == "stale_past_resolution"
    assert resolution_time == datetime(2026, 4, 25, 16, 0, tzinfo=timezone.utc)


def test_candidate_resolution_gate_quiets_paris_after_local_midnight():
    now = datetime(2026, 4, 25, 22, 25, tzinfo=timezone.utc)
    market = type(
        "MarketStub",
        (),
        {
            "category": "weather",
            "category_override": None,
            "question": "Will the highest temperature in Paris be 23°C on April 25?",
            "resolution_time": datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc),
        },
    )()
    pos = type("PositionStub", (), {"end_date": "2026-04-25", "title": ""})()

    allowed, code, resolution_time = sync_positions._candidate_resolution_gate(market, pos, now)

    assert allowed is False
    assert code == "stale_past_resolution"
    assert resolution_time == datetime(2026, 4, 25, 22, 0, tzinfo=timezone.utc)


def test_unknown_weather_city_does_not_hard_skip_generic_gamma_noon():
    now = datetime(2026, 4, 25, 22, 25, tzinfo=timezone.utc)
    market = type(
        "MarketStub",
        (),
        {
            "category": "weather",
            "category_override": None,
            "question": "Will the highest temperature in Unknownville be 23°C on April 25?",
            "resolution_time": datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc),
        },
    )()
    pos = type("PositionStub", (), {"end_date": "2026-04-25", "title": ""})()

    allowed, code, resolution_time = sync_positions._candidate_resolution_gate(market, pos, now)

    assert allowed is True
    assert code == "weather_time_unknown"
    assert resolution_time == datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc)


def test_candidate_resolution_gate_falls_back_to_position_end_date():
    now = datetime(2026, 4, 25, 16, 0, tzinfo=timezone.utc)
    pos = type("PositionStub", (), {"end_date": "2026-04-25T16:30:00Z"})()

    allowed, code, resolution_time = sync_positions._candidate_resolution_gate(None, pos, now)

    assert allowed is True
    assert code == "before_resolution_time"
    assert resolution_time == datetime(2026, 4, 25, 16, 30, tzinfo=timezone.utc)


def test_candidate_resolution_gate_skips_missing_timestamp():
    pos = type("PositionStub", (), {"end_date": ""})()

    allowed, code, resolution_time = sync_positions._candidate_resolution_gate(None, pos)

    assert allowed is False
    assert code == "missing_resolution_time"
    assert resolution_time is None


def test_candidate_resolution_gate_treats_date_only_as_missing_timestamp():
    pos = type("PositionStub", (), {"end_date": "2026-04-25"})()

    allowed, code, resolution_time = sync_positions._candidate_resolution_gate(None, pos)

    assert allowed is False
    assert code == "missing_resolution_time"
    assert resolution_time is None


def test_low_contract_price_is_not_tiny_when_aggregate_position_is_real():
    assert sync_positions._whale_position_notional(10_000, 0.05) == 500.0
    assert sync_positions._whale_position_notional(46, 0.0419) > sync_positions.MIN_NOTIONAL_USDC


@pytest.mark.asyncio
async def test_resolution_backlog_health_failure_does_not_block_live_buys(monkeypatch):
    async def fail_count():
        raise RuntimeError("db unavailable")

    monkeypatch.setattr(sync_positions, "_resolution_backlog_count", fail_count)

    assert await sync_positions._allow_new_buys() is True


@pytest.mark.asyncio
async def test_resolution_backlog_over_threshold_is_audit_only(monkeypatch):
    async def high_count():
        return settings.max_unresolved_past_resolution_markets + 100

    monkeypatch.setattr(sync_positions, "_resolution_backlog_count", high_count)

    assert await sync_positions._allow_new_buys() is True


@pytest.mark.asyncio
async def test_live_balance_failure_returns_zero(monkeypatch):
    monkeypatch.setattr(settings, "live_execution_enabled", True)

    class BadAuth:
        async def get_balance(self):
            raise RuntimeError("balance unavailable")

    import src.polymarket.clob_auth as clob_auth

    monkeypatch.setattr(clob_auth, "get_auth_client", lambda: BadAuth())

    assert await sync_positions._get_wallet_usdc_for_sizing() == 0.0


def test_trade_attribution_is_per_source_wallet():
    whale_a = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    whale_b = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    trade = MyTrade(
        condition_id="0xabc",
        outcome="YES",
        fill_status="FILLED",
        source_wallets=[whale_a],
    )

    assert sync_positions._trade_is_attributed_to(trade, whale_a) is True
    assert sync_positions._trade_is_attributed_to(trade, whale_b) is False


def test_buy_price_quantization_uses_mill_tick_without_crossing_limit():
    assert quantize_order_price(0.0099, "BUY") == pytest.approx(0.009)
    assert quantize_order_price(0.0101, "BUY") == pytest.approx(0.010)
    assert quantize_order_price(0.9999, "BUY") == pytest.approx(0.999)


def test_sell_price_quantization_uses_mill_tick_without_crossing_limit():
    assert quantize_order_price(0.0091, "SELL") == pytest.approx(0.010)
    assert quantize_order_price(0.0100, "SELL") == pytest.approx(0.010)


def test_fallback_stale_decision_records_are_throttled():
    sync_positions._LAST_STALE_DECISION_RECORD.clear()
    now = datetime(2026, 4, 26, 12, 0, tzinfo=timezone.utc)

    assert sync_positions._should_record_copy_decision(
        "fallback_poll", "0xwallet", "0xcid", "YES", "stale_past_resolution", now
    ) is True
    assert sync_positions._should_record_copy_decision(
        "fallback_poll", "0xwallet", "0xcid", "YES", "stale_past_resolution", now + timedelta(minutes=30)
    ) is False
    assert sync_positions._should_record_copy_decision(
        "fallback_poll", "0xwallet", "0xcid", "YES", "stale_past_resolution", now + timedelta(hours=2)
    ) is True


def test_websocket_decisions_are_not_throttled_like_fallback_stale_scans():
    sync_positions._LAST_STALE_DECISION_RECORD.clear()
    now = datetime(2026, 4, 26, 12, 0, tzinfo=timezone.utc)

    assert sync_positions._should_record_copy_decision(
        "websocket", "0xwallet", "0xcid", "YES", "past_resolution_time", now
    ) is True
    assert sync_positions._should_record_copy_decision(
        "websocket", "0xwallet", "0xcid", "YES", "past_resolution_time", now
    ) is True


def test_log_audit_classifies_recent_operational_failures(tmp_path):
    log_path = tmp_path / "bot.log"
    log_path.write_text(
        "\n".join(
            [
                "2026-04-26 14:20:01 ERROR    [src.polymarket.gamma_api] Failed to fetch /markets: Temporary failure in name resolution",
                "2026-04-26 14:20:02 WARNING  [src.polymarket.polynode_wallet_ws] PolyNode wallet stream error: timed out during opening handshake",
                "2026-04-26 14:20:03 ERROR    [src.execution.order_manager] Order placement failed: BUY Singapore 27 YES Apr27 5.00 @ 0.0000",
                "2026-04-26 14:20:04 WARNING  [src.tracking.resolution] Trade 99 resolved as UNFILLED (fill_status=CANCELLED) — no P&L",
            ]
        )
    )

    counts, samples, _cutoff = audit_whale_pipeline._summarize_logs(
        log_path,
        days=2,
        tz_name="America/Montreal",
    )

    assert counts["network_dns_failure"] == 1
    assert counts["polynode_handshake_timeout"] == 1
    assert counts["zero_price_order_attempt"] == 1
    assert counts["cancelled_unfilled_resolution_noise"] == 1
    assert "Singapore" in samples["zero_price_order_attempt"][0]
