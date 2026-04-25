from datetime import datetime, timezone
from decimal import Decimal

import pytest

from src.execution.reconcile import _apply_resolved_position_to_trades
from src.models import MyTrade
from src.signals import sync_positions
from src.tracking.resolution import apply_resolution_to_trade


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
