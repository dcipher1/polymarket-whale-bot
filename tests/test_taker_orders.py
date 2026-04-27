import pytest

from py_clob_client.clob_types import OrderType

from src.execution import order_manager
from src.polymarket.clob_auth import AuthenticatedCLOBClient


@pytest.mark.asyncio
async def test_clob_auth_posts_market_buy_as_fak(monkeypatch):
    calls = {}

    async def direct_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    class FakeClient:
        def create_market_order(self, order_args):
            calls["order_args"] = order_args
            return {"signed": True}

        def post_order(self, order, order_type):
            calls["posted_order"] = order
            calls["order_type"] = order_type
            return {"orderID": "order-1", "success": True}

    auth = AuthenticatedCLOBClient()
    auth._get_client = lambda: FakeClient()
    monkeypatch.setattr("src.polymarket.clob_auth.asyncio.to_thread", direct_to_thread)

    result = await auth.place_market_buy_fak("token-1", amount_usdc=12.34, max_price=0.56)

    assert result == {"orderID": "order-1", "success": True}
    assert calls["order_args"].token_id == "token-1"
    assert calls["order_args"].amount == 12.34
    assert calls["order_args"].price == 0.56
    assert calls["order_args"].side == "BUY"
    assert calls["order_args"].order_type == OrderType.FAK
    assert calls["order_type"] == OrderType.FAK


@pytest.mark.asyncio
async def test_place_taker_buy_rejects_zero_price_before_clob(monkeypatch):
    calls = {"client": 0}

    def fake_client():
        calls["client"] += 1
        raise AssertionError("CLOB must not be called for invalid price")

    monkeypatch.setattr(order_manager, "get_auth_client", fake_client)

    result = await order_manager.place_taker_buy("token-1", amount_usdc=5.0, max_price=0.0)

    assert result.accepted is False
    assert result.error == "invalid_price"
    assert calls["client"] == 0


@pytest.mark.asyncio
async def test_place_taker_buy_reports_fak_no_fill(monkeypatch):
    class FakeAuth:
        async def place_market_buy_fak(self, token_id, amount_usdc, max_price):
            return {"orderID": "order-1", "success": True}

        async def get_order(self, order_id):
            return {"status": "CANCELLED", "size_matched": "0"}

    monkeypatch.setattr(order_manager, "get_auth_client", lambda: FakeAuth())

    result = await order_manager.place_taker_buy("token-1", amount_usdc=25.0, max_price=0.5)

    assert result.order_id == "order-1"
    assert result.accepted is True
    assert result.status == "CANCELLED"
    assert result.filled_contracts == 0
    assert result.error is None


@pytest.mark.asyncio
async def test_place_taker_buy_reports_fak_fill(monkeypatch):
    class FakeAuth:
        async def place_market_buy_fak(self, token_id, amount_usdc, max_price):
            return {"orderID": "order-1", "success": True}

        async def get_order(self, order_id):
            return {"status": "MATCHED", "size_matched": "42", "price": "0.51"}

    monkeypatch.setattr(order_manager, "get_auth_client", lambda: FakeAuth())

    result = await order_manager.place_taker_buy("token-1", amount_usdc=25.0, max_price=0.512)

    assert result.order_id == "order-1"
    assert result.accepted is True
    assert result.status == "MATCHED"
    assert result.filled_contracts == 42
    assert result.avg_fill_price == 0.51
    assert result.max_price == 0.512
