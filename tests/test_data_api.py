"""Tests for Data API models."""

from src.polymarket.data_api import Position, Activity, Trade


class TestDataModels:
    def test_position_model(self):
        data = {
            "asset": "token123",
            "market": "market123",
            "conditionId": "cond123",
            "tokenId": "tok123",
            "side": "YES",
            "size": "100",
            "avgPrice": "0.45",
            "currentValue": "50",
        }
        pos = Position.model_validate(data)
        assert pos.condition_id == "cond123"
        assert pos.token_id == "tok123"
        assert pos.avg_price == "0.45"

    def test_activity_model(self):
        data = {
            "id": "act1",
            "type": "trade",
            "conditionId": "cond123",
            "tokenId": "tok123",
            "side": "BUY",
            "size": "50",
            "price": "0.50",
            "timestamp": "2026-03-01T00:00:00Z",
            "transactionHash": "0xhash",
        }
        act = Activity.model_validate(data)
        assert act.condition_id == "cond123"
        assert act.transaction_hash == "0xhash"

    def test_trade_model(self):
        data = {
            "id": "trade1",
            "assetId": "tok123",
            "side": "BUY",
            "size": "100",
            "price": "0.42",
            "matchTime": "2026-03-01T12:00:00Z",
            "transactionHash": "0xhash2",
        }
        trade = Trade.model_validate(data)
        assert trade.asset_id == "tok123"
        assert trade.transaction_hash == "0xhash2"
