"""Test fixtures."""

import pytest
from decimal import Decimal
from datetime import datetime, timezone, timedelta


@pytest.fixture
def sample_wallet():
    return {
        "address": "0xabc123def456abc123def456abc123def456abc1",
        "total_pnl_usdc": Decimal("75000"),
        "conviction_score": 72,
        "trades_per_month": Decimal("15"),
    }


@pytest.fixture
def sample_market():
    return {
        "condition_id": "0xcondition123",
        "question": "Will the Fed cut rates at the March 2026 FOMC meeting?",
        "slug": "fed-rate-cut-march-2026",
        "tags": ["fed", "fomc", "interest-rates"],
        "end_date_iso": (datetime.now(timezone.utc) + timedelta(days=7)).isoformat(),
    }


@pytest.fixture
def sample_trades():
    base_time = datetime.now(timezone.utc) - timedelta(days=30)
    return [
        {
            "wallet_address": "0xabc123def456abc123def456abc123def456abc1",
            "condition_id": "0xcondition123",
            "token_id": "token_yes_123",
            "side": "BUY",
            "outcome": "YES",
            "price": Decimal("0.45"),
            "size_usdc": Decimal("500"),
            "num_contracts": Decimal("1111.11"),
            "timestamp": base_time,
            "tx_hash": "0xtx1",
        },
        {
            "wallet_address": "0xabc123def456abc123def456abc123def456abc1",
            "condition_id": "0xcondition123",
            "token_id": "token_yes_123",
            "side": "BUY",
            "outcome": "YES",
            "price": Decimal("0.48"),
            "size_usdc": Decimal("300"),
            "num_contracts": Decimal("625.00"),
            "timestamp": base_time + timedelta(hours=2),
            "tx_hash": "0xtx2",
        },
    ]


@pytest.fixture
def sample_category_metrics():
    return {
        "macro": {
            "win_rate": 0.62,
            "profit_factor": 2.1,
            "trade_count": 55,
            "followability": 0.68,
            "expectancy": 12.5,
        },
        "crypto_weekly": {
            "win_rate": 0.51,
            "profit_factor": 1.2,
            "trade_count": 30,
            "followability": 0.45,
            "expectancy": 3.2,
        },
    }
