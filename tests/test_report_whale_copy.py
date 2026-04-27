from types import SimpleNamespace
import importlib.util
from pathlib import Path
from datetime import datetime, timezone

import pytest

from src.polymarket.clob_client import _parse_midpoints_response


_SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "report_whale_copy.py"
_SPEC = importlib.util.spec_from_file_location("report_whale_copy", _SCRIPT_PATH)
report_whale_copy = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(report_whale_copy)


def test_position_copy_statuses():
    assert report_whale_copy._position_copy_status(100, 100, 1) == "FULL"
    assert report_whale_copy._position_copy_status(100, 50, 1) == "PARTIAL"
    assert report_whale_copy._position_copy_status(100, 0, 1) == "NO FILL"
    assert report_whale_copy._position_copy_status(100, 0, 0) == "NO ORDER"
    assert report_whale_copy._position_copy_status(100, 110, 1) == "OVERCOPIED"


def test_weather_position_parts_parse_city_temp_and_date():
    assert report_whale_copy._position_parts(
        "Will the highest temperature in San Francisco be between 62-63°F on April 27?",
        "0xcondition",
    ) == ("San Francisco", "62-63°F", "Apr27")
    assert report_whale_copy._position_parts(
        "Will the highest temperature in Paris be 22°C on April 27?",
        "0xcondition",
    ) == ("Paris", "22°C", "Apr27")
    assert report_whale_copy._position_parts(
        "Will the highest temperature in New York City be between 56-57°F on April 26?",
        "0xcondition",
    ) == ("New York City", "56-57°F", "Apr26")


def test_position_breakdown_render_shows_contract_gap_and_reason(monkeypatch):
    wallet = "0xabc123"
    monkeypatch.setattr(report_whale_copy.settings, "watch_whales", [wallet])
    rows = [
        SimpleNamespace(
            local_day="2026-04-26",
            wallet_address=wallet,
            condition_id="0xcondition",
            outcome="YES",
            question="Will the highest temperature in Atlanta be between 82-83°F on April 26?",
            category="weather",
            resolution_time=None,
            whale_buy_fills=4,
            whale_contracts=100.0,
            whale_cost=42.0,
            order_attempts=2,
            cancelled_orders=1,
            filled_contracts=45.0,
            filled_cost=20.0,
            statuses="CANCELLED,PARTIAL",
            latest_decision_code="stale_below",
            latest_decision_reason="stale_below_20pct",
            latest_decision_source="fallback_poll",
            latest_latency_seconds=None,
        )
    ]

    output = report_whale_copy._render_position_breakdown_markdown(rows, "America/Montreal")

    assert "| Day | Whale | City | Temp | Date | Side | Whale fills | Whale contracts | Whale cost | Bot attempts | Bot filled | Gap | Copy % | Status | Reason | Source |" in output
    assert "| 2026-04-26 | 0xabc123 | Atlanta | 82-83°F | Apr26 | YES | 4 | 100 | $42.00 | 2 | 45 | 55 | 45% | PARTIAL | stale_below | fallback_poll |" in output
    assert "| 2026-04-26 | 0xabc123 | 1 | 100 | 45 | 55 | 0 | 1 | 0 | 0 | 0 |" in output


def test_position_breakdown_text_is_plain_table(monkeypatch):
    wallet = "0xabc123"
    monkeypatch.setattr(report_whale_copy.settings, "watch_whales", [wallet])
    rows = [
        SimpleNamespace(
            local_day="2026-04-26",
            wallet_address=wallet,
            condition_id="0xcondition",
            outcome="YES",
            question="Will the highest temperature in Atlanta be between 82-83°F on April 26?",
            category="weather",
            resolution_time=None,
            whale_buy_fills=4,
            whale_contracts=100.0,
            whale_cost=42.0,
            order_attempts=2,
            cancelled_orders=1,
            filled_contracts=45.0,
            filled_cost=20.0,
            statuses="CANCELLED,PARTIAL",
            latest_decision_code="stale_below",
            latest_decision_reason="stale_below_20pct",
            latest_decision_source="fallback_poll",
            latest_latency_seconds=None,
        )
    ]

    output = report_whale_copy._render_position_breakdown_text(rows, "America/Montreal", width=160)

    assert "Summary" in output
    assert "Positions" in output
    assert "╭" in output
    assert "│" in output
    assert "Atlanta" in output
    assert "82-83°F" in output
    assert "Apr26" in output
    assert "PARTIAL" in output
    assert "stale_below" in output
    assert "| Market |" not in output


def test_position_breakdown_markdown_keeps_yes_and_no_as_separate_rows(monkeypatch):
    wallet = "0xabc123"
    monkeypatch.setattr(report_whale_copy.settings, "watch_whales", [wallet])
    base = dict(
        local_day="2026-04-27",
        wallet_address=wallet,
        condition_id="0xcondition",
        question="Will the highest temperature in Seattle be between 60-61°F on April 27?",
        category="weather",
        resolution_time=None,
        whale_buy_fills=1,
        whale_contracts=10.0,
        whale_cost=5.0,
        order_attempts=1,
        cancelled_orders=0,
        filled_contracts=10.0,
        filled_cost=5.0,
        statuses="FILLED",
        latest_decision_code="HOLD",
        latest_decision_reason="target_met",
        latest_decision_source="fallback_poll",
        latest_latency_seconds=None,
    )
    rows = [
        SimpleNamespace(**base, outcome="YES"),
        SimpleNamespace(**base, outcome="NO"),
    ]

    output = report_whale_copy._render_position_breakdown_markdown(rows, "America/Montreal")

    assert "| 2026-04-27 | 0xabc123 | Seattle | 60-61°F | Apr27 | YES |" in output
    assert "| 2026-04-27 | 0xabc123 | Seattle | 60-61°F | Apr27 | NO |" in output


def test_sell_ignored_is_not_used_as_buy_skip_reason():
    row = SimpleNamespace(
        latest_decision_code="SELL_IGNORED",
        order_attempts=0,
        filled_contracts=0,
        question="Will the highest temperature in New York City be between 56-57°F on April 26?",
        category="weather",
        resolution_time=None,
    )

    assert report_whale_copy._fallback_reason(row) == "NO_DECISION_LOGGED"


def test_same_day_weather_no_decision_is_not_reported_past_resolution():
    row = SimpleNamespace(
        latest_decision_code=None,
        order_attempts=0,
        filled_contracts=0,
        question="Will the highest temperature in New York City be between 54-55°F on April 26?",
        category="weather",
        resolution_time=datetime(2026, 4, 26, 0, 0, tzinfo=timezone.utc),
        first_whale_ts=datetime(2026, 4, 26, 18, 16, tzinfo=timezone.utc),
        last_whale_ts=datetime(2026, 4, 26, 19, 1, tzinfo=timezone.utc),
    )

    assert report_whale_copy._fallback_reason(row) == "NO_DECISION_LOGGED"


def test_weather_after_city_cutoff_is_reported_as_city_cutoff():
    row = SimpleNamespace(
        latest_decision_code=None,
        order_attempts=0,
        filled_contracts=0,
        question="Will the highest temperature in Paris be 22°C on April 26?",
        category="weather",
        resolution_time=datetime(2026, 4, 26, 0, 0, tzinfo=timezone.utc),
        first_whale_ts=datetime(2026, 4, 26, 23, 1, tzinfo=timezone.utc),
        last_whale_ts=datetime(2026, 4, 26, 23, 1, tzinfo=timezone.utc),
    )

    assert report_whale_copy._fallback_reason(row) == "PAST_CITY_CUTOFF"


def test_batch_midpoint_response_parser_accepts_dict_and_list_shapes():
    assert _parse_midpoints_response({"token-a": "0.42", "token-b": {"mid": "0.13"}}) == {
        "token-a": 0.42,
        "token-b": 0.13,
    }
    assert _parse_midpoints_response(
        [
            {"token_id": "token-a", "mid": "0.42"},
            {"asset_id": "token-b", "price": "0.13"},
            {"token_id": "token-c", "mid": "bad"},
        ]
    ) == {"token-a": 0.42, "token-b": 0.13, "token-c": None}


@pytest.mark.asyncio
async def test_midpoints_fetches_one_live_batch(monkeypatch):
    rows = [
        SimpleNamespace(token_id="token-a", resolved=False, resolved_outcome=None),
        SimpleNamespace(token_id="token-b", resolved=False, resolved_outcome=None),
        SimpleNamespace(token_id="resolved-token", resolved=True, resolved_outcome="YES"),
    ]
    calls = []

    class FakeClob:
        async def get_midpoints(self, token_ids):
            calls.append(token_ids)
            return {"token-a": 0.42, "token-b": 0.13}

        async def close(self):
            return None

    monkeypatch.setattr(report_whale_copy, "CLOBClient", FakeClob)

    mids, fetched_at = await report_whale_copy._midpoints(rows, 5.0)

    assert calls == [["token-a", "token-b"]]
    assert mids == {"token-a": 0.42, "token-b": 0.13}
    assert fetched_at is not None


@pytest.mark.asyncio
async def test_midpoints_fail_loud_by_default_when_live_price_missing(monkeypatch):
    rows = [
        SimpleNamespace(
            token_id="token-a",
            resolved=False,
            resolved_outcome=None,
            question="Will the highest temperature in Paris be 22°C on April 27?",
            condition_id="0xcondition",
            outcome="YES",
        )
    ]

    class FakeClob:
        async def get_midpoints(self, token_ids):
            return {"token-a": None}

        async def close(self):
            return None

    monkeypatch.setattr(report_whale_copy, "CLOBClient", FakeClob)

    with pytest.raises(report_whale_copy.FreshPriceError) as exc:
        await report_whale_copy._midpoints(rows, 5.0)

    assert exc.value.code == "missing_midpoints"
    assert "Paris" in str(exc.value)


@pytest.mark.asyncio
async def test_midpoints_can_allow_missing_prices_for_diagnostics(monkeypatch):
    rows = [SimpleNamespace(token_id="token-a", resolved=False, resolved_outcome=None)]

    class FakeClob:
        async def get_midpoints(self, token_ids):
            return {"token-a": None}

        async def close(self):
            return None

    monkeypatch.setattr(report_whale_copy, "CLOBClient", FakeClob)

    mids, _ = await report_whale_copy._midpoints(rows, 5.0, allow_missing=True)

    assert mids == {"token-a": None}


@pytest.mark.asyncio
async def test_position_breakdown_mode_does_not_fetch_live_prices(monkeypatch, capsys):
    async def fake_breakdown_rows(start_utc, end_utc, tz_name):
        return []

    async def fail_midpoints(*args, **kwargs):
        raise AssertionError("position breakdown must not fetch CLOB prices")

    monkeypatch.setattr(report_whale_copy, "_fetch_position_breakdown_rows", fake_breakdown_rows)
    monkeypatch.setattr(report_whale_copy, "_midpoints", fail_midpoints)
    monkeypatch.setattr(
        "sys.argv",
        ["report_whale_copy.py", "--position-breakdown", "--format", "markdown"],
    )

    await report_whale_copy.main()

    assert "Whale Aggregate Buy Copy Breakdown" in capsys.readouterr().out
