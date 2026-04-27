from types import SimpleNamespace
import importlib.util
from pathlib import Path


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

    assert "| Market | Side | Whale buy fills | Whale contracts | Whale cost | Bot attempts | Bot filled | Unfilled gap | Copy % | Status | Reason | Source |" in output
    assert "Atlanta 82-83°F Apr26?" in output
    assert "| 2026-04-26 | 0xabc123 | 1 | 100 | 45 | 55 | 0 | 1 | 0 | 0 | 0 |" in output
    assert "| Atlanta 82-83°F Apr26? | YES | 4 | 100 | $42.00 | 2 | 45 | 55 | 45% | PARTIAL | stale_below | fallback_poll |" in output


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
    assert "Whale 0xabc123" in output
    assert "Atlanta 82-83°F Apr26?" in output
    assert "PARTIAL" in output
    assert "stale_below" in output
    assert "| Market |" not in output


def test_sell_ignored_is_not_used_as_buy_skip_reason():
    row = SimpleNamespace(
        latest_decision_code="SELL_IGNORED",
        order_attempts=0,
        filled_contracts=0,
        question="Will the highest temperature in New York City be between 56-57°F on April 26?",
        category="weather",
        resolution_time=None,
    )

    assert report_whale_copy._fallback_reason(row) == "no order logged"
