"""Tests for wallet scoring components."""

from src.scorer.behavior import (
    _frequency_score,
    _hold_duration_score,
    _resolution_holding_score,
    _concentration_score,
)
from src.scorer.copyability import classify_copyability, compute_rank_score


class TestFrequencyScore:
    def test_low_frequency(self):
        assert _frequency_score(10) == 100

    def test_medium_frequency(self):
        score = _frequency_score(35)
        assert 70 < score < 100

    def test_high_frequency(self):
        score = _frequency_score(75)
        assert 30 < score < 70

    def test_bot_frequency(self):
        assert _frequency_score(150) == 0


class TestHoldDurationScore:
    def test_long_hold(self):
        assert _hold_duration_score(200) == 100

    def test_medium_hold(self):
        score = _hold_duration_score(100)
        assert 40 < score < 100

    def test_short_hold(self):
        assert _hold_duration_score(30) == 40

    def test_daytrader(self):
        assert _hold_duration_score(12) == 0


class TestResolutionHoldingScore:
    def test_holds_to_resolution(self):
        assert _resolution_holding_score(0.90) == 100

    def test_partial_hold(self):
        score = _resolution_holding_score(0.65)
        assert 20 < score < 100

    def test_early_exit(self):
        assert _resolution_holding_score(0.30) == 20


class TestConcentrationScore:
    def test_high_concentration(self):
        assert _concentration_score(0.08) == 100

    def test_medium_concentration(self):
        score = _concentration_score(0.035)
        assert 0 < score < 100

    def test_low_concentration(self):
        assert _concentration_score(0.005) == 20


class TestCopyabilityClassification:
    def test_copyable(self, sample_category_metrics):
        result, best_cat = classify_copyability(
            conviction_score=72,
            total_pnl=75000,
            trades_per_month=15,
            category_metrics=sample_category_metrics,
        )
        assert result == "COPYABLE"
        assert best_cat == "macro"

    def test_watch(self):
        metrics = {
            "macro": {
                "win_rate": 0.53,
                "profit_factor": 1.2,
                "trade_count": 35,
                "followability": 0.40,
            }
        }
        result, _ = classify_copyability(
            conviction_score=45,
            total_pnl=15000,
            trades_per_month=25,
            category_metrics=metrics,
        )
        assert result == "WATCH"

    def test_reject_low_conviction(self):
        metrics = {
            "macro": {
                "win_rate": 0.60,
                "profit_factor": 2.0,
                "trade_count": 60,
                "followability": 0.70,
            }
        }
        result, _ = classify_copyability(
            conviction_score=30,
            total_pnl=5000,
            trades_per_month=200,
            category_metrics=metrics,
        )
        assert result == "REJECT"

    def test_reject_no_categories(self):
        result, _ = classify_copyability(
            conviction_score=80,
            total_pnl=100000,
            trades_per_month=10,
            category_metrics={},
        )
        assert result == "REJECT"


class TestRankScore:
    def test_high_followability_ranks_higher(self):
        high_follow = compute_rank_score(
            followability=0.85, win_rate=0.55, profit_factor=1.8, conviction_score=65
        )
        low_follow = compute_rank_score(
            followability=0.50, win_rate=0.65, profit_factor=2.5, conviction_score=80
        )
        assert high_follow > low_follow
