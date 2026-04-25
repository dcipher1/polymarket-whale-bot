"""Tests for wallet scoring components."""

from src.scorer.behavior import (
    _hold_duration_score,
    _resolution_holding_score,
)
from src.scorer.copyability import classify_copyability, compute_rank_score


class TestHoldDurationScore:
    def test_long_hold(self):
        assert _hold_duration_score(200) == 100

    def test_medium_hold(self):
        score = _hold_duration_score(100)
        assert score == 100

    def test_short_hold(self):
        score = _hold_duration_score(30)
        assert 50 < score < 60

    def test_daytrader(self):
        score = _hold_duration_score(12)
        assert 20 < score < 40


class TestResolutionHoldingScore:
    def test_holds_to_resolution(self):
        assert _resolution_holding_score(0.90) == 100

    def test_partial_hold(self):
        score = _resolution_holding_score(0.65)
        assert 20 < score < 100

    def test_early_exit(self):
        assert _resolution_holding_score(0.30) == 20


class TestCopyabilityClassification:
    def test_copyable(self, sample_category_metrics):
        result, best_cat = classify_copyability(
            conviction_score=72,
            category_metrics=sample_category_metrics,
        )
        assert result == "COPYABLE"
        assert best_cat == "weather"

    def test_watch(self):
        metrics = {
            "weather": {
                "win_rate": 0.66,
                "profit_factor": 1.2,
                "trade_count": 35,
                "followability": 0.40,
                "wins": 23,
                "losses": 12,
                "category_pnl": 5000,
            }
        }
        result, _ = classify_copyability(
            conviction_score=45,
            category_metrics=metrics,
        )
        assert result == "WATCH"

    def test_watch_low_conviction_high_stats(self):
        metrics = {
            "weather": {
                "win_rate": 0.80,
                "profit_factor": 3.0,
                "trade_count": 60,
                "followability": 0.70,
                "wins": 48,
                "losses": 12,
                "category_pnl": 5000,
            }
        }
        result, _ = classify_copyability(
            conviction_score=30,
            category_metrics=metrics,
        )
        assert result == "WATCH"

    def test_reject_no_categories(self):
        result, _ = classify_copyability(
            conviction_score=80,
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
