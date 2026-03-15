"""Tests for risk management config."""

from src.config import settings


class TestRiskConfig:
    def test_max_position_pct(self):
        assert settings.max_position_pct == 0.05

    def test_max_total_exposure(self):
        assert settings.max_total_exposure_pct == 0.15

    def test_max_same_category(self):
        assert settings.max_same_category_positions == 3

    def test_daily_loss_halt(self):
        assert settings.daily_loss_halt_pct == 0.05

    def test_weekly_drawdown_halt(self):
        assert settings.weekly_drawdown_halt_pct == 0.10

    def test_monthly_drawdown_halt(self):
        assert settings.monthly_drawdown_halt_pct == 0.20

    def test_strict_mode_default(self):
        assert settings.strict_mode is True

    def test_live_execution_disabled(self):
        assert settings.live_execution_enabled is False
