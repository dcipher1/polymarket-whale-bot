"""Tests for convergence detection logic."""

from src.config import settings


class TestConvergenceConfig:
    def test_category_windows(self):
        assert settings.get_convergence_window("macro") == 72
        assert settings.get_convergence_window("crypto_weekly") == 36
        assert settings.get_convergence_window("politics") == 168
        assert settings.get_convergence_window("geopolitics") == 48

    def test_min_hours_to_resolution(self):
        assert settings.get_min_hours_to_resolution("macro") == 6
        assert settings.get_min_hours_to_resolution("crypto_weekly") == 24
        assert settings.get_min_hours_to_resolution("politics") == 168

    def test_followability_windows(self):
        assert settings.get_followability_window("macro") == "24h"
        assert settings.get_followability_window("crypto_weekly") == "2h"

    def test_min_followability_by_category(self):
        assert settings.get_min_followability("macro") == 0.55
        assert settings.get_min_followability("crypto_weekly") == 0.65
        assert settings.get_min_followability("geopolitics") == 0.65

    def test_default_convergence_min_wallets(self):
        assert settings.convergence_min_wallets == 2
