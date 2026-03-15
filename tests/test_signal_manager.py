"""Tests for signal management logic."""

from src.config import settings


class TestSignalConfig:
    def test_convergence_min_wallets(self):
        assert settings.convergence_min_wallets == 2

    def test_max_slippage(self):
        assert settings.max_slippage_pct == 0.10

    def test_fixed_position_size(self):
        assert settings.fixed_position_size_usdc == 100

    def test_starting_capital(self):
        assert settings.starting_capital == 15000
