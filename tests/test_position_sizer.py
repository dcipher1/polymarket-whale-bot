"""Tests for position sizing."""

from src.execution.position_sizer import FixedSizer, KellySizer, ConvergenceScaledSizer, get_sizer
from src.config import settings


class TestFixedSizer:
    def test_returns_fixed_amount(self):
        sizer = FixedSizer()
        size = sizer.compute_size(capital=15000, signal_confidence=80)
        assert size == settings.fixed_position_size_usdc

    def test_caps_at_max_position_pct(self):
        sizer = FixedSizer()
        size = sizer.compute_size(capital=500, signal_confidence=80)
        assert size <= 500 * settings.max_position_pct


class TestKellySizer:
    def test_positive_edge(self):
        sizer = KellySizer()
        size = sizer.compute_size(
            capital=15000, signal_confidence=80,
            win_rate=0.60, avg_win=100, avg_loss=80,
        )
        assert size > 0
        assert size <= 15000 * settings.max_position_pct

    def test_no_edge(self):
        sizer = KellySizer()
        size = sizer.compute_size(
            capital=15000, signal_confidence=50,
            win_rate=0.40, avg_win=50, avg_loss=100,
        )
        assert size == 0  # negative edge = no bet

    def test_half_kelly(self):
        sizer = KellySizer()
        # With clear edge, should return positive but conservative size
        size = sizer.compute_size(
            capital=15000, signal_confidence=90,
            win_rate=0.65, avg_win=100, avg_loss=80,
        )
        assert 0 < size < 15000


class TestSizerFactory:
    def test_default_sizer(self):
        sizer = get_sizer()
        if settings.convergence_scale_enabled:
            assert isinstance(sizer, ConvergenceScaledSizer)
        else:
            assert isinstance(sizer, FixedSizer)
