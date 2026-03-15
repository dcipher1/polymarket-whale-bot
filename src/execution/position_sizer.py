"""Position sizing strategies."""

import logging
from abc import ABC, abstractmethod

from src.config import settings

logger = logging.getLogger(__name__)


class PositionSizer(ABC):
    @abstractmethod
    def compute_size(self, capital: float, signal_confidence: int, **kwargs) -> float:
        """Compute position size in USDC."""
        ...


class FixedSizer(PositionSizer):
    """Fixed dollar amount per trade (Phases 1-4)."""

    def compute_size(self, capital: float, signal_confidence: int, **kwargs) -> float:
        size = settings.fixed_position_size_usdc
        max_size = capital * settings.max_position_pct
        return min(size, max_size)


class KellySizer(PositionSizer):
    """Kelly criterion sizing (Phase 5+ only, after 50+ live trades)."""

    def compute_size(self, capital: float, signal_confidence: int, **kwargs) -> float:
        win_rate = kwargs.get("win_rate", 0.55)
        avg_win = kwargs.get("avg_win", 1.0)
        avg_loss = kwargs.get("avg_loss", 1.0)

        if avg_loss == 0:
            return settings.fixed_position_size_usdc

        b = avg_win / avg_loss  # gain/loss ratio
        p = win_rate
        q = 1 - p

        kelly_fraction = (b * p - q) / b
        kelly_fraction = max(0, kelly_fraction)

        # Half-Kelly for safety
        half_kelly = kelly_fraction / 2
        size = capital * half_kelly

        max_size = capital * settings.max_position_pct
        return min(size, max_size)


def get_sizer() -> PositionSizer:
    """Get the appropriate sizer for the current phase."""
    # Always fixed for now; Kelly only in Phase 5+
    return FixedSizer()
