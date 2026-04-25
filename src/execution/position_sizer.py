"""Position sizing strategies."""

import logging
import math
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
        max_size = math.floor(capital * settings.max_position_pct * 100) / 100
        return min(size, max_size)


class ConvergenceScaledSizer(PositionSizer):
    """Scale position size by convergence strength and category.

    2 whales = 1x ($100), 3 = 1.5x ($150), 4 = 2x ($200), 5+ = 2.5x ($250)
    Category multiplier: crypto_weekly 1.2x, politics 0.8x, etc.
    Cluster boost from convergence signal metadata.
    Conviction score used as multiplier (0.8-1.2x range).
    """

    CONVERGENCE_MULTIPLIERS = {
        2: 1.0,
        3: 1.5,
        4: 2.0,
    }

    def compute_size(self, capital: float, signal_confidence: int, **kwargs) -> float:
        base_size = settings.fixed_position_size_usdc

        # Convergence count multiplier
        convergence_count = kwargs.get("convergence_count", 2)
        conv_mult = self.CONVERGENCE_MULTIPLIERS.get(
            convergence_count,
            2.5 if convergence_count >= 5 else 1.0,
        )

        # Category multiplier
        category = kwargs.get("category", "")
        cat_mult = settings.get_category_multiplier(category)

        # Cluster boost from signal metadata
        cluster_boost = kwargs.get("cluster_boost", 1.0)

        # Conviction multiplier: map 0-100 to 0.8-1.2 range
        conviction = kwargs.get("conviction_score", 50)
        conv_score_mult = 0.8 + (conviction / 100) * 0.4

        size = base_size * conv_mult * cat_mult * cluster_boost * conv_score_mult

        # Cap by max_position_pct
        max_size = math.floor(capital * settings.max_position_pct * 100) / 100
        final_size = min(size, max_size)

        logger.debug(
            "ConvergenceScaledSizer: base=$%.0f × conv=%.1f × cat=%.1f × "
            "cluster=%.2f × conviction=%.2f = $%.0f (capped $%.0f)",
            base_size, conv_mult, cat_mult, cluster_boost, conv_score_mult,
            size, final_size,
        )
        return final_size


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

        max_size = math.floor(capital * settings.max_position_pct * 100) / 100
        return min(size, max_size)


def get_sizer() -> PositionSizer:
    """Get the appropriate sizer for the current phase."""
    if settings.convergence_scale_enabled:
        return ConvergenceScaledSizer()
    return FixedSizer()
