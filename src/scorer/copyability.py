"""Copyability classification: REJECT / WATCH / COPYABLE."""

import logging
from decimal import Decimal

from src.config import settings
from src.scorer.performance import PerformanceMetrics

logger = logging.getLogger(__name__)

VALID_CATEGORIES = ["macro", "crypto_weekly", "crypto_monthly", "politics", "geopolitics"]


def classify_copyability(
    conviction_score: int,
    total_pnl: float,
    trades_per_month: float,
    category_metrics: dict[str, dict],
) -> tuple[str, str | None]:
    """Classify a wallet as COPYABLE, WATCH, or REJECT.

    Args:
        conviction_score: 0-100 conviction score
        total_pnl: total P&L in USDC
        trades_per_month: average trades per month
        category_metrics: {category: {win_rate, profit_factor, trade_count, followability}}

    Returns:
        (classification, best_category)
    """

    # Check COPYABLE
    best_category = None
    is_copyable = False

    for category in VALID_CATEGORIES:
        metrics = category_metrics.get(category)
        if not metrics:
            continue

        min_follow = settings.get_min_followability(category)

        if (
            conviction_score >= settings.min_conviction_score
            and metrics.get("win_rate", 0) >= settings.min_win_rate
            and metrics.get("profit_factor", 0) >= settings.min_profit_factor
            and metrics.get("trade_count", 0) >= settings.min_resolved_trades
            and metrics.get("followability", 0) >= min_follow
            and total_pnl >= settings.min_wallet_pnl
            and trades_per_month <= settings.max_trades_per_month
        ):
            is_copyable = True
            best_category = category
            break

    if is_copyable:
        return "COPYABLE", best_category

    # Check WATCH
    for category in VALID_CATEGORIES:
        metrics = category_metrics.get(category)
        if not metrics:
            continue

        if (
            conviction_score >= 40
            and metrics.get("win_rate", 0) >= 0.52
            and metrics.get("trade_count", 0) >= 30
            and total_pnl >= 10_000
        ):
            return "WATCH", category

    return "REJECT", None


def compute_rank_score(
    followability: float,
    win_rate: float,
    profit_factor: float,
    conviction_score: int,
) -> float:
    """Compute ranking score for ordering COPYABLE wallets.

    Followability has 0.40 weight (largest) because a profitable wallet
    you can't follow is useless.
    """
    # Normalize profit factor to 0-1 scale (cap at 5.0)
    pf_normalized = min(profit_factor / 5.0, 1.0)

    return (
        followability * 0.40
        + win_rate * 0.20
        + pf_normalized * 0.20
        + (conviction_score / 100) * 0.20
    )
