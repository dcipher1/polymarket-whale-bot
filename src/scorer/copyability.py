"""Copyability classification: REJECT / WATCH / COPYABLE."""

import logging
import math
from datetime import datetime, timezone
from decimal import Decimal

from src.config import settings
from src.scorer.performance import PerformanceMetrics

logger = logging.getLogger(__name__)

VALID_CATEGORIES = list(settings.valid_categories)


def wilson_lower_bound(wins: int, total: int, confidence: float = 0.90) -> float:
    """Wilson score lower bound for binomial proportion.

    Returns the lower bound of a confidence interval for the true win rate.
    With small samples (e.g. 10 trades), this properly reflects uncertainty —
    6/10 wins gives ~0.44 lower bound, not 0.60 raw rate.
    """
    if total == 0:
        return 0.0
    z = 1.645 if confidence == 0.90 else 1.96  # 90% or 95% CI
    p_hat = wins / total
    denominator = 1 + z * z / total
    centre = p_hat + z * z / (2 * total)
    spread = z * math.sqrt((p_hat * (1 - p_hat) + z * z / (4 * total)) / total)
    return (centre - spread) / denominator


def is_category_qualifying(
    metrics: dict,
) -> bool:
    """Per-category quality gate. Used for both COPYABLE classification and
    the qualifying flag written to WalletCategoryScore at scoring time."""
    trade_count = metrics.get("trade_count", 0)
    wins = metrics.get("wins", 0)
    wilson_lb = wilson_lower_bound(wins, trade_count)

    # Recency: must have a qualifying trade within max_qualifying_trade_age_days
    last_ts = metrics.get("last_trade_ts")
    if last_ts:
        age_days = (datetime.now(timezone.utc) - last_ts).total_seconds() / 86400
        recent = age_days <= settings.max_qualifying_trade_age_days
    else:
        recent = False

    return (
        wilson_lb >= settings.min_win_rate
        and metrics.get("profit_factor", 0) >= settings.min_profit_factor
        and trade_count >= settings.min_resolved_trades
        and recent
    )


def classify_copyability(
    conviction_score: int,
    category_metrics: dict[str, dict],
    current_class: str = "REJECT",
) -> tuple[str, str | None]:
    """Classify a wallet as COPYABLE, WATCH, or REJECT.

    Classification is purely category-based. A whale qualifies based on
    per-category performance — global PnL is irrelevant because we only
    copy trades in categories where the whale has a proven edge.

    Args:
        conviction_score: 0-100 conviction score (gate: must be >= min_conviction_score)
        category_metrics: {category: {win_rate, profit_factor, trade_count, followability, wins, losses, category_pnl, last_trade_ts}}

    Returns:
        (classification, best_category)
    """

    best_category = None
    best_category_pnl = 0.0
    is_copyable = False

    for category in VALID_CATEGORIES:
        metrics = category_metrics.get(category)
        if not metrics:
            continue

        cat_pnl = metrics.get("category_pnl", 0)

        if (
            is_category_qualifying(metrics)
            and conviction_score >= settings.min_conviction_score
            and cat_pnl >= settings.get_min_category_pnl(category)
        ):
            is_copyable = True
            if cat_pnl > best_category_pnl:
                best_category = category
                best_category_pnl = cat_pnl

    if is_copyable:
        return "COPYABLE", best_category

    # WATCH: promising per-category stats but not yet qualifying
    for category in VALID_CATEGORIES:
        metrics = category_metrics.get(category)
        if not metrics:
            continue

        trade_count = metrics.get("trade_count", 0)
        wins = metrics.get("wins", 0)
        wilson_lb = wilson_lower_bound(wins, trade_count)

        if (
            wilson_lb >= 0.48
            and trade_count >= 15
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
    Conviction is used as a sizing multiplier, not a filter — so still
    included in ranking but lower weight.
    """
    # Normalize profit factor to 0-1 scale (cap at 5.0)
    pf_normalized = min(profit_factor / 5.0, 1.0)

    return (
        followability * 0.40
        + win_rate * 0.25
        + pf_normalized * 0.25
        + (conviction_score / 100) * 0.10
    )
