"""Order manager stub for Phase 3+ live execution."""

import logging

logger = logging.getLogger(__name__)


async def place_order(
    token_id: str,
    side: str,
    price: float,
    size: float,
) -> str | None:
    """Place an order on Polymarket CLOB. Stub for Phase 3+."""
    logger.warning("Live execution not implemented — Phase 3+ required")
    return None


async def cancel_order(order_id: str) -> bool:
    """Cancel an open order. Stub for Phase 3+."""
    logger.warning("Live execution not implemented — Phase 3+ required")
    return False
