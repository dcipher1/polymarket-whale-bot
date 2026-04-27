"""Order manager — routes orders through the authenticated CLOB client."""

import logging

from src.polymarket.clob_auth import get_auth_client

logger = logging.getLogger(__name__)

MIN_ORDER_PRICE = 0.001
MAX_ORDER_PRICE = 0.999
PRICE_TICK = 0.001


def quantize_order_price(price: float, side: str) -> float:
    """Round to CLOB price tick without crossing the caller's limit."""
    side = side.upper()
    if side == "BUY":
        return int(price / PRICE_TICK + 1e-9) * PRICE_TICK
    if side == "SELL":
        return (int(price / PRICE_TICK - 1e-9) + 1) * PRICE_TICK
    return price


async def place_order(
    token_id: str,
    side: str,
    price: float,
    size: float,
    tag: str | None = None,
) -> str | None:
    """Place a limit order on Polymarket CLOB.

    Args:
        token_id: Conditional token ID.
        side: "BUY" or "SELL".
        price: Limit price (0-1).
        size: Number of contracts.
        tag: Optional human-readable market label for log lines (e.g. "NYC 70-71 NO Apr23").

    Returns:
        Order ID string on success, None on failure.
    """
    if not token_id:
        logger.error("place_order called with empty token_id")
        return None

    if price < MIN_ORDER_PRICE or price > MAX_ORDER_PRICE:
        logger.error(
            "Invalid price %.6f — must be between %.3f and %.3f",
            price, MIN_ORDER_PRICE, MAX_ORDER_PRICE,
        )
        return None

    if size <= 0:
        logger.error("Invalid size %.2f — must be positive", size)
        return None

    side = side.upper()
    if side not in ("BUY", "SELL"):
        logger.error("Invalid side %r — must be BUY or SELL", side)
        return None

    # Round to CLOB tick. Floor BUY so we never exceed the requested limit;
    # ceil SELL symmetrically.
    price = quantize_order_price(price, side)
    if price < MIN_ORDER_PRICE or price > MAX_ORDER_PRICE:
        logger.error(
            "Invalid tick-rounded price %.6f — must be between %.3f and %.3f",
            price, MIN_ORDER_PRICE, MAX_ORDER_PRICE,
        )
        return None

    label = tag or token_id[:16]

    try:
        client = get_auth_client()
        order_id = await client.place_limit_order(
            token_id=token_id,
            side=side,
            price=price,
            size=size,
        )
        if order_id:
            logger.info(
                "Order placed: %s %s %.2f contracts @ %.4f",
                side, label, size, price,
            )
        else:
            logger.warning(
                "Order placement returned no ID: %s %s %.2f @ %.4f",
                side, label, size, price,
            )
        return order_id

    except Exception as e:
        logger.error(
            "Order placement failed: %s %s %.2f @ %.4f — %s",
            side, label, size, price, e,
        )
        return None


async def cancel_order(order_id: str, tag: str | None = None) -> bool:
    """Cancel an open order.

    Args:
        order_id: CLOB order ID to cancel.
        tag: Optional human-readable market label for log lines.

    Returns:
        True if cancellation succeeded, False otherwise.
    """
    if not order_id:
        logger.error("cancel_order called with empty order_id")
        return False

    label = tag or order_id[:16]

    try:
        client = get_auth_client()
        success = await client.cancel_order(order_id)
        if success:
            logger.debug("Order cancelled: %s (%s)", label, order_id[:16])
        else:
            logger.warning("Order cancellation returned False: %s (%s)", label, order_id[:16])
        return success

    except Exception as e:
        logger.error("Order cancellation failed: %s (%s) — %s", label, order_id[:16], e)
        return False


async def get_order_status(order_id: str) -> dict | None:
    """Get the current status of an order.

    Returns:
        Order details dict or None on failure.
    """
    if not order_id:
        return None

    try:
        client = get_auth_client()
        return await client.get_order(order_id)
    except Exception as e:
        logger.error("Failed to get order status for %s: %s", order_id, e)
        return None
