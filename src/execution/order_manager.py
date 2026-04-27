"""Order manager — routes orders through the authenticated CLOB client."""

import asyncio
from dataclasses import dataclass
import logging

from src.polymarket.clob_auth import get_auth_client

logger = logging.getLogger(__name__)

MIN_ORDER_PRICE = 0.001
MAX_ORDER_PRICE = 0.999
PRICE_TICK = 0.001


@dataclass
class TakerOrderResult:
    order_id: str | None
    accepted: bool
    status: str | None
    requested_usdc: float
    max_price: float
    filled_contracts: int
    avg_fill_price: float | None
    raw_response: dict | None
    raw_status: dict | None
    error: str | None = None


def quantize_order_price(price: float, side: str) -> float:
    """Round to CLOB price tick without crossing the caller's limit."""
    side = side.upper()
    if side == "BUY":
        return int(price / PRICE_TICK + 1e-9) * PRICE_TICK
    if side == "SELL":
        return (int(price / PRICE_TICK - 1e-9) + 1) * PRICE_TICK
    return price


def _extract_order_id(payload: dict | None) -> str | None:
    if not isinstance(payload, dict):
        return None
    return (
        payload.get("orderID")
        or payload.get("order_id")
        or payload.get("id")
        or payload.get("orderHash")
        or payload.get("hash")
    )


def _extract_error(payload: dict | None) -> str | None:
    if not isinstance(payload, dict):
        return None
    error = payload.get("errorMsg") or payload.get("error") or payload.get("message")
    return str(error) if error else None


def _num(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _extract_filled_contracts(*payloads: dict | None) -> int:
    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        for key in (
            "size_matched",
            "matched_size",
            "filled_size",
            "filledSize",
            "matchedAmount",
        ):
            value = _num(payload.get(key))
            if value > 0:
                return int(value)
    return 0


def _extract_avg_fill_price(status: dict | None, fallback_price: float) -> float | None:
    if not isinstance(status, dict):
        return None
    explicit = _num(status.get("average_price") or status.get("avg_price") or status.get("price"))
    if explicit > 0:
        return explicit
    matched = _num(status.get("size_matched"))
    original_size = _num(status.get("original_size") or status.get("size"))
    if matched > 0 and original_size > 0:
        return fallback_price
    return None


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


async def place_taker_buy(
    token_id: str,
    amount_usdc: float,
    max_price: float,
    tag: str | None = None,
) -> TakerOrderResult:
    """Place a real FAK taker BUY.

    ``amount_usdc`` is the maximum USDC to spend. ``max_price`` is the worst
    acceptable fill price.
    """
    label = tag or token_id[:16]

    def fail(error: str) -> TakerOrderResult:
        return TakerOrderResult(
            order_id=None,
            accepted=False,
            status=None,
            requested_usdc=amount_usdc,
            max_price=max_price,
            filled_contracts=0,
            avg_fill_price=None,
            raw_response=None,
            raw_status=None,
            error=error,
        )

    if not token_id:
        logger.error("place_taker_buy called with empty token_id")
        return fail("empty_token_id")
    if amount_usdc <= 0:
        logger.error("Invalid FAK amount %.6f — must be positive", amount_usdc)
        return fail("invalid_amount")
    if max_price < MIN_ORDER_PRICE or max_price > MAX_ORDER_PRICE:
        logger.error(
            "Invalid FAK max price %.6f — must be between %.3f and %.3f",
            max_price, MIN_ORDER_PRICE, MAX_ORDER_PRICE,
        )
        return fail("invalid_price")

    max_price = quantize_order_price(max_price, "BUY")
    if max_price < MIN_ORDER_PRICE or max_price > MAX_ORDER_PRICE:
        logger.error(
            "Invalid tick-rounded FAK max price %.6f — must be between %.3f and %.3f",
            max_price, MIN_ORDER_PRICE, MAX_ORDER_PRICE,
        )
        return fail("invalid_price")

    try:
        client = get_auth_client()
        response = await client.place_market_buy_fak(
            token_id=token_id,
            amount_usdc=amount_usdc,
            max_price=max_price,
        )
        if not response:
            logger.warning(
                "FAK BUY placement returned no response: %s $%.2f max %.4f",
                label, amount_usdc, max_price,
            )
            return fail("place_failed")

        error = _extract_error(response)
        order_id = _extract_order_id(response)
        if error:
            logger.warning(
                "FAK BUY rejected: %s $%.2f max %.4f — %s",
                label, amount_usdc, max_price, error,
            )
            return TakerOrderResult(
                order_id=order_id,
                accepted=False,
                status=str(response.get("status")) if isinstance(response, dict) and response.get("status") else None,
                requested_usdc=amount_usdc,
                max_price=max_price,
                filled_contracts=0,
                avg_fill_price=None,
                raw_response=response,
                raw_status=None,
                error=error,
            )

        status = None
        if order_id:
            for attempt in range(3):
                status = await client.get_order(order_id)
                if status:
                    break
                if attempt < 2:
                    await asyncio.sleep(0.2)
        status_text = None
        if isinstance(status, dict):
            status_text = str(status.get("status") or "").upper() or None
        if not status_text and isinstance(response, dict) and response.get("status"):
            status_text = str(response.get("status")).upper()

        filled_contracts = _extract_filled_contracts(status, response)
        avg_fill_price = _extract_avg_fill_price(status, max_price)
        if filled_contracts > 0:
            logger.info(
                "FAK BUY filled: %s %d contracts, $%.2f max %.4f status=%s",
                label, filled_contracts, amount_usdc, max_price, status_text or "unknown",
            )
        else:
            logger.warning(
                "FAK BUY no fill: %s $%.2f max %.4f status=%s order=%s",
                label, amount_usdc, max_price, status_text or "unknown", (order_id or "")[:16],
            )

        return TakerOrderResult(
            order_id=order_id,
            accepted=bool(order_id or response.get("success", False)),
            status=status_text,
            requested_usdc=amount_usdc,
            max_price=max_price,
            filled_contracts=filled_contracts,
            avg_fill_price=avg_fill_price,
            raw_response=response,
            raw_status=status,
            error=None,
        )

    except Exception as e:
        logger.error(
            "FAK BUY placement failed: %s $%.2f max %.4f — %s",
            label, amount_usdc, max_price, e,
        )
        return fail(str(e))


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
