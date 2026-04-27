"""Authenticated CLOB client wrapper for live trading.

Wraps the synchronous py_clob_client.ClobClient with async-friendly methods
and provides the interface needed by the order manager and live executor.
"""

import asyncio
import logging
from functools import lru_cache

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    MarketOrderArgs,
    OpenOrderParams,
    OrderArgs,
    OrderType,
)
from py_clob_client.constants import POLYGON

from src.config import settings

logger = logging.getLogger(__name__)

CLOB_HOST = "https://clob.polymarket.com"

# Side constants matching Polymarket CLOB expectations
SIDE_BUY = "BUY"
SIDE_SELL = "SELL"


class AuthenticatedCLOBClient:
    """Async wrapper around the synchronous ClobClient for live trading."""

    def __init__(self):
        self._client: ClobClient | None = None

    def _get_client(self) -> ClobClient:
        """Lazily initialize the authenticated ClobClient."""
        if self._client is not None:
            return self._client

        private_key = settings.polymarket_private_key
        if not private_key:
            raise RuntimeError("POLYMARKET_PRIVATE_KEY not set in .env")

        # Build credentials if API key/secret/passphrase are available
        api_key = getattr(settings, "polymarket_api_key", "")
        api_secret = getattr(settings, "polymarket_api_secret", "")
        api_passphrase = getattr(settings, "polymarket_api_passphrase", "")

        creds = None
        if api_key and api_secret and api_passphrase:
            creds = ApiCreds(
                api_key=api_key,
                api_secret=api_secret,
                api_passphrase=api_passphrase,
            )

        funder = settings.polymarket_funder_address or None

        self._client = ClobClient(
            CLOB_HOST,
            key=private_key,
            chain_id=POLYGON,
            creds=creds,
            funder=funder,
        )

        # If no stored creds, derive them now
        if creds is None:
            logger.info("No stored API creds — deriving from private key...")
            derived = self._client.create_or_derive_api_creds()
            self._client.set_api_creds(derived)
            logger.info("API credentials derived successfully")

        return self._client

    async def test_connectivity(self) -> bool:
        """Test that the CLOB client can reach the server. Returns True on success."""
        try:
            client = await asyncio.to_thread(self._get_client)
            result = await asyncio.to_thread(client.get_ok)
            return result == "OK" or bool(result)
        except Exception as e:
            logger.error("CLOB connectivity test failed: %s", e)
            return False

    async def get_balance(self, refresh: bool = False) -> float:
        """Get USDC balance (collateral). Returns balance in USDC.

        Args:
            refresh: If True, calls update_balance_allowance first to get fresh data.
                     CLOB caches balances — after trading, the cached value may be stale.
        """
        try:
            client = await asyncio.to_thread(self._get_client)
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            if refresh:
                try:
                    await asyncio.to_thread(client.update_balance_allowance, params)
                except Exception:
                    pass  # non-fatal — still try to read balance
            result = await asyncio.to_thread(client.get_balance_allowance, params)
            # CLOB returns balance as a string in atomic units (6 decimals)
            # e.g., "198294330" = $198.29 USDC
            balance = float(result.get("balance", 0)) / 1_000_000
            return balance
        except Exception as e:
            logger.error("Failed to get balance: %s", e)
            return 0.0

    async def get_allowances(self) -> dict:
        """Get USDC allowances for exchange contracts. Returns raw allowance dict."""
        try:
            client = await asyncio.to_thread(self._get_client)
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            result = await asyncio.to_thread(client.get_balance_allowance, params)
            return result.get("allowances", {})
        except Exception as e:
            logger.error("Failed to get allowances: %s", e)
            return {}

    async def place_limit_order(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
    ) -> str | None:
        """Place a GTC limit order. Returns order_id on success, None on failure.

        Args:
            token_id: The conditional token ID to trade.
            side: "BUY" or "SELL".
            price: Limit price (0-1 range for Polymarket).
            size: Number of contracts (not USDC amount).
        """
        try:
            client = await asyncio.to_thread(self._get_client)

            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side=side,
            )

            logger.debug(
                "Placing %s order: token=%s price=%.4f size=%.2f",
                side, token_id[:16], price, size,
            )

            result = await asyncio.to_thread(
                client.create_and_post_order, order_args
            )

            # Result contains order details; extract order_id
            if isinstance(result, dict):
                order_id = result.get("orderID") or result.get("order_id") or result.get("id")
                if order_id:
                    logger.debug("Order placed: %s", order_id)
                    return order_id
                # Check for errors in response
                if result.get("errorMsg") or result.get("error"):
                    error = result.get("errorMsg") or result.get("error")
                    logger.error("Order rejected: %s", error)
                    return None
                # Some responses nest the order ID
                if "orderID" not in result and "success" in result:
                    logger.warning("Order response (no ID): %s", result)
                    return None

            logger.warning("Unexpected order response: %s", result)
            return None

        except Exception as e:
            # Closed/resolved whale positions have no CLOB orderbook; log as DEBUG so the signal is
            # the failed-order warning in the sync loop, not 300+ ERROR lines on startup.
            msg = str(e)
            if "orderbook" in msg and "does not exist" in msg:
                logger.debug("Failed to place order (no orderbook): %s", e)
            else:
                logger.error("Failed to place order: %s", e)
            return None

    async def place_market_buy_fak(
        self,
        token_id: str,
        amount_usdc: float,
        max_price: float,
    ) -> dict | None:
        """Place an immediate-or-cancel style BUY using Polymarket FAK.

        For CLOB market BUY orders, ``amount`` is USDC to spend and ``price`` is
        the worst acceptable fill price. FAK takes available liquidity
        immediately and cancels anything unfilled.
        """
        try:
            client = await asyncio.to_thread(self._get_client)
            order_args = MarketOrderArgs(
                token_id=token_id,
                amount=amount_usdc,
                side=SIDE_BUY,
                price=max_price,
                order_type=OrderType.FAK,
            )

            logger.debug(
                "Placing FAK BUY: token=%s amount=$%.2f max_price=%.4f",
                token_id[:16],
                amount_usdc,
                max_price,
            )
            order = await asyncio.to_thread(client.create_market_order, order_args)
            result = await asyncio.to_thread(client.post_order, order, OrderType.FAK)
            return result if isinstance(result, dict) else {"raw": result}

        except Exception as e:
            msg = str(e)
            if "orderbook" in msg and "does not exist" in msg:
                logger.debug("Failed to place FAK BUY (no orderbook): %s", e)
            else:
                logger.error("Failed to place FAK BUY: %s", e)
            return None

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an open order. Returns True if the order is gone (freshly cancelled or
        already cancelled/matched). Returns False only if we couldn't reach CLOB."""
        try:
            client = await asyncio.to_thread(self._get_client)
            result = await asyncio.to_thread(client.cancel, order_id)
            logger.info("Cancel result for %s: %s", order_id, result)
            if not isinstance(result, dict):
                return bool(result)
            if result.get("canceled"):
                return True
            # CLOB reports orders that were already cancelled/matched as "not_canceled" with
            # a reason string. Treat those as effective success — the order is gone.
            not_canceled = result.get("not_canceled") or {}
            if any(("already canceled" in str(v).lower()) or ("matched" in str(v).lower())
                   for v in not_canceled.values()):
                return True
            return False
        except Exception as e:
            logger.error("Failed to cancel order %s: %s", order_id, e)
            return False

    async def get_order(self, order_id: str) -> dict | None:
        """Get order details by ID."""
        try:
            client = await asyncio.to_thread(self._get_client)
            result = await asyncio.to_thread(client.get_order, order_id)
            return result if isinstance(result, dict) else None
        except Exception as e:
            logger.error("Failed to get order %s: %s", order_id, e)
            return None

    async def get_open_orders(self, market: str = None) -> list[dict]:
        """Get all open orders, optionally filtered by market (condition_id)."""
        try:
            client = await asyncio.to_thread(self._get_client)
            params = OpenOrderParams(market=market) if market else OpenOrderParams()
            result = await asyncio.to_thread(client.get_orders, params)
            return result if isinstance(result, list) else []
        except Exception as e:
            logger.error("Failed to get open orders: %s", e)
            return []


# Module-level singleton
_auth_client: AuthenticatedCLOBClient | None = None


def get_auth_client() -> AuthenticatedCLOBClient:
    """Get or create the singleton authenticated CLOB client."""
    global _auth_client
    if _auth_client is None:
        _auth_client = AuthenticatedCLOBClient()
    return _auth_client
