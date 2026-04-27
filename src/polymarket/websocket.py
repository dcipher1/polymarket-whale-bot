"""Polymarket CLOB websocket adapters for market data and our user stream.

These official channels are for token orderbooks/BBO and authenticated updates
for this bot's own orders. Watched-wallet trade detection is handled by
`src.polymarket.polynode_wallet_ws`.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Awaitable, Callable

import websockets

from src.config import settings
from src.events import cache_set

logger = logging.getLogger(__name__)

MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
USER_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"

MessageHandler = Callable[[dict[str, Any]], Awaitable[None]]


class _BasePolymarketWebSocket:
    def __init__(self, url: str, name: str, on_message: MessageHandler | None = None):
        self.url = url
        self.name = name
        self._ws = None
        self._running = False
        self._reconnect_delay = 1.0
        self._on_message = on_message

    async def connect(self) -> None:
        self._running = True
        while self._running:
            try:
                async with websockets.connect(self.url, ping_interval=None, max_queue=None) as ws:
                    self._ws = ws
                    self._reconnect_delay = 1.0
                    await self._on_connect(ws)
                    heartbeat = asyncio.create_task(self._heartbeat_loop(ws))
                    try:
                        async for raw in ws:
                            await self._handle_raw(raw)
                    finally:
                        heartbeat.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(
                    "%s websocket error: %s; reconnecting in %.1fs",
                    self.name,
                    e,
                    self._reconnect_delay,
                )
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, 60.0)

    async def _on_connect(self, ws: Any) -> None:
        raise NotImplementedError

    async def _heartbeat_loop(self, ws: Any) -> None:
        while self._running:
            await asyncio.sleep(10)
            await ws.send("PING")

    async def _handle_raw(self, raw: str | bytes) -> None:
        if raw in ("PONG", b"PONG"):
            return
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.debug("Ignoring non-JSON %s websocket message: %r", self.name, raw[:80])
            return
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    await self._handle_message(item)
            return
        if isinstance(data, dict):
            await self._handle_message(data)

    async def _handle_message(self, data: dict[str, Any]) -> None:
        if self._on_message:
            await self._on_message(data)

    async def stop(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()


class MarketWebSocket(_BasePolymarketWebSocket):
    """Official Polymarket market channel.

    Subscription shape:
    {"assets_ids": ["..."], "type": "market", "custom_feature_enabled": true}
    """

    def __init__(self, on_price_update: Callable | None = None, on_message: MessageHandler | None = None):
        super().__init__(MARKET_WS_URL, "market", on_message=on_message)
        self._subscribed_tokens: set[str] = set()
        self._on_price_update = on_price_update

    async def _on_connect(self, ws: Any) -> None:
        logger.info("Polymarket market websocket connected")
        if self._subscribed_tokens:
            await self._send_subscribe(ws)

    async def _send_subscribe(self, ws: Any) -> None:
        msg = {
            "assets_ids": sorted(self._subscribed_tokens),
            "type": "market",
            "custom_feature_enabled": True,
        }
        await ws.send(json.dumps(msg))
        logger.debug("Subscribed to %d market websocket tokens", len(self._subscribed_tokens))

    async def subscribe(self, token_id: str) -> None:
        self._subscribed_tokens.add(str(token_id))
        if self._ws:
            await self._send_subscribe(self._ws)

    async def unsubscribe(self, token_id: str) -> None:
        self._subscribed_tokens.discard(str(token_id))
        if self._ws and self._subscribed_tokens:
            await self._send_subscribe(self._ws)

    async def _handle_message(self, data: dict[str, Any]) -> None:
        await super()._handle_message(data)
        event_type = data.get("event_type", data.get("type", ""))
        asset_id = data.get("asset_id", data.get("assets_id", ""))

        if event_type in ("price_change", "last_trade_price", "best_bid_ask"):
            price_data = self._parse_price(data)
            if price_data and asset_id:
                await cache_set(f"ws:price:{asset_id}", price_data, ex=60)
                if self._on_price_update:
                    await self._on_price_update(asset_id, price_data)
        elif event_type == "book" and asset_id:
            await cache_set(f"ws:book:{asset_id}", data, ex=60)

    def _parse_price(self, data: dict[str, Any]) -> dict[str, float] | None:
        try:
            price = data.get("price") or data.get("last_trade_price")
            if price is None and data.get("best_ask") is not None and data.get("best_bid") is not None:
                price = (float(data["best_ask"]) + float(data["best_bid"])) / 2
            if price is not None:
                return {"price": float(price), "timestamp": data.get("timestamp", "")}
        except (ValueError, TypeError):
            return None
        return None


class UserWebSocket(_BasePolymarketWebSocket):
    """Official authenticated user channel for this bot's own orders/fills."""

    def __init__(self, markets: list[str] | None = None, on_message: MessageHandler | None = None):
        super().__init__(USER_WS_URL, "user", on_message=on_message)
        self._markets = set(markets or [])

    async def _on_connect(self, ws: Any) -> None:
        if not (
            settings.polymarket_api_key
            and settings.polymarket_api_secret
            and settings.polymarket_api_passphrase
        ):
            raise RuntimeError("Polymarket API credentials are required for the user websocket")
        msg = {
            "auth": {
                "apiKey": settings.polymarket_api_key,
                "secret": settings.polymarket_api_secret,
                "passphrase": settings.polymarket_api_passphrase,
            },
            "markets": sorted(self._markets),
            "type": "user",
        }
        await ws.send(json.dumps(msg))
        logger.info("Polymarket user websocket connected for %d markets", len(self._markets))

    async def subscribe_market(self, condition_id: str) -> None:
        self._markets.add(condition_id)
        if self._ws:
            await self._on_connect(self._ws)
