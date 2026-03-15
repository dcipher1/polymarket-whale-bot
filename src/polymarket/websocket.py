"""Polymarket CLOB WebSocket client for live market data."""

import asyncio
import json
import logging
from typing import Any, Callable

import websockets

from src.events import cache_set

logger = logging.getLogger(__name__)

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class MarketWebSocket:
    """WebSocket adapter for Polymarket market channel.

    Wraps subscription format in one place so message parsing is centralized.
    """

    def __init__(self, on_price_update: Callable | None = None):
        self._ws = None
        self._subscribed_tokens: set[str] = set()
        self._on_price_update = on_price_update
        self._running = False
        self._reconnect_delay = 1

    async def connect(self):
        self._running = True
        while self._running:
            try:
                async with websockets.connect(WS_URL) as ws:
                    self._ws = ws
                    self._reconnect_delay = 1
                    logger.info("WebSocket connected to %s", WS_URL)

                    # Resubscribe to all tokens
                    for token_id in self._subscribed_tokens:
                        await self._send_subscribe(ws, token_id)

                    await self._listen(ws)
            except websockets.ConnectionClosed:
                logger.warning("WebSocket connection closed, reconnecting in %ds", self._reconnect_delay)
            except Exception as e:
                logger.error("WebSocket error: %s, reconnecting in %ds", e, self._reconnect_delay)

            if self._running:
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, 60)

    async def _send_subscribe(self, ws, token_id: str):
        msg = {
            "type": "subscribe",
            "channel": "market",
            "assets_id": token_id,
        }
        await ws.send(json.dumps(msg))
        logger.debug("Subscribed to token %s", token_id)

    async def subscribe(self, token_id: str):
        self._subscribed_tokens.add(token_id)
        if self._ws:
            await self._send_subscribe(self._ws, token_id)

    async def unsubscribe(self, token_id: str):
        self._subscribed_tokens.discard(token_id)

    async def _listen(self, ws):
        async for message in ws:
            try:
                data = json.loads(message)
                await self._handle_message(data)
            except json.JSONDecodeError:
                logger.warning("Invalid JSON from WebSocket: %s", message[:200])
            except Exception as e:
                logger.error("Error handling WebSocket message: %s", e)

    async def _handle_message(self, data: dict[str, Any]):
        event_type = data.get("event_type", data.get("type", ""))
        asset_id = data.get("asset_id", data.get("assets_id", ""))

        if event_type in ("price_change", "last_trade_price"):
            price_data = self._parse_price(data)
            if price_data and asset_id:
                await cache_set(f"ws:price:{asset_id}", price_data, ex=60)
                if self._on_price_update:
                    await self._on_price_update(asset_id, price_data)

        elif event_type == "book":
            # Orderbook snapshot/delta — store for depth analysis
            if asset_id:
                await cache_set(f"ws:book:{asset_id}", data, ex=60)

    def _parse_price(self, data: dict[str, Any]) -> dict[str, float] | None:
        try:
            price = data.get("price", data.get("last_trade_price"))
            if price is not None:
                price = float(price)
                return {"price": price, "timestamp": data.get("timestamp", "")}
        except (ValueError, TypeError):
            pass
        return None

    async def stop(self):
        self._running = False
        if self._ws:
            await self._ws.close()
