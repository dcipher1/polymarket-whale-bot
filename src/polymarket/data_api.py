"""Polymarket Data API client for wallet trade/position data."""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
from pydantic import BaseModel, Field

from src.config import settings
from src.events import cache_get, cache_set

logger = logging.getLogger(__name__)

BASE_URL = "https://data-api.polymarket.com"
API_RESPONSES_DIR = Path("data/api_responses")
RATE_LIMIT_DELAY = 0.2  # 200ms between calls


class _CoerceStr:
    """Pydantic config that coerces all values to strings where expected."""


class Position(BaseModel):
    asset: str = ""
    market: str = ""
    condition_id: str = Field(default="", alias="conditionId")
    token_id: str = Field(default="", alias="tokenId")
    side: str = ""
    size: str = "0"
    avg_price: str = Field(default="0", alias="avgPrice")
    current_value: str = Field(default="0", alias="currentValue")
    initial_value: str = Field(default="0", alias="initialValue")
    pnl: str = Field(default="0")
    realized_pnl: str = Field(default="0", alias="realizedPnl")
    cur_price: str = Field(default="0", alias="curPrice")
    cashflow: str = Field(default="0")
    title: str = ""
    slug: str = ""
    icon: str = ""
    outcome: str = ""
    opposite_outcome: str = Field(default="", alias="oppositeOutcome")
    end_date: str = Field(default="", alias="endDate")
    neg_risk: bool = Field(default=False, alias="negRisk")
    proxyWallet: str = ""

    model_config = {"populate_by_name": True, "coerce_numbers_to_str": True}


class Activity(BaseModel):
    id: str = ""
    type: str = ""
    condition_id: str = Field(default="", alias="conditionId")
    token_id: str = Field(default="", alias="tokenId")
    side: str = ""
    size: str = "0"
    price: str = Field(default="0")
    value: str = Field(default="0")
    timestamp: str = ""
    title: str = ""
    slug: str = ""
    icon: str = ""
    outcome: str = ""
    tx_hash: str = Field(default="", alias="transactionHash")
    market: str = ""

    model_config = {"populate_by_name": True, "coerce_numbers_to_str": True}


class Trade(BaseModel):
    id: str = ""
    taker_order_id: str = Field(default="", alias="takerOrderId")
    market: str = ""
    asset_id: str = Field(default="", alias="assetId")
    asset: str = ""  # token ID (from /trades endpoint)
    condition_id: str = Field(default="", alias="conditionId")
    side: str = ""
    size: str = "0"
    price: str = "0"
    status: str = ""
    match_time: str = Field(default="", alias="matchTime")
    last_update: str = Field(default="", alias="lastUpdate")
    timestamp: str = ""  # unix epoch from /trades
    outcome: str = ""
    outcome_index: int = Field(default=0, alias="outcomeIndex")
    maker_address: str = Field(default="", alias="makerAddress")
    trader_side: str = Field(default="", alias="traderSide")
    fee_rate_bps: str = Field(default="0", alias="feeRateBps")
    transaction_hash: str = Field(default="", alias="transactionHash")
    bucket_index: int = Field(default=0, alias="bucketIndex")
    type: str = ""
    owner: str = ""
    title: str = ""
    slug: str = ""
    proxy_wallet: str = Field(default="", alias="proxyWallet")
    name: str = ""

    model_config = {"populate_by_name": True, "coerce_numbers_to_str": True}


class DataAPIClient:
    def __init__(self):
        self._session: aiohttp.ClientSession | None = None
        self._logged_endpoints: set[str] = set()
        self._trades_param: str | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _request(
        self, endpoint: str, params: dict[str, Any] | None = None
    ) -> Any:
        session = await self._get_session()
        url = f"{BASE_URL}/{endpoint}"

        for attempt in range(3):
            try:
                await asyncio.sleep(RATE_LIMIT_DELAY)
                async with session.get(url, params=params) as resp:
                    if resp.status == 429:
                        wait = 2 ** (attempt + 1)
                        logger.warning("Rate limited on %s, waiting %ds", endpoint, wait)
                        await asyncio.sleep(wait)
                        continue
                    resp.raise_for_status()
                    data = await resp.json()

                    # Log raw response on first successful call per endpoint
                    if endpoint not in self._logged_endpoints:
                        self._logged_endpoints.add(endpoint)
                        self._log_raw_response(endpoint, data)

                    return data
            except aiohttp.ClientError as e:
                if attempt == 2:
                    logger.error("Failed to fetch %s after 3 attempts: %s", endpoint, e)
                    raise
                await asyncio.sleep(2 ** attempt)

    def _log_raw_response(self, endpoint: str, data: Any) -> None:
        API_RESPONSES_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        safe_name = endpoint.replace("/", "_").replace("?", "_")
        path = API_RESPONSES_DIR / f"{safe_name}_{ts}.json"
        try:
            with open(path, "w") as f:
                json.dump(data, f, indent=2, default=str)
            logger.info("Logged raw API response for %s to %s", endpoint, path)
        except Exception as e:
            logger.warning("Failed to log raw response for %s: %s", endpoint, e)

    async def get_positions(self, wallet_address: str) -> list[Position]:
        data = await self._request("positions", {"user": wallet_address})
        if not isinstance(data, list):
            data = data.get("positions", data.get("data", []))
        return [Position.model_validate(item) for item in data]

    async def get_closed_positions(self, wallet_address: str) -> list[Position]:
        data = await self._request("closed-positions", {"user": wallet_address})
        if not isinstance(data, list):
            data = data.get("positions", data.get("data", []))
        return [Position.model_validate(item) for item in data]

    async def get_activity(
        self, wallet_address: str, limit: int = 100, offset: int = 0
    ) -> list[Activity]:
        data = await self._request(
            "activity", {"user": wallet_address, "limit": limit, "offset": offset}
        )
        if not isinstance(data, list):
            data = data.get("activity", data.get("data", []))
        return [Activity.model_validate(item) for item in data]

    async def get_all_activity(self, wallet_address: str, max_pages: int = 30) -> list[Activity]:
        all_activity = []
        offset = 0
        for _ in range(max_pages):
            try:
                batch = await self.get_activity(wallet_address, limit=100, offset=offset)
            except Exception:
                break  # API returns 400 when offset exceeds available data
            if not batch:
                break
            all_activity.extend(batch)
            if len(batch) < 100:
                break
            offset += 100
        return all_activity

    async def _discover_trades_param(self, wallet_address: str) -> str:
        """Discover which parameter name the /trades endpoint accepts."""
        cached = await cache_get("data_api:trades_param")
        if cached:
            self._trades_param = cached
            return cached

        for param_name in ["user", "address", "maker_address"]:
            try:
                session = await self._get_session()
                await asyncio.sleep(RATE_LIMIT_DELAY)
                async with session.get(
                    f"{BASE_URL}/trades",
                    params={param_name: wallet_address, "limit": 1},
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        items = data if isinstance(data, list) else data.get("trades", data.get("data", []))
                        if items:
                            logger.info("Trades endpoint works with param '%s'", param_name)
                            self._trades_param = param_name
                            await cache_set("data_api:trades_param", param_name)
                            return param_name
            except Exception as e:
                logger.debug("Trades param '%s' failed: %s", param_name, e)

        raise RuntimeError(
            "Could not discover working parameter for /trades endpoint. "
            "Tried: user, address, maker_address. Check API docs."
        )

    async def get_trades(
        self, wallet_address: str, limit: int = 100, offset: int = 0
    ) -> list[Trade]:
        if self._trades_param is None:
            await self._discover_trades_param(wallet_address)

        data = await self._request(
            "trades",
            {self._trades_param: wallet_address, "limit": limit, "offset": offset},
        )
        if not isinstance(data, list):
            data = data.get("trades", data.get("data", []))
        return [Trade.model_validate(item) for item in data]

    async def get_all_trades(self, wallet_address: str, max_pages: int = 30) -> list[Trade]:
        all_trades = []
        offset = 0
        for _ in range(max_pages):
            try:
                batch = await self.get_trades(wallet_address, limit=100, offset=offset)
            except Exception:
                break  # API returns 400 when offset exceeds available data
            if not batch:
                break
            all_trades.extend(batch)
            if len(batch) < 100:
                break
            offset += 100
        return all_trades
