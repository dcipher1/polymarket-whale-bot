"""Polymarket Data API client for wallet trade/position data."""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone, timedelta
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
    cash_pnl: str = Field(default="0", alias="cashPnl")
    percent_pnl: str = Field(default="0", alias="percentPnl")
    realized_pnl: str = Field(default="0", alias="realizedPnl")
    percent_realized_pnl: str = Field(default="0", alias="percentRealizedPnl")
    total_bought: str = Field(default="0", alias="totalBought")
    cur_price: str = Field(default="0", alias="curPrice")
    cashflow: str = Field(default="0")
    title: str = ""
    slug: str = ""
    icon: str = ""
    outcome: str = ""
    opposite_outcome: str = Field(default="", alias="oppositeOutcome")
    end_date: str = Field(default="", alias="endDate")
    event_id: str = Field(default="", alias="eventId")
    event_slug: str = Field(default="", alias="eventSlug")
    neg_risk: bool = Field(default=False, alias="negativeRisk")
    redeemable: bool = Field(default=False)
    mergeable: bool = Field(default=False)
    proxyWallet: str = ""

    model_config = {"populate_by_name": True, "coerce_numbers_to_str": True}

    @property
    def cur_price_float(self) -> float | None:
        """Return cur_price as float, or None when the API returns '0' (no price available)."""
        cp = float(self.cur_price) if self.cur_price else 0.0
        return cp if cp > 0 else None


class Activity(BaseModel):
    id: str = ""
    type: str = ""
    condition_id: str = Field(default="", alias="conditionId")
    token_id: str = Field(default="", alias="tokenId")
    asset: str = ""  # token ID from activity endpoint
    side: str = ""
    size: str = "0"
    price: str = Field(default="0")
    value: str = Field(default="0")
    timestamp: str = ""
    title: str = ""
    slug: str = ""
    icon: str = ""
    outcome: str = ""
    outcome_index: int = Field(default=0, alias="outcomeIndex")
    transaction_hash: str = Field(default="", alias="transactionHash")
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

                    # Only log raw responses at DEBUG level
                    if settings.log_level == "DEBUG" and endpoint not in self._logged_endpoints:
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
            logger.debug("Logged raw API response for %s to %s", endpoint, path)
        except Exception as e:
            logger.warning("Failed to log raw response for %s: %s", endpoint, e)

    async def get_user_pnl_series(
        self,
        user_address: str,
        interval: str = "all",
        fidelity: str = "1h",
    ) -> list[dict[str, float]]:
        """Authoritative windowed PnL from user-pnl-api (the source Polymarket's UI uses).

        Returns list of {"t": epoch_seconds, "p": cumulative_pnl}. The last sample
        is current cumulative PnL; first sample is PnL at the window's start.
        Windowed delta = last["p"] - first["p"].
        """
        session = await self._get_session()
        url = "https://user-pnl-api.polymarket.com/user-pnl"
        params = {"user_address": user_address, "interval": interval, "fidelity": fidelity}
        for attempt in range(3):
            try:
                await asyncio.sleep(RATE_LIMIT_DELAY)
                async with session.get(url, params=params) as resp:
                    if resp.status == 429:
                        await asyncio.sleep(2 ** (attempt + 1))
                        continue
                    resp.raise_for_status()
                    data = await resp.json()
                    return data if isinstance(data, list) else []
            except aiohttp.ClientError as e:
                if attempt == 2:
                    logger.error("user-pnl fetch failed for %s: %s", user_address[:10], e)
                    return []
                await asyncio.sleep(2 ** attempt)
        return []

    async def get_positions(self, wallet_address: str) -> list[Position]:
        """Fetch all open positions for a wallet, paginating through the limit=500 cap."""
        all_positions: list[Position] = []
        offset = 0
        for _ in range(20):  # max 20 pages × 500 = 10,000 positions
            data = await self._request(
                "positions",
                {"user": wallet_address, "limit": 500, "offset": offset},
            )
            if not isinstance(data, list):
                data = data.get("positions", data.get("data", []))
            if not data:
                break
            all_positions.extend(Position.model_validate(item) for item in data)
            if len(data) < 500:
                break
            offset += 500
        return all_positions

    async def get_closed_positions(self, wallet_address: str) -> list[Position]:
        all_positions = []
        offset = 0
        for _ in range(20):  # max 20 pages × 50 = 1000 positions
            data = await self._request(
                "closed-positions",
                {"user": wallet_address, "limit": 50, "offset": offset},
            )
            if not isinstance(data, list):
                data = data.get("positions", data.get("data", []))
            if not data:
                break
            all_positions.extend(Position.model_validate(item) for item in data)
            if len(data) < 50:
                break
            offset += 50
        return all_positions

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
        self,
        wallet_address: str,
        limit: int = 100,
        offset: int = 0,
        since: datetime | None = None,
    ) -> list[Trade]:
        if self._trades_param is None:
            await self._discover_trades_param(wallet_address)

        params = {self._trades_param: wallet_address, "limit": limit, "offset": offset}
        if since:
            # Pass as unix timestamp — API may support filtering by time
            params["since"] = str(int(since.timestamp()))

        data = await self._request("trades", params)
        if not isinstance(data, list):
            data = data.get("trades", data.get("data", []))
        return [Trade.model_validate(item) for item in data]

    async def get_all_trades(
        self,
        wallet_address: str,
        max_pages: int = 30,
        since: datetime | None = None,
    ) -> list[Trade]:
        all_trades = []
        offset = 0
        for _ in range(max_pages):
            try:
                batch = await self.get_trades(
                    wallet_address, limit=100, offset=offset, since=since,
                )
            except Exception:
                break  # API returns 400 when offset exceeds available data
            if not batch:
                break

            # Client-side early termination: API returns newest-first but
            # ignores the `since` param. Stop when we hit old trades.
            if since:
                filtered = []
                hit_old = False
                for trade in batch:
                    ts = self._parse_trade_ts(trade)
                    if ts and ts <= since:
                        hit_old = True
                    else:
                        filtered.append(trade)
                all_trades.extend(filtered)
                if hit_old:
                    break
            else:
                all_trades.extend(batch)

            if len(batch) < 100:
                break
            offset += 100
        return all_trades

    @staticmethod
    def _parse_trade_ts(trade: Trade) -> datetime | None:
        """Parse timestamp from a Trade for early-termination comparison."""
        ts_str = trade.timestamp or trade.match_time or trade.last_update
        if not ts_str:
            return None
        try:
            ts_val = int(float(ts_str))
            if ts_val > 1_000_000_000:
                return datetime.fromtimestamp(ts_val, tz=timezone.utc)
        except (ValueError, TypeError):
            pass
        try:
            return datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None
