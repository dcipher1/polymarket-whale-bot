"""Polymarket Gamma API client for market metadata."""

import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
from pydantic import BaseModel, Field

from src.events import cache_get, cache_set

logger = logging.getLogger(__name__)

BASE_URL = "https://gamma-api.polymarket.com"
API_RESPONSES_DIR = Path("data/api_responses")
RATE_LIMIT_DELAY = 0.2


class GammaToken(BaseModel):
    token_id: str = Field(default="", alias="token_id")
    outcome: str = ""
    price: float = 0.0
    winner: bool = False

    model_config = {"populate_by_name": True}


class GammaMarket(BaseModel):
    condition_id: str = Field(default="", alias="conditionId")
    question: str = ""
    slug: str = ""
    outcomes: str = "[]"  # JSON string
    tokens: list[dict] = Field(default_factory=list)
    clob_token_ids: str | list[str] = Field(default="[]", alias="clobTokenIds")
    resolution_source: str = Field(default="", alias="resolutionSource")
    end_date_iso: str = Field(default="", alias="endDate")
    active: bool = True
    closed: bool = False
    tags: list[dict] = Field(default_factory=list)
    volume: str = "0"
    created_at: str = Field(default="", alias="createdAt")
    accepting_orders: bool = Field(default=True, alias="acceptingOrders")
    neg_risk: bool = Field(default=False, alias="negRisk")
    neg_risk_market_id: str = Field(default="", alias="negRiskMarketId")
    neg_risk_request_id: str = Field(default="", alias="negRiskRequestId")
    description: str = ""
    game_start_time: str = Field(default="", alias="gameStartTime")

    model_config = {"populate_by_name": True, "coerce_numbers_to_str": True}

    def get_token_ids(self) -> list[str]:
        if isinstance(self.clob_token_ids, list):
            return self.clob_token_ids
        if isinstance(self.clob_token_ids, str):
            try:
                import json as _json
                return _json.loads(self.clob_token_ids)
            except (json.JSONDecodeError, TypeError):
                return []
        return []

    def get_outcomes_list(self) -> list[str]:
        if isinstance(self.outcomes, str):
            try:
                import json as _json
                return _json.loads(self.outcomes)
            except (json.JSONDecodeError, TypeError):
                return []
        return self.outcomes

    def get_tag_labels(self) -> list[str]:
        labels = []
        for tag in self.tags:
            if isinstance(tag, dict):
                label = tag.get("label", tag.get("slug", ""))
                if label:
                    labels.append(label.lower())
            elif isinstance(tag, str):
                labels.append(tag.lower())
        return labels


class GammaAPIClient:
    def __init__(self):
        self._session: aiohttp.ClientSession | None = None
        self._logged_endpoints: set[str] = set()

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
        path = API_RESPONSES_DIR / f"gamma_{safe_name}_{ts}.json"
        try:
            with open(path, "w") as f:
                json.dump(data, f, indent=2, default=str)
            logger.info("Logged raw Gamma API response for %s to %s", endpoint, path)
        except Exception as e:
            logger.warning("Failed to log raw response: %s", e)

    async def get_markets(
        self,
        active: bool = True,
        closed: bool = False,
        limit: int = 100,
        offset: int = 0,
        tag: str | None = None,
    ) -> list[GammaMarket]:
        params: dict[str, Any] = {
            "active": str(active).lower(),
            "closed": str(closed).lower(),
            "limit": limit,
            "offset": offset,
        }
        if tag:
            params["tag"] = tag

        data = await self._request("markets", params)
        if not isinstance(data, list):
            data = data.get("markets", data.get("data", []))
        return [GammaMarket.model_validate(item) for item in data]

    async def get_all_active_markets(self) -> list[GammaMarket]:
        """Fetch all active markets with pagination."""
        all_markets = []
        offset = 0
        while True:
            batch = await self.get_markets(active=True, closed=False, limit=100, offset=offset)
            if not batch:
                break
            all_markets.extend(batch)
            if len(batch) < 100:
                break
            offset += 100
        logger.info("Fetched %d active markets from Gamma API", len(all_markets))
        return all_markets

    async def get_market(self, condition_id: str) -> GammaMarket | None:
        """Fetch a single market by condition_id."""
        # Try cache first
        cache_key = f"gamma:market:{condition_id}"
        cached = await cache_get(cache_key)
        if cached:
            return GammaMarket.model_validate(cached)

        data = await self._request(f"markets/{condition_id}")
        if not data:
            return None

        if isinstance(data, list) and data:
            data = data[0]
        elif isinstance(data, dict) and "markets" in data:
            markets = data["markets"]
            if markets:
                data = markets[0]

        market = GammaMarket.model_validate(data)
        await cache_set(cache_key, data, ex=900)  # cache 15 min
        return market

    async def get_market_by_slug(self, slug: str) -> GammaMarket | None:
        data = await self._request("markets", {"slug": slug})
        if isinstance(data, list) and data:
            return GammaMarket.model_validate(data[0])
        return None
