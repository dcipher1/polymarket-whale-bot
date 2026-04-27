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
ERROR_LOG_INTERVAL_SECONDS = 300
_LAST_ERROR_LOG: dict[str, datetime] = {}


def _throttled_error(message: str, endpoint: str, exc: Exception) -> None:
    key = f"{endpoint}:{type(exc).__name__}:{exc}"
    now = datetime.now(timezone.utc)
    last = _LAST_ERROR_LOG.get(key)
    if last is None or (now - last).total_seconds() >= ERROR_LOG_INTERVAL_SECONDS:
        _LAST_ERROR_LOG[key] = now
        logger.error(message, endpoint, exc)
    else:
        logger.debug(message, endpoint, exc)


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
    liquidity_num: float = Field(default=0.0, alias="liquidityNum")
    volume_24hr: float = Field(default=0.0, alias="volume24hr")
    created_at: str = Field(default="", alias="createdAt")
    accepting_orders: bool = Field(default=True, alias="acceptingOrders")
    neg_risk: bool = Field(default=False, alias="negRisk")
    neg_risk_market_id: str = Field(default="", alias="negRiskMarketId")
    neg_risk_request_id: str = Field(default="", alias="negRiskRequestId")
    description: str = ""
    game_start_time: str = Field(default="", alias="gameStartTime")
    events: list[dict] = Field(default_factory=list)

    model_config = {"populate_by_name": True, "coerce_numbers_to_str": True}

    def get_event_category(self) -> str | None:
        """Extract category from the nested event object (Polymarket's own classification)."""
        for event in self.events:
            cat = event.get("category", "")
            if cat:
                return cat
        return None

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
        url = endpoint if endpoint.startswith("http") else f"{BASE_URL}/{endpoint}"

        for attempt in range(3):
            try:
                await asyncio.sleep(RATE_LIMIT_DELAY)
                async with session.get(url, params=params) as resp:
                    if resp.status == 429:
                        wait = 2 ** (attempt + 1)
                        logger.warning("Rate limited on %s, waiting %ds", endpoint, wait)
                        await asyncio.sleep(wait)
                        continue
                    if resp.status == 422:
                        # Permanent error — market removed/gone, don't retry
                        raise aiohttp.ClientResponseError(
                            resp.request_info, resp.history,
                            status=422, message="Unprocessable Entity",
                        )
                    resp.raise_for_status()
                    data = await resp.json()

                    from src.config import settings as _settings
                    if _settings.log_level == "DEBUG" and endpoint not in self._logged_endpoints:
                        self._logged_endpoints.add(endpoint)
                        self._log_raw_response(endpoint, data)

                    return data
            except aiohttp.ClientError as e:
                if "422" in str(e) or "Unprocessable" in str(e):
                    logger.debug("Market gone (422): %s", endpoint)
                    raise
                if attempt == 2:
                    _throttled_error("Failed to fetch %s after 3 attempts: %s", endpoint, e)
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

    async def get_market(self, condition_id: str, slug: str | None = None) -> GammaMarket | None:
        """Fetch a single market by slug (preferred) or condition_id fallback.

        The Gamma path-based endpoint ``/markets/{id}`` expects a numeric ID,
        NOT a condition_id hex hash, so Gamma lookups use query-param endpoints.
        Some Gamma filters are silently ignored, so every response is checked
        against the requested condition id before it is accepted.
        """
        # Try cache first
        cache_key = f"gamma:market:{condition_id}"
        cached = await cache_get(cache_key)
        if cached:
            cached_market = GammaMarket.model_validate(cached)
            if cached_market.condition_id.lower() == condition_id.lower():
                return cached_market
            logger.debug(
                "Ignoring mismatched cached market for %s: got %s",
                condition_id[:10],
                cached_market.condition_id[:10],
            )

        data = None

        # Strategy 1: slug-based lookup (most reliable)
        if slug:
            try:
                data = await self._request("markets", {"slug": slug})
            except Exception as e:
                logger.debug("Gamma slug lookup failed for %s: %s", slug, e)

        # Strategy 2: condition_id query param fallback
        if not data or (isinstance(data, list) and not data):
            try:
                data = await self._request("markets", {"condition_ids": condition_id})
            except Exception as e:
                logger.debug("Gamma condition_id lookup failed for %s: %s", condition_id[:10], e)

        market = self._market_from_response(data, condition_id)
        if market is None:
            market = await self._get_clob_market(condition_id)
        if market is None:
            return None

        await cache_set(cache_key, market.model_dump(by_alias=True), ex=900)  # cache 15 min
        return market

    def _market_from_response(self, data: Any, condition_id: str) -> GammaMarket | None:
        if not data:
            return None
        if isinstance(data, dict) and "markets" in data:
            data = data.get("markets") or []
        if isinstance(data, list):
            for item in data:
                market = GammaMarket.model_validate(item)
                if market.condition_id.lower() == condition_id.lower():
                    return market
            return None
        market = GammaMarket.model_validate(data)
        if market.condition_id.lower() != condition_id.lower():
            logger.debug(
                "Ignoring mismatched Gamma market for %s: got %s",
                condition_id[:10],
                market.condition_id[:10],
            )
            return None
        return market

    async def _get_clob_market(self, condition_id: str) -> GammaMarket | None:
        try:
            data = await self._request(f"https://clob.polymarket.com/markets/{condition_id}")
        except Exception as e:
            logger.debug("CLOB market lookup failed for %s: %s", condition_id[:10], e)
            return None
        if not isinstance(data, dict):
            return None
        if str(data.get("condition_id") or "").lower() != condition_id.lower():
            return None
        token_ids = []
        outcomes = []
        for token in data.get("tokens") or []:
            if not isinstance(token, dict):
                continue
            token_id = str(token.get("token_id") or "")
            if not token_id:
                continue
            token_ids.append(token_id)
            outcomes.append(str(token.get("outcome") or ""))

        payload = {
            "conditionId": data.get("condition_id") or condition_id,
            "question": data.get("question") or "",
            "slug": data.get("market_slug") or data.get("slug") or "",
            "outcomes": json.dumps(outcomes),
            "clobTokenIds": json.dumps(token_ids),
            "endDate": data.get("end_date_iso") or data.get("endDate") or data.get("game_start_time") or "",
            "active": bool(data.get("active", True)),
            "closed": bool(data.get("closed", False)),
            "volume": str(data.get("volume") or data.get("volume_num") or "0"),
            "liquidityNum": float(data.get("liquidity") or data.get("liquidity_num") or 0),
            "volume24hr": float(data.get("volume_24hr") or data.get("volume24hr") or 0),
            "createdAt": data.get("created_at") or data.get("createdAt") or "",
            "acceptingOrders": bool(data.get("accepting_orders", False)),
            "description": data.get("description") or "",
            "gameStartTime": data.get("game_start_time") or "",
            "events": data.get("events") or [],
            "tokens": data.get("tokens") or [],
        }
        return GammaMarket.model_validate(payload)

    async def get_event_tags(self, event_id: str) -> list[str]:
        """Fetch tag labels from an event by numeric ID."""
        try:
            data = await self._request(f"events/{event_id}")
            if isinstance(data, dict):
                return [t.get("label", "") for t in data.get("tags", []) if t.get("label")]
        except Exception as e:
            logger.debug("Failed to fetch event %s tags: %s", event_id, e)
        return []

    async def get_market_by_slug(self, slug: str) -> GammaMarket | None:
        data = await self._request("markets", {"slug": slug})
        if isinstance(data, list) and data:
            return GammaMarket.model_validate(data[0])
        return None
