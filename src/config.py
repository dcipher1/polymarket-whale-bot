from typing import Annotated

from pydantic_settings import BaseSettings, NoDecode
from pydantic import Field, field_validator, model_validator


class Settings(BaseSettings):
    # Polymarket
    polymarket_private_key: str = ""
    polymarket_proxy_address: str = ""
    polymarket_funder_address: str = ""
    polymarket_wallet_address: str = ""
    polymarket_api_key: str = ""
    polymarket_api_secret: str = ""
    polymarket_api_passphrase: str = ""

    # PolyNode wallet websocket
    polynode_api_key: str = ""
    polynode_ws_url: str = "wss://ws.polynode.dev/ws"
    polynode_enabled: bool = True
    polynode_snapshot_count: int = 50
    polynode_subscription_type: str = "dome"
    wallet_sync_fallback_interval_seconds: float = 300

    # Infrastructure
    database_url: str = "postgresql+asyncpg://whale:whale@localhost:5432/whale_bot"
    redis_url: str = "redis://localhost:6379/0"

    # Telegram
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    # Wallet scoring thresholds (lowered responsibly for Phase 1 data)
    min_wallet_pnl: float = 50_000
    min_win_rate: float = 0.55
    min_profit_factor: float = 2.5
    min_resolved_trades: int = 10
    min_category_pnl: float = 5_000
    # Category-specific PnL minimums (weather/culture have small-stakes markets)
    min_category_pnl_weather: float = 300
    min_category_pnl_culture: float = 500

    # Candidate-level quality gate (looser than copyability thresholds)
    candidate_min_win_rate: float = 0.55
    candidate_min_profit_factor: float = 1.3
    candidate_min_trade_count: int = 10
    max_trades_per_month: int = 2000
    max_qualifying_trade_age_days: int = 30
    min_followability: float = 0.50
    min_conviction_score: int = 40

    # Category-specific followability minimums
    min_followability_macro: float = 0.55
    min_followability_crypto_weekly: float = 0.65
    min_followability_crypto_monthly: float = 0.55
    min_followability_politics: float = 0.55
    min_followability_geopolitics: float = 0.65
    min_followability_sports: float = 0.55
    min_followability_culture: float = 0.55
    min_followability_tech: float = 0.60
    min_followability_weather: float = 0.55

    # Signal thresholds
    convergence_min_wallets: int = 2
    max_slippage_pct: float = 0.05  # 5% above whale avg entry price
    max_absolute_slippage: float = 0.15  # legacy, unused
    min_edge_threshold: float = 0.02
    max_entry_price: float = 0.90  # don't buy effectively-resolved markets
    min_entry_price: float = 0.10  # don't buy extreme-longshot markets
    max_entry_price_trade: float = 0.90  # don't buy near-resolved markets

    # Valid categories for signal generation — weather copy-trader only tracks weather
    valid_categories: set[str] = {"weather"}

    # Hardcoded whale allowlist — the only wallets we copy (no scoring, no discovery).
    # All three are hold-to-resolution weather traders. MyTrade.source_wallets tags each
    # fill with the originating whale so we can break PnL down per-whale.
    # 8 whales by belief-edge calibration + city specialization + currently
    # profitable (1w PnL ≥ $0). Refreshed 2026-05-05.
    # Each whale ONLY trades their assigned cities (see watch_whale_cities).
    watch_whales: list[str] = [
        "0xf59026150af59ec41e85be554fcfae137ed6303c",   # +57.2% London/Shanghai/Seoul
        "0x35c4316574a4649865095ee9fe0c206005f6d144",   # +40.7% Seattle/Wellington
        "0x5264b001677b47dd2e20fe20213749dbd7909a63",   # +37.8% Helsinki/Chengdu/Taipei
        "0x3024712d12a75c911f8abf2a88ad44e3a4778ece",   # +36.6% Toronto/Madrid
        "0x8c994f56c15feb7086f4de73e8908ad2c8123ef0",   # +36.1% Paris
        "0x044f334595a7fd42c143e11c8ec47f23c8d1d1f1",   # +34.3% NYC (top performer)
        "0xae68539542db9d69e66445c21e6e3bdde417064a",   # +30.4% Beijing/Chicago
        "0x8aa29c27241b6909a7c4d6cb4f400267aa215a0b",   # +29.8% Miami/Houston/Austin
    ]

    # Copy sizing — mirror a fraction of the whale's *share count* PER WHALE.
    # With 8 whales and overlap possible on same markets, 1.0 keeps combined exposure manageable.
    # Each order is capped at $200 notional (MAX_ORDER_USDC in sync_positions.py)
    # and each per-market position is capped at $400 total (MAX_POSITION_USDC,
    # same file). Trading halts when wallet balance falls to MIN_WALLET_USDC ($50).
    position_size_fraction: float = 1.0

    # Fast-execution whales — websocket-driven BUYs from these wallets bypass
    # the whale_avg×1.05 slippage band and bid up to FAST_MAX_PRICE with a
    # FAST_MAX_ORDER_USDC nibble. Env: FAST_EXECUTION_WHALES (comma-separated).
    fast_execution_whales: Annotated[list[str], NoDecode] = []

    # When True, for bucketed neg-risk events (multi-bucket weather markets) the bot
    # picks a single bucket per event by computing the whale's max-payout-if-true
    # across all buckets in the event, and only buys YES on that bucket. Skips every
    # other bucket. See src/signals/event_pick.py.
    event_pick_enabled: bool = False

    # When True, replaces single-bucket picker with the belief-edge trader: infers
    # whale's implied probability per bucket from their full position book,
    # compares to live market prices, and trades any bucket with |edge| >= 3%.
    # See src/signals/whale_belief.py and scripts/whale_belief_discovery.py for
    # the calibration that picked these whales + modes. Mutually exclusive with
    # event_pick_enabled (asserted at startup).
    edge_trader_enabled: bool = False

    # Per-whale inference mode for the belief-edge trader. Modes from
    # whale_belief.py: "directional" | "size_weighted" | "price_complement".
    # Picked from data/whale_belief_ranking.json (best mode per whale).
    edge_trader_whales: dict[str, str] = {
        "0xf59026150af59ec41e85be554fcfae137ed6303c": "size_weighted",  # +57.2%
        "0x35c4316574a4649865095ee9fe0c206005f6d144": "directional",    # +40.7%
        "0x5264b001677b47dd2e20fe20213749dbd7909a63": "directional",    # +37.8%
        "0x3024712d12a75c911f8abf2a88ad44e3a4778ece": "directional",    # +36.6%
        "0x8c994f56c15feb7086f4de73e8908ad2c8123ef0": "directional",    # +36.1%
        "0x044f334595a7fd42c143e11c8ec47f23c8d1d1f1": "size_weighted",  # +34.3%
        "0xae68539542db9d69e66445c21e6e3bdde417064a": "size_weighted",  # +30.4%
        "0x8aa29c27241b6909a7c4d6cb4f400267aa215a0b": "directional",    # +29.8%
    }

    # Per-whale city allowlist. ENFORCED in edge_trader path: each whale ONLY
    # generates signals on events whose city is in their list. Refreshed
    # 2026-05-05 from scripts/whale_city_allocator.py — picks each whale's top
    # specialty cities (≥$100 PnL, ≥3 positions per city), no overlaps.
    watch_whale_cities: dict[str, list[str]] = {
        "0xf59026150af59ec41e85be554fcfae137ed6303c": ["London", "Shanghai", "Seoul"],
        "0x35c4316574a4649865095ee9fe0c206005f6d144": ["Seattle", "Wellington"],
        "0x5264b001677b47dd2e20fe20213749dbd7909a63": ["Helsinki", "Chengdu", "Taipei"],
        "0x3024712d12a75c911f8abf2a88ad44e3a4778ece": ["Toronto", "Madrid"],
        "0x8c994f56c15feb7086f4de73e8908ad2c8123ef0": ["Paris"],
        "0x044f334595a7fd42c143e11c8ec47f23c8d1d1f1": ["NYC"],
        "0xae68539542db9d69e66445c21e6e3bdde417064a": ["Beijing", "Chicago"],
        "0x8aa29c27241b6909a7c4d6cb4f400267aa215a0b": ["Miami", "Houston", "Austin"],
    }

    # Wallet freshness
    wallet_stale_days: int = 7

    # Orderbook depth check (observe-only in Phase 2)
    enforce_depth_check: bool = True
    min_depth_multiple: float = 3.0

    # Market quality filters
    min_market_volume: float = 100
    min_market_liquidity: float = 5000

    # Category-specific convergence windows (hours)
    convergence_window_macro: float = 72
    convergence_window_crypto_weekly: float = 36
    convergence_window_crypto_monthly: float = 72
    convergence_window_politics: float = 168
    convergence_window_geopolitics: float = 48
    convergence_window_sports: float = 72
    convergence_window_culture: float = 72
    convergence_window_tech: float = 48
    convergence_window_weather: float = 48

    # Dynamic convergence window fractions (fraction of hours_to_resolution)
    convergence_fraction_macro: float = 0.20
    convergence_fraction_crypto_weekly: float = 0.25
    convergence_fraction_crypto_monthly: float = 0.15
    convergence_fraction_politics: float = 0.10
    convergence_fraction_geopolitics: float = 0.20
    convergence_fraction_sports: float = 0.20
    convergence_fraction_culture: float = 0.20
    convergence_fraction_tech: float = 0.20
    convergence_fraction_weather: float = 0.20

    # Category-specific min hours to resolution
    min_hours_to_resolution_macro: float = 6
    min_hours_to_resolution_crypto_weekly: float = 24
    min_hours_to_resolution_crypto_monthly: float = 48
    min_hours_to_resolution_politics: float = 168
    min_hours_to_resolution_geopolitics: float = 24
    min_hours_to_resolution_sports: float = 24
    min_hours_to_resolution_culture: float = 24
    min_hours_to_resolution_tech: float = 24
    min_hours_to_resolution_weather: float = 6

    # Category-specific primary followability window
    followability_window_macro: str = "24h"
    followability_window_crypto_weekly: str = "2h"
    followability_window_crypto_monthly: str = "24h"
    followability_window_politics: str = "24h"
    followability_window_geopolitics: str = "2h"
    followability_window_sports: str = "24h"
    followability_window_culture: str = "24h"
    followability_window_tech: str = "2h"
    followability_window_weather: str = "24h"

    # Risk management
    max_position_pct: float = 0.05
    max_total_exposure_pct: float = 0.15  # legacy fallback, used only when balance unavailable
    wallet_reserve_usdc: float = 50  # minimum USDC to keep in wallet
    max_same_category_positions: int = 6
    max_per_whale_positions: int = 10
    daily_loss_halt_pct: float = 0.05
    weekly_drawdown_halt_pct: float = 0.10
    monthly_drawdown_halt_pct: float = 0.20
    halt_on_resolution_backlog: bool = True
    max_unresolved_past_resolution_markets: int = 10

    # Position sizing
    fixed_position_size_usdc: float = 50
    convergence_scale_enabled: bool = True
    category_multiplier_crypto_weekly: float = 1.2
    category_multiplier_crypto_monthly: float = 1.0
    category_multiplier_politics: float = 0.8
    category_multiplier_macro: float = 1.0
    category_multiplier_geopolitics: float = 1.0
    category_multiplier_sports: float = 0.8
    category_multiplier_culture: float = 0.8
    category_multiplier_tech: float = 0.8
    category_multiplier_weather: float = 1.0

    # Capital
    starting_capital: float = 15_000

    # Phase control
    live_execution_enabled: bool = False
    strict_mode: bool = True

    # Operational
    geoblock_check_interval_hours: float = 6
    wallet_rescore_interval_hours: float = 2
    market_refresh_interval_minutes: float = 15
    log_level: str = "INFO"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    @field_validator("fast_execution_whales", mode="before")
    @classmethod
    def _parse_fast_execution_whales(cls, v):
        if isinstance(v, str):
            return [w.strip().lower() for w in v.split(",") if w.strip()]
        if isinstance(v, list):
            return [str(w).strip().lower() for w in v if str(w).strip()]
        return v or []

    @model_validator(mode="after")
    def _check_signal_mode_mutex(self):
        if self.event_pick_enabled and self.edge_trader_enabled:
            raise ValueError(
                "event_pick_enabled and edge_trader_enabled are mutually exclusive — "
                "set one to false."
            )
        return self

    _SENSITIVE_FIELDS = {
        "polymarket_private_key", "polymarket_api_key", "polymarket_api_secret",
        "polymarket_api_passphrase", "polynode_api_key", "telegram_bot_token",
    }

    def __repr__(self) -> str:
        fields = []
        for k, v in self.model_dump().items():
            if k in self._SENSITIVE_FIELDS and v:
                fields.append(f"{k}='***'")
            else:
                fields.append(f"{k}={v!r}")
        return f"Settings({', '.join(fields)})"

    def __str__(self) -> str:
        return self.__repr__()

    def get_convergence_window(self, category: str) -> float:
        return getattr(self, f"convergence_window_{category}", 72)

    def get_min_hours_to_resolution(self, category: str) -> float:
        return getattr(self, f"min_hours_to_resolution_{category}", 24)

    def get_min_followability(self, category: str) -> float:
        return getattr(self, f"min_followability_{category}", self.min_followability)

    def get_min_category_pnl(self, category: str) -> float:
        return getattr(self, f"min_category_pnl_{category}", self.min_category_pnl)

    def get_followability_window(self, category: str) -> str:
        return getattr(self, f"followability_window_{category}", "24h")

    def get_convergence_fraction(self, category: str) -> float:
        return getattr(self, f"convergence_fraction_{category}", 0.20)

    def get_category_multiplier(self, category: str) -> float:
        return getattr(self, f"category_multiplier_{category}", 1.0)


settings = Settings()
