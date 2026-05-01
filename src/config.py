from typing import Annotated

from pydantic_settings import BaseSettings, NoDecode
from pydantic import Field, field_validator


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
    watch_whales: list[str] = [
        "0x8aa29c27241b6909a7c4d6cb4f400267aa215a0b",   # US weather, multi-bucket directional + partial hedges
        "0x9c68b13a2c9b6d2e80826f26cea746cc22ba7936",   # International weather (Jakarta, Karachi, Singapore, Lagos) — fast_execution
        "0xaa930fdc4caa3c0f6067404a7bd7899ca45f0bc7",   # 82% weather, slow accumulator, ~133 open low-price YES positions, +$12K all-time
        "0x672fe5d8a7b946fa9bff1b902a44a56718a213cc",   # BA / Tel Aviv / Madrid weather, BUY-only, +$4.5k/month, median $19
    ]

    # Copy sizing — mirror a fraction of the whale's *share count* PER WHALE.
    # With 3 whales and overlap possible on same markets, 1.0 keeps combined exposure manageable.
    # Each order is capped at $100 notional (MAX_ORDER_USDC in sync_positions.py).
    position_size_fraction: float = 1.0

    # Per-whale sizing override. Multiplied into the global position_size_fraction
    # so a value of 2.0 means we copy 2x that whale's share count. Addresses must
    # be lowercase. The per-position 10% bankroll cap still applies as a backstop.
    position_size_multipliers: dict[str, float] = {
        "0x8aa29c27241b6909a7c4d6cb4f400267aa215a0b": 2.0,  # best copy whale, 2x
    }

    # Fast-execution whales — websocket-driven BUYs from these wallets bypass
    # the whale_avg×1.05 slippage band and bid up to FAST_MAX_PRICE with a
    # FAST_MAX_ORDER_USDC nibble. Env: FAST_EXECUTION_WHALES (comma-separated).
    fast_execution_whales: Annotated[list[str], NoDecode] = []

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
