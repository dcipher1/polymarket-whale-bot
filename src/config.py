from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    # Polymarket
    polymarket_private_key: str = ""
    polymarket_proxy_address: str = ""
    polymarket_funder_address: str = ""

    # Infrastructure
    database_url: str = "postgresql+asyncpg://whale:whale@localhost:5432/whale_bot"
    redis_url: str = "redis://localhost:6379/0"

    # Telegram
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    # Wallet scoring thresholds (lowered responsibly for Phase 1 data)
    min_wallet_pnl: float = 50_000
    min_win_rate: float = 0.55
    min_profit_factor: float = 1.3
    min_resolved_trades: int = 10
    max_trades_per_month: int = 200
    min_followability: float = 0.50
    min_conviction_score: int = 40

    # Category-specific followability minimums
    min_followability_macro: float = 0.55
    min_followability_crypto_weekly: float = 0.65
    min_followability_crypto_monthly: float = 0.55
    min_followability_politics: float = 0.55
    min_followability_geopolitics: float = 0.65

    # Signal thresholds
    convergence_min_wallets: int = 2
    max_slippage_pct: float = 0.10

    # Category-specific convergence windows (hours)
    convergence_window_macro: float = 72
    convergence_window_crypto_weekly: float = 36
    convergence_window_crypto_monthly: float = 72
    convergence_window_politics: float = 168
    convergence_window_geopolitics: float = 48

    # Category-specific min hours to resolution
    min_hours_to_resolution_macro: float = 6
    min_hours_to_resolution_crypto_weekly: float = 24
    min_hours_to_resolution_crypto_monthly: float = 48
    min_hours_to_resolution_politics: float = 168
    min_hours_to_resolution_geopolitics: float = 24

    # Category-specific primary followability window
    followability_window_macro: str = "24h"
    followability_window_crypto_weekly: str = "2h"
    followability_window_crypto_monthly: str = "24h"
    followability_window_politics: str = "24h"
    followability_window_geopolitics: str = "2h"

    # Risk management
    max_position_pct: float = 0.05
    max_total_exposure_pct: float = 0.15
    max_same_category_positions: int = 3
    daily_loss_halt_pct: float = 0.05
    weekly_drawdown_halt_pct: float = 0.10
    monthly_drawdown_halt_pct: float = 0.20

    # Position sizing
    fixed_position_size_usdc: float = 100

    # Capital
    starting_capital: float = 15_000

    # Phase control
    live_execution_enabled: bool = False
    strict_mode: bool = True

    # Operational
    geoblock_check_interval_hours: float = 6
    wallet_rescore_interval_hours: float = 6
    market_refresh_interval_minutes: float = 15
    log_level: str = "INFO"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    def get_convergence_window(self, category: str) -> float:
        return getattr(self, f"convergence_window_{category}", 72)

    def get_min_hours_to_resolution(self, category: str) -> float:
        return getattr(self, f"min_hours_to_resolution_{category}", 24)

    def get_min_followability(self, category: str) -> float:
        return getattr(self, f"min_followability_{category}", self.min_followability)

    def get_followability_window(self, category: str) -> str:
        return getattr(self, f"followability_window_{category}", "24h")


settings = Settings()
