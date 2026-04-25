from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Date,
    Index,
    Integer,
    Numeric,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    ForeignKey,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, TIMESTAMP
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


def utcnow():
    return datetime.now(timezone.utc)


class Wallet(Base):
    __tablename__ = "wallets"

    address: Mapped[str] = mapped_column(Text, primary_key=True)
    display_name: Mapped[str | None] = mapped_column(Text)
    first_seen: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=utcnow
    )
    last_active: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    last_ingested_ts: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    last_scored: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    total_trades: Mapped[int] = mapped_column(Integer, default=0)
    total_pnl_usdc: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=0)
    estimated_bankroll: Mapped[Decimal | None] = mapped_column(Numeric(18, 2))
    conviction_score: Mapped[int] = mapped_column(SmallInteger, default=0)
    copyability_class: Mapped[str] = mapped_column(
        Text, default="WATCH"
    )
    cluster_id: Mapped[str | None] = mapped_column(Text)
    trades_per_month: Mapped[Decimal | None] = mapped_column(Numeric(8, 2))
    median_hold_hours: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
    pct_held_to_resolution: Mapped[Decimal | None] = mapped_column(Numeric(5, 4))
    discovery_win_rate: Mapped[Decimal | None] = mapped_column(Numeric(8, 4))
    discovery_profit_factor: Mapped[Decimal | None] = mapped_column(Numeric(8, 4))
    specialization_ratio: Mapped[Decimal | None] = mapped_column(Numeric(5, 4))
    meta: Mapped[dict] = mapped_column(JSONB, default=dict)

    category_scores: Mapped[list["WalletCategoryScore"]] = relationship(
        back_populates="wallet", cascade="all, delete-orphan"
    )
    trades: Mapped[list["WhaleTrade"]] = relationship(
        back_populates="wallet", cascade="all, delete-orphan"
    )
    positions: Mapped[list["WhalePosition"]] = relationship(
        back_populates="wallet", cascade="all, delete-orphan"
    )

    __table_args__ = (
        CheckConstraint(
            "copyability_class IN ('REJECT', 'WATCH', 'COPYABLE')",
            name="ck_wallets_copyability",
        ),
        Index("idx_wallets_copyable", "copyability_class", postgresql_where=text("copyability_class = 'COPYABLE'")),
    )


class WalletCategoryScore(Base):
    __tablename__ = "wallet_category_scores"

    wallet_address: Mapped[str] = mapped_column(
        Text, ForeignKey("wallets.address", ondelete="CASCADE"), primary_key=True
    )
    category: Mapped[str] = mapped_column(Text, primary_key=True)

    win_rate: Mapped[Decimal | None] = mapped_column(Numeric(5, 4))
    profit_factor: Mapped[Decimal | None] = mapped_column(Numeric(8, 2))
    gain_loss_ratio: Mapped[Decimal | None] = mapped_column(Numeric(8, 2))
    expectancy: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    trade_count: Mapped[int | None] = mapped_column(Integer)
    avg_hold_hours: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))

    followability_5m: Mapped[Decimal | None] = mapped_column(Numeric(5, 4))
    followability_30m: Mapped[Decimal | None] = mapped_column(Numeric(5, 4))
    followability_2h: Mapped[Decimal | None] = mapped_column(Numeric(5, 4))
    followability_24h: Mapped[Decimal | None] = mapped_column(Numeric(5, 4))
    followability: Mapped[Decimal | None] = mapped_column(Numeric(5, 4))
    followability_provisional: Mapped[bool] = mapped_column(Boolean, default=True)

    avg_position_pct: Mapped[Decimal | None] = mapped_column(Numeric(5, 4))
    category_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=0)
    qualifying: Mapped[bool] = mapped_column(Boolean, default=False)
    last_updated: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))

    wallet: Mapped["Wallet"] = relationship(back_populates="category_scores")


class Market(Base):
    __tablename__ = "markets"

    condition_id: Mapped[str] = mapped_column(Text, primary_key=True)
    question: Mapped[str] = mapped_column(Text, nullable=False)
    slug: Mapped[str | None] = mapped_column(Text)
    category: Mapped[str | None] = mapped_column(Text)
    category_matched_keywords: Mapped[list[str] | None] = mapped_column(ARRAY(Text))
    category_matched_tags: Mapped[list[str] | None] = mapped_column(ARRAY(Text))
    classification_source: Mapped[str] = mapped_column(Text, default="auto")
    category_override: Mapped[str | None] = mapped_column(Text)
    resolution_time: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    resolved: Mapped[bool] = mapped_column(Boolean, default=False)
    outcome: Mapped[str | None] = mapped_column(Text)
    tags: Mapped[list[str] | None] = mapped_column(ARRAY(Text))
    volume_usdc: Mapped[Decimal | None] = mapped_column(Numeric(18, 2))
    liquidity_usdc: Mapped[Decimal | None] = mapped_column(Numeric(18, 2))
    volume_24hr_usdc: Mapped[Decimal | None] = mapped_column(Numeric(18, 2))
    created_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    last_updated: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=utcnow
    )
    meta: Mapped[dict] = mapped_column(JSONB, default=dict)

    tokens: Mapped[list["MarketToken"]] = relationship(
        back_populates="market", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("idx_markets_unresolved", "resolution_time", postgresql_where=text("resolved = FALSE")),
        Index("idx_markets_category", "category"),
    )


class MarketToken(Base):
    __tablename__ = "market_tokens"

    token_id: Mapped[str] = mapped_column(Text, primary_key=True)
    condition_id: Mapped[str] = mapped_column(
        Text, ForeignKey("markets.condition_id", ondelete="CASCADE"), nullable=False
    )
    outcome: Mapped[str] = mapped_column(Text, nullable=False)
    has_taker_fees: Mapped[bool | None] = mapped_column(Boolean)
    taker_fee_rate: Mapped[Decimal | None] = mapped_column(Numeric(8, 6))
    fee_last_checked: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))

    market: Mapped["Market"] = relationship(back_populates="tokens")

    __table_args__ = (
        CheckConstraint("outcome IN ('YES', 'NO')", name="ck_market_tokens_outcome"),
        UniqueConstraint("condition_id", "outcome", name="uq_market_tokens_condition_outcome"),
        Index("idx_market_tokens_condition", "condition_id"),
    )


class WhaleTrade(Base):
    __tablename__ = "whale_trades"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    wallet_address: Mapped[str] = mapped_column(
        Text, ForeignKey("wallets.address", ondelete="CASCADE"), nullable=False
    )
    condition_id: Mapped[str] = mapped_column(
        Text, ForeignKey("markets.condition_id", ondelete="CASCADE"), nullable=False
    )
    token_id: Mapped[str] = mapped_column(Text, nullable=False)
    side: Mapped[str] = mapped_column(Text, nullable=False)
    outcome: Mapped[str] = mapped_column(Text, nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(8, 6), nullable=False)
    size_usdc: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    num_contracts: Mapped[Decimal | None] = mapped_column(Numeric(18, 2))
    timestamp: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    tx_hash: Mapped[str | None] = mapped_column(Text)
    detected_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=utcnow
    )

    wallet: Mapped["Wallet"] = relationship(back_populates="trades")

    __table_args__ = (
        CheckConstraint("side IN ('BUY', 'SELL')", name="ck_whale_trades_side"),
        CheckConstraint("outcome IN ('YES', 'NO')", name="ck_whale_trades_outcome"),
        UniqueConstraint("wallet_address", "tx_hash", "token_id", name="uq_whale_trades_dedup"),
        Index("idx_whale_trades_wallet_time", "wallet_address", timestamp.desc()),
        Index("idx_whale_trades_market", "condition_id", timestamp.desc()),
    )


class WhalePosition(Base):
    __tablename__ = "whale_positions"

    wallet_address: Mapped[str] = mapped_column(
        Text, ForeignKey("wallets.address", ondelete="CASCADE"), primary_key=True
    )
    condition_id: Mapped[str] = mapped_column(
        Text, ForeignKey("markets.condition_id", ondelete="CASCADE"), primary_key=True
    )
    outcome: Mapped[str] = mapped_column(Text, primary_key=True)
    avg_entry_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6))
    total_size_usdc: Mapped[Decimal | None] = mapped_column(Numeric(18, 2))
    num_contracts: Mapped[Decimal | None] = mapped_column(Numeric(18, 2))
    first_entry: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    last_updated: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    is_open: Mapped[bool] = mapped_column(Boolean, default=True)
    last_event_type: Mapped[str | None] = mapped_column(Text)
    slug: Mapped[str | None] = mapped_column(Text)

    wallet: Mapped["Wallet"] = relationship(back_populates="positions")

    __table_args__ = (
        CheckConstraint("outcome IN ('YES', 'NO')", name="ck_whale_positions_outcome"),
        CheckConstraint(
            "last_event_type IN ('OPEN', 'ADD', 'REDUCE', 'CLOSE')",
            name="ck_whale_positions_event_type",
        ),
        Index("idx_whale_positions_open", "condition_id", postgresql_where=text("is_open = TRUE")),
    )


class Signal(Base):
    __tablename__ = "signals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    condition_id: Mapped[str] = mapped_column(
        Text, ForeignKey("markets.condition_id"), nullable=False
    )
    signal_type: Mapped[str] = mapped_column(Text, nullable=False)
    outcome: Mapped[str] = mapped_column(Text, nullable=False)
    confidence: Mapped[int | None] = mapped_column(SmallInteger)
    source_wallets: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False)
    whale_avg_price: Mapped[Decimal | None] = mapped_column(Numeric(8, 6))
    market_price_at_signal: Mapped[Decimal | None] = mapped_column(Numeric(8, 6))
    max_entry_price: Mapped[Decimal | None] = mapped_column(Numeric(8, 6))
    suggested_size_usdc: Mapped[Decimal | None] = mapped_column(Numeric(18, 2))
    category: Mapped[str | None] = mapped_column(Text)
    hours_to_resolution: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
    convergence_count: Mapped[int] = mapped_column(Integer, default=1)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=utcnow
    )
    expires_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    status: Mapped[str] = mapped_column(Text, default="PENDING")
    status_reason: Mapped[str | None] = mapped_column(Text)
    audit_trail: Mapped[dict | None] = mapped_column(JSONB)
    meta: Mapped[dict] = mapped_column(JSONB, default=dict)

    __table_args__ = (
        CheckConstraint(
            "signal_type IN ('CANDIDATE', 'CONVERGENCE', 'LIVE')",
            name="ck_signals_type",
        ),
        CheckConstraint(
            "status IN ('PENDING', 'EXECUTED', 'EXPIRED', 'SKIPPED', 'REJECTED')",
            name="ck_signals_status",
        ),
        Index("idx_signals_pending", "status", created_at.desc(), postgresql_where=text("status = 'PENDING'")),
    )


class MyTrade(Base):
    __tablename__ = "my_trades"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    signal_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("signals.id"))
    condition_id: Mapped[str] = mapped_column(
        Text, ForeignKey("markets.condition_id"), nullable=False
    )
    outcome: Mapped[str] = mapped_column(Text, nullable=False)
    entry_price: Mapped[Decimal | None] = mapped_column(Numeric(8, 6))
    size_usdc: Mapped[Decimal | None] = mapped_column(Numeric(18, 2))
    num_contracts: Mapped[int | None] = mapped_column(Integer)
    order_id: Mapped[str | None] = mapped_column(Text)
    fill_status: Mapped[str] = mapped_column(Text, default="PAPER")
    entry_timestamp: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    exit_price: Mapped[Decimal | None] = mapped_column(Numeric(8, 6))
    exit_timestamp: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    pnl_usdc: Mapped[Decimal | None] = mapped_column(Numeric(18, 2))
    resolved: Mapped[bool] = mapped_column(Boolean, default=False)
    trade_outcome: Mapped[str | None] = mapped_column(Text)
    fees_paid: Mapped[Decimal] = mapped_column(Numeric(18, 4), default=0)
    source_wallets: Mapped[list[str] | None] = mapped_column(ARRAY(Text))
    attribution: Mapped[dict] = mapped_column(JSONB, default=dict)

    __table_args__ = (
        CheckConstraint(
            "fill_status IN ('PAPER', 'PENDING', 'PARTIAL', 'FILLED', 'CANCELLED', 'FAILED')",
            name="ck_my_trades_fill_status",
        ),
    )


class PortfolioSnapshot(Base):
    __tablename__ = "portfolio_snapshots"

    date: Mapped[datetime] = mapped_column(Date, primary_key=True)
    total_capital: Mapped[Decimal | None] = mapped_column(Numeric(18, 2))
    open_positions: Mapped[int | None] = mapped_column(Integer)
    total_exposure: Mapped[Decimal | None] = mapped_column(Numeric(18, 2))
    daily_pnl: Mapped[Decimal | None] = mapped_column(Numeric(18, 2))
    cumulative_pnl: Mapped[Decimal | None] = mapped_column(Numeric(18, 2))
    win_count_30d: Mapped[int | None] = mapped_column(Integer)
    loss_count_30d: Mapped[int | None] = mapped_column(Integer)
    profit_factor_30d: Mapped[Decimal | None] = mapped_column(Numeric(8, 4))
    expectancy_30d: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    max_drawdown_30d: Mapped[Decimal | None] = mapped_column(Numeric(5, 4))
