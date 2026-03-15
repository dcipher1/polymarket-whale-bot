"""Initial schema

Revision ID: 001
Revises:
Create Date: 2026-03-15
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, TIMESTAMP

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')

    op.create_table(
        "wallets",
        sa.Column("address", sa.Text(), primary_key=True),
        sa.Column("display_name", sa.Text()),
        sa.Column("first_seen", TIMESTAMP(timezone=True), server_default=sa.func.now()),
        sa.Column("last_active", TIMESTAMP(timezone=True)),
        sa.Column("last_scored", TIMESTAMP(timezone=True)),
        sa.Column("total_trades", sa.Integer(), server_default="0"),
        sa.Column("total_pnl_usdc", sa.Numeric(18, 2), server_default="0"),
        sa.Column("estimated_bankroll", sa.Numeric(18, 2)),
        sa.Column("conviction_score", sa.SmallInteger(), server_default="0"),
        sa.Column("copyability_class", sa.Text(), server_default="WATCH"),
        sa.Column("cluster_id", sa.Text()),
        sa.Column("trades_per_month", sa.Numeric(8, 2)),
        sa.Column("median_hold_hours", sa.Numeric(10, 2)),
        sa.Column("pct_held_to_resolution", sa.Numeric(5, 4)),
        sa.Column("meta", JSONB(), server_default="{}"),
        sa.CheckConstraint(
            "copyability_class IN ('REJECT', 'WATCH', 'COPYABLE')",
            name="ck_wallets_copyability",
        ),
    )

    op.create_table(
        "wallet_category_scores",
        sa.Column("wallet_address", sa.Text(), sa.ForeignKey("wallets.address", ondelete="CASCADE"), primary_key=True),
        sa.Column("category", sa.Text(), primary_key=True),
        sa.Column("win_rate", sa.Numeric(5, 4)),
        sa.Column("profit_factor", sa.Numeric(8, 2)),
        sa.Column("gain_loss_ratio", sa.Numeric(8, 2)),
        sa.Column("expectancy", sa.Numeric(8, 4)),
        sa.Column("trade_count", sa.Integer()),
        sa.Column("avg_hold_hours", sa.Numeric(10, 2)),
        sa.Column("followability_5m", sa.Numeric(5, 4)),
        sa.Column("followability_30m", sa.Numeric(5, 4)),
        sa.Column("followability_2h", sa.Numeric(5, 4)),
        sa.Column("followability_24h", sa.Numeric(5, 4)),
        sa.Column("followability", sa.Numeric(5, 4)),
        sa.Column("followability_provisional", sa.Boolean(), server_default="true"),
        sa.Column("avg_position_pct", sa.Numeric(5, 4)),
        sa.Column("last_updated", TIMESTAMP(timezone=True)),
    )

    op.create_table(
        "markets",
        sa.Column("condition_id", sa.Text(), primary_key=True),
        sa.Column("question", sa.Text(), nullable=False),
        sa.Column("slug", sa.Text()),
        sa.Column("category", sa.Text()),
        sa.Column("category_matched_keywords", ARRAY(sa.Text())),
        sa.Column("category_matched_tags", ARRAY(sa.Text())),
        sa.Column("classification_source", sa.Text(), server_default="auto"),
        sa.Column("category_override", sa.Text()),
        sa.Column("resolution_time", TIMESTAMP(timezone=True)),
        sa.Column("resolved", sa.Boolean(), server_default="false"),
        sa.Column("outcome", sa.Text()),
        sa.Column("tags", ARRAY(sa.Text())),
        sa.Column("volume_usdc", sa.Numeric(18, 2)),
        sa.Column("created_at", TIMESTAMP(timezone=True)),
        sa.Column("last_updated", TIMESTAMP(timezone=True), server_default=sa.func.now()),
        sa.Column("meta", JSONB(), server_default="{}"),
    )

    op.create_table(
        "market_tokens",
        sa.Column("token_id", sa.Text(), primary_key=True),
        sa.Column("condition_id", sa.Text(), sa.ForeignKey("markets.condition_id", ondelete="CASCADE"), nullable=False),
        sa.Column("outcome", sa.Text(), nullable=False),
        sa.Column("has_taker_fees", sa.Boolean()),
        sa.Column("taker_fee_rate", sa.Numeric(8, 6)),
        sa.Column("fee_last_checked", TIMESTAMP(timezone=True)),
        sa.CheckConstraint("outcome IN ('YES', 'NO')", name="ck_market_tokens_outcome"),
        sa.UniqueConstraint("condition_id", "outcome", name="uq_market_tokens_condition_outcome"),
    )

    op.create_table(
        "whale_trades",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("wallet_address", sa.Text(), sa.ForeignKey("wallets.address", ondelete="CASCADE"), nullable=False),
        sa.Column("condition_id", sa.Text(), sa.ForeignKey("markets.condition_id", ondelete="CASCADE"), nullable=False),
        sa.Column("token_id", sa.Text(), nullable=False),
        sa.Column("side", sa.Text(), nullable=False),
        sa.Column("outcome", sa.Text(), nullable=False),
        sa.Column("price", sa.Numeric(8, 6), nullable=False),
        sa.Column("size_usdc", sa.Numeric(18, 2), nullable=False),
        sa.Column("num_contracts", sa.Numeric(18, 2)),
        sa.Column("timestamp", TIMESTAMP(timezone=True), nullable=False),
        sa.Column("tx_hash", sa.Text()),
        sa.Column("detected_at", TIMESTAMP(timezone=True), server_default=sa.func.now()),
        sa.CheckConstraint("side IN ('BUY', 'SELL')", name="ck_whale_trades_side"),
        sa.CheckConstraint("outcome IN ('YES', 'NO')", name="ck_whale_trades_outcome"),
        sa.UniqueConstraint("wallet_address", "tx_hash", "token_id", name="uq_whale_trades_dedup"),
    )

    op.create_table(
        "whale_positions",
        sa.Column("wallet_address", sa.Text(), sa.ForeignKey("wallets.address", ondelete="CASCADE"), primary_key=True),
        sa.Column("condition_id", sa.Text(), sa.ForeignKey("markets.condition_id", ondelete="CASCADE"), primary_key=True),
        sa.Column("outcome", sa.Text(), primary_key=True),
        sa.Column("avg_entry_price", sa.Numeric(8, 6)),
        sa.Column("total_size_usdc", sa.Numeric(18, 2)),
        sa.Column("num_contracts", sa.Numeric(18, 2)),
        sa.Column("first_entry", TIMESTAMP(timezone=True)),
        sa.Column("last_updated", TIMESTAMP(timezone=True)),
        sa.Column("is_open", sa.Boolean(), server_default="true"),
        sa.Column("last_event_type", sa.Text()),
        sa.CheckConstraint("outcome IN ('YES', 'NO')", name="ck_whale_positions_outcome"),
        sa.CheckConstraint(
            "last_event_type IN ('OPEN', 'ADD', 'REDUCE', 'CLOSE')",
            name="ck_whale_positions_event_type",
        ),
    )

    op.create_table(
        "signals",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("condition_id", sa.Text(), sa.ForeignKey("markets.condition_id"), nullable=False),
        sa.Column("signal_type", sa.Text(), nullable=False),
        sa.Column("outcome", sa.Text(), nullable=False),
        sa.Column("confidence", sa.SmallInteger()),
        sa.Column("source_wallets", ARRAY(sa.Text()), nullable=False),
        sa.Column("whale_avg_price", sa.Numeric(8, 6)),
        sa.Column("market_price_at_signal", sa.Numeric(8, 6)),
        sa.Column("max_entry_price", sa.Numeric(8, 6)),
        sa.Column("suggested_size_usdc", sa.Numeric(18, 2)),
        sa.Column("category", sa.Text()),
        sa.Column("hours_to_resolution", sa.Numeric(10, 2)),
        sa.Column("convergence_count", sa.Integer(), server_default="1"),
        sa.Column("created_at", TIMESTAMP(timezone=True), server_default=sa.func.now()),
        sa.Column("expires_at", TIMESTAMP(timezone=True)),
        sa.Column("status", sa.Text(), server_default="PENDING"),
        sa.Column("status_reason", sa.Text()),
        sa.Column("audit_trail", JSONB()),
        sa.Column("meta", JSONB(), server_default="{}"),
        sa.CheckConstraint("signal_type IN ('CANDIDATE', 'CONVERGENCE', 'LIVE')", name="ck_signals_type"),
        sa.CheckConstraint(
            "status IN ('PENDING', 'EXECUTED', 'EXPIRED', 'SKIPPED', 'REJECTED')",
            name="ck_signals_status",
        ),
    )

    op.create_table(
        "my_trades",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("signal_id", sa.BigInteger(), sa.ForeignKey("signals.id")),
        sa.Column("condition_id", sa.Text(), sa.ForeignKey("markets.condition_id"), nullable=False),
        sa.Column("outcome", sa.Text(), nullable=False),
        sa.Column("entry_price", sa.Numeric(8, 6)),
        sa.Column("size_usdc", sa.Numeric(18, 2)),
        sa.Column("num_contracts", sa.Integer()),
        sa.Column("order_id", sa.Text()),
        sa.Column("fill_status", sa.Text(), server_default="PAPER"),
        sa.Column("entry_timestamp", TIMESTAMP(timezone=True)),
        sa.Column("exit_price", sa.Numeric(8, 6)),
        sa.Column("exit_timestamp", TIMESTAMP(timezone=True)),
        sa.Column("pnl_usdc", sa.Numeric(18, 2)),
        sa.Column("resolved", sa.Boolean(), server_default="false"),
        sa.Column("trade_outcome", sa.Text()),
        sa.Column("fees_paid", sa.Numeric(18, 4), server_default="0"),
        sa.Column("source_wallets", ARRAY(sa.Text())),
        sa.Column("attribution", JSONB(), server_default="{}"),
        sa.CheckConstraint(
            "fill_status IN ('PAPER', 'PENDING', 'PARTIAL', 'FILLED', 'CANCELLED')",
            name="ck_my_trades_fill_status",
        ),
    )

    op.create_table(
        "portfolio_snapshots",
        sa.Column("date", sa.Date(), primary_key=True),
        sa.Column("total_capital", sa.Numeric(18, 2)),
        sa.Column("open_positions", sa.Integer()),
        sa.Column("total_exposure", sa.Numeric(18, 2)),
        sa.Column("daily_pnl", sa.Numeric(18, 2)),
        sa.Column("cumulative_pnl", sa.Numeric(18, 2)),
        sa.Column("win_count_30d", sa.Integer()),
        sa.Column("loss_count_30d", sa.Integer()),
        sa.Column("profit_factor_30d", sa.Numeric(8, 4)),
        sa.Column("expectancy_30d", sa.Numeric(8, 4)),
        sa.Column("max_drawdown_30d", sa.Numeric(5, 4)),
    )

    # Indexes
    op.create_index("idx_market_tokens_condition", "market_tokens", ["condition_id"])
    op.create_index("idx_whale_trades_wallet_time", "whale_trades", ["wallet_address", sa.text("timestamp DESC")])
    op.create_index("idx_whale_trades_market", "whale_trades", ["condition_id", sa.text("timestamp DESC")])
    op.create_index(
        "idx_whale_positions_open", "whale_positions", ["condition_id"],
        postgresql_where=sa.text("is_open = TRUE"),
    )
    op.create_index(
        "idx_signals_pending", "signals", ["status", sa.text("created_at DESC")],
        postgresql_where=sa.text("status = 'PENDING'"),
    )
    op.create_index(
        "idx_markets_unresolved", "markets", ["resolution_time"],
        postgresql_where=sa.text("resolved = FALSE"),
    )
    op.create_index("idx_markets_category", "markets", ["category"])
    op.create_index(
        "idx_wallets_copyable", "wallets", ["copyability_class"],
        postgresql_where=sa.text("copyability_class = 'COPYABLE'"),
    )


def downgrade() -> None:
    op.drop_table("portfolio_snapshots")
    op.drop_table("my_trades")
    op.drop_table("signals")
    op.drop_table("whale_positions")
    op.drop_table("whale_trades")
    op.drop_table("market_tokens")
    op.drop_table("markets")
    op.drop_table("wallet_category_scores")
    op.drop_table("wallets")
