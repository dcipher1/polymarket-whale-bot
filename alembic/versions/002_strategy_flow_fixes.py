"""Strategy flow fixes: add columns for incremental ingestion, liquidity, volume_24hr.

Revision ID: 002
Revises: 001
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import TIMESTAMP

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade():
    # Wallet: incremental ingestion timestamp
    op.add_column("wallets", sa.Column("last_ingested_ts", TIMESTAMP(timezone=True), nullable=True))

    # Market: liquidity and 24hr volume from Gamma API
    op.add_column("markets", sa.Column("liquidity_usdc", sa.Numeric(18, 2), nullable=True))
    op.add_column("markets", sa.Column("volume_24hr_usdc", sa.Numeric(18, 2), nullable=True))

    # Fix bad entry prices (one-time cleanup)
    op.execute(
        "UPDATE whale_positions SET avg_entry_price = LEAST(GREATEST(avg_entry_price, 0), 1) "
        "WHERE avg_entry_price > 1 OR avg_entry_price < 0"
    )


def downgrade():
    op.drop_column("markets", "volume_24hr_usdc")
    op.drop_column("markets", "liquidity_usdc")
    op.drop_column("wallets", "last_ingested_ts")
