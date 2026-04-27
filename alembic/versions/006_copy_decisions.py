"""Add copy decisions audit table.

Revision ID: 006
Revises: 005
"""

from alembic import op


revision = "006"
down_revision = "005"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS copy_decisions (
            id BIGSERIAL PRIMARY KEY,
            whale_trade_id BIGINT REFERENCES whale_trades(id) ON DELETE SET NULL,
            wallet_address TEXT,
            condition_id TEXT,
            outcome TEXT,
            decision_code TEXT NOT NULL,
            decision_reason TEXT,
            decision_source TEXT NOT NULL,
            event_timestamp TIMESTAMPTZ,
            detected_at TIMESTAMPTZ,
            decided_at TIMESTAMPTZ DEFAULT now() NOT NULL,
            latency_seconds NUMERIC(18, 3),
            requested_contracts NUMERIC(18, 2),
            filled_contracts NUMERIC(18, 2),
            order_ids TEXT[],
            context JSONB DEFAULT '{}'::jsonb NOT NULL
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_copy_decisions_whale_trade
        ON copy_decisions (whale_trade_id)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_copy_decisions_wallet_time
        ON copy_decisions (wallet_address, decided_at DESC)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_copy_decisions_market
        ON copy_decisions (condition_id, outcome, decided_at DESC)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_copy_decisions_source_time
        ON copy_decisions (decision_source, decided_at DESC)
        """
    )


def downgrade():
    op.drop_table("copy_decisions")
