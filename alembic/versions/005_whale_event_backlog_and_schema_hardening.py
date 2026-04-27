"""Add whale event backlog and schema hardening.

Revision ID: 005
Revises: 004
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP

revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS whale_event_backlog (
            id BIGSERIAL PRIMARY KEY,
            provider TEXT NOT NULL,
            wallet_address TEXT,
            condition_id TEXT,
            token_id TEXT,
            outcome TEXT,
            side TEXT,
            tx_hash TEXT,
            reason TEXT NOT NULL,
            raw_event JSONB DEFAULT '{}'::jsonb NOT NULL,
            created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
            last_attempt_at TIMESTAMPTZ,
            attempts INTEGER DEFAULT 0 NOT NULL,
            resolved_at TIMESTAMPTZ
        )
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'uq_whale_event_backlog_dedup'
                  AND conrelid = 'whale_event_backlog'::regclass
            ) THEN
                ALTER TABLE whale_event_backlog
                ADD CONSTRAINT uq_whale_event_backlog_dedup
                UNIQUE (provider, wallet_address, tx_hash, token_id);
            END IF;
        END $$;
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_whale_event_backlog_open
        ON whale_event_backlog (created_at)
        WHERE resolved_at IS NULL
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_whale_event_backlog_market
        ON whale_event_backlog (condition_id, token_id)
        """
    )

    op.execute("ALTER TABLE whale_positions ADD COLUMN IF NOT EXISTS slug TEXT")

    # Add missing foreign keys as NOT VALID so future writes are protected without
    # blocking this migration on legacy orphan rows. The audit/backfill script
    # validates these after orphan counts are zero.
    _add_not_valid_fk(
        "whale_trades",
        "fk_whale_trades_condition_id_markets",
        "condition_id",
        "markets",
        "condition_id",
        on_delete="CASCADE",
    )
    _add_not_valid_fk(
        "whale_trades",
        "fk_whale_trades_token_id_market_tokens",
        "token_id",
        "market_tokens",
        "token_id",
        on_delete="CASCADE",
    )
    _add_not_valid_fk(
        "market_tokens",
        "fk_market_tokens_condition_id_markets",
        "condition_id",
        "markets",
        "condition_id",
        on_delete="CASCADE",
    )
    _add_not_valid_fk(
        "whale_positions",
        "fk_whale_positions_condition_id_markets",
        "condition_id",
        "markets",
        "condition_id",
        on_delete="CASCADE",
    )
    _add_not_valid_fk(
        "my_trades",
        "fk_my_trades_condition_id_markets",
        "condition_id",
        "markets",
        "condition_id",
    )


def downgrade():
    for table, name in (
        ("my_trades", "fk_my_trades_condition_id_markets"),
        ("whale_positions", "fk_whale_positions_condition_id_markets"),
        ("market_tokens", "fk_market_tokens_condition_id_markets"),
        ("whale_trades", "fk_whale_trades_token_id_market_tokens"),
        ("whale_trades", "fk_whale_trades_condition_id_markets"),
    ):
        op.execute(
            f"""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = '{name}'
                      AND conrelid = '{table}'::regclass
                ) THEN
                    ALTER TABLE {table} DROP CONSTRAINT {name};
                END IF;
            END $$;
            """
        )
    op.drop_table("whale_event_backlog")


def _add_not_valid_fk(
    table: str,
    name: str,
    column: str,
    ref_table: str,
    ref_column: str,
    *,
    on_delete: str | None = None,
) -> None:
    on_delete_sql = f" ON DELETE {on_delete}" if on_delete else ""
    op.execute(
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint c
                JOIN pg_attribute a
                  ON a.attrelid = c.conrelid
                 AND a.attnum = ANY(c.conkey)
                JOIN pg_class r
                  ON r.oid = c.confrelid
                JOIN pg_namespace rn
                  ON rn.oid = r.relnamespace
                WHERE c.contype = 'f'
                  AND c.conrelid = '{table}'::regclass
                  AND a.attname = '{column}'
                  AND r.relname = '{ref_table}'
            ) THEN
                ALTER TABLE {table}
                ADD CONSTRAINT {name}
                FOREIGN KEY ({column})
                REFERENCES {ref_table} ({ref_column}){on_delete_sql}
                NOT VALID;
            END IF;
        END $$;
        """
    )
