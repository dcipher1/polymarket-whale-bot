"""Add FAILED to fill_status check constraint.

Revision ID: 004
Revises: 003
"""
from alembic import op

revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint("ck_my_trades_fill_status", "my_trades", type_="check")
    op.create_check_constraint(
        "ck_my_trades_fill_status",
        "my_trades",
        "fill_status IN ('PAPER', 'PENDING', 'PARTIAL', 'FILLED', 'CANCELLED', 'FAILED')",
    )


def downgrade():
    op.drop_constraint("ck_my_trades_fill_status", "my_trades", type_="check")
    op.create_check_constraint(
        "ck_my_trades_fill_status",
        "my_trades",
        "fill_status IN ('PAPER', 'PENDING', 'PARTIAL', 'FILLED', 'CANCELLED')",
    )
