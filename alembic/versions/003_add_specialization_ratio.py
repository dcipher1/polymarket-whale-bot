"""Add specialization_ratio column to wallets.

Revision ID: 003
Revises: 002
"""
from alembic import op
import sqlalchemy as sa

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "wallets",
        sa.Column("specialization_ratio", sa.Numeric(5, 4), nullable=True),
    )


def downgrade():
    op.drop_column("wallets", "specialization_ratio")
