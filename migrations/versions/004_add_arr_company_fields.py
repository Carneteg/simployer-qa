"""Add ARR and company fields to tickets table

Revision ID: 004_arr_company
Revises: 003_cx_fields
Create Date: 2026-03-28

ARR lives on the Freshdesk company record, not the ticket.
At evaluation time we fetch the company and store:
  company_id          — Freshdesk company ID
  company_name        — company display name
  arr                 — ARR from custom_fields.arr (authoritative)
  planhat_phase       — lifecycle phase (e.g. "Notice Given", "Active")
  planhat_health      — health score 1-10
  planhat_segmentation — segment (e.g. "Scalable", "Enterprise")

This powers:
  - ARR at Risk card on Overview
  - Category churn analysis with revenue context
  - Planhat lifecycle segmentation filters
"""

from alembic import op
import sqlalchemy as sa

revision      = "004_arr_company"
down_revision = "003_cx_fields"
branch_labels = None
depends_on    = None


def upgrade():
    op.add_column("tickets", sa.Column("company_id",           sa.String(),        nullable=True))
    op.add_column("tickets", sa.Column("company_name",         sa.String(),        nullable=True))
    op.add_column("tickets", sa.Column("arr",                  sa.Numeric(12, 2),  nullable=True))
    op.add_column("tickets", sa.Column("planhat_phase",        sa.String(),        nullable=True))
    op.add_column("tickets", sa.Column("planhat_health",       sa.Integer(),       nullable=True))
    op.add_column("tickets", sa.Column("planhat_segmentation", sa.String(),        nullable=True))

    # Index for ARR at Risk queries: user_id + churn_flag + arr
    op.create_index(
        "ix_tickets_user_churn_arr", "tickets",
        ["user_id", "company_id"],
        if_not_exists=True,
    )


def downgrade():
    op.drop_index("ix_tickets_user_churn_arr", table_name="tickets")
    op.drop_column("tickets", "planhat_segmentation")
    op.drop_column("tickets", "planhat_health")
    op.drop_column("tickets", "planhat_phase")
    op.drop_column("tickets", "arr")
    op.drop_column("tickets", "company_name")
    op.drop_column("tickets", "company_id")
