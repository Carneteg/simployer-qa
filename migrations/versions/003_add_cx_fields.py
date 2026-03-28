"""Add CX proxy fields and churn_confirmed to evaluations table

Revision ID: 003_cx_fields
Revises: 002_perf_indexes
Create Date: 2026-03-28

New columns on evaluations:
  msg_count      INT        — total messages in the ticket thread (agent + customer)
  cx_bad         BOOLEAN    — computed: contact_problem OR msg_count>2 OR churn_risk_flag
  cx_signals     TEXT[]     — which signals fired: ['contact_problem','high_msgs','churn_flag']
  churn_confirmed BOOLEAN   — True when salesforce tag present = verified contract termination

Reasoning:
  Data analysis of 98 tickets showed 91.8% bad CX using the proxy definition.
  Storing msg_count at evaluation time avoids N+1 message fetches on every
  dashboard load. cx_bad and cx_signals make the mismatch analysis (high QA
  score + bad CX) queryable directly from the evaluations table.
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY

revision      = "003_cx_fields"
down_revision = "002_perf_indexes"
branch_labels = None
depends_on    = None


def upgrade():
    op.add_column("evaluations", sa.Column(
        "msg_count", sa.Integer(), nullable=True
    ))
    op.add_column("evaluations", sa.Column(
        "cx_bad", sa.Boolean(), nullable=False, server_default="false"
    ))
    op.add_column("evaluations", sa.Column(
        "cx_signals", ARRAY(sa.String()), nullable=True
    ))
    op.add_column("evaluations", sa.Column(
        "churn_confirmed", sa.Boolean(), nullable=False, server_default="false"
    ))

    # Index to support cx_bad filter on the tickets tab and agents aggregation
    op.create_index(
        "ix_evals_user_cx_bad", "evaluations", ["user_id", "cx_bad"],
        if_not_exists=True,
    )


def downgrade():
    op.drop_index("ix_evals_user_cx_bad", table_name="evaluations")
    op.drop_column("evaluations", "churn_confirmed")
    op.drop_column("evaluations", "cx_signals")
    op.drop_column("evaluations", "cx_bad")
    op.drop_column("evaluations", "msg_count")
