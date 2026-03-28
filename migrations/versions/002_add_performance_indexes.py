"""Add performance indexes for ticket list, agents aggregation, scorecard queries

Revision ID: 002_perf_indexes
Revises: 001_initial_schema
Create Date: 2026-03-28
"""

from alembic import op

revision = "002_perf_indexes"
down_revision = "001_initial_schema"
branch_labels = None
depends_on = None


def upgrade():
    # tickets table
    op.create_index("ix_tickets_user_id",         "tickets",     ["user_id"],                         if_not_exists=True)
    op.create_index("ix_tickets_user_agent_group", "tickets",    ["user_id", "agent_name", "group_name"], if_not_exists=True)
    op.create_index("ix_tickets_user_id_id",       "tickets",    ["user_id", "id"],                   if_not_exists=True)

    # evaluations table
    op.create_index("ix_evals_ticket_user",        "evaluations", ["ticket_id", "user_id"],            if_not_exists=True)
    op.create_index("ix_evals_user_score_churn",   "evaluations", ["user_id", "total_score", "churn_risk_flag"], if_not_exists=True)
    op.create_index("ix_evals_run_id",             "evaluations", ["run_id"],                          if_not_exists=True)
    op.create_index("ix_evals_user_churn",         "evaluations", ["user_id", "churn_risk_flag"],      if_not_exists=True)

    # messages table
    op.create_index("ix_messages_ticket_user",     "messages",    ["ticket_id", "user_id"],            if_not_exists=True)


def downgrade():
    op.drop_index("ix_tickets_user_id",         table_name="tickets")
    op.drop_index("ix_tickets_user_agent_group", table_name="tickets")
    op.drop_index("ix_tickets_user_id_id",       table_name="tickets")
    op.drop_index("ix_evals_ticket_user",        table_name="evaluations")
    op.drop_index("ix_evals_user_score_churn",   table_name="evaluations")
    op.drop_index("ix_evals_run_id",             table_name="evaluations")
    op.drop_index("ix_evals_user_churn",         table_name="evaluations")
    op.drop_index("ix_messages_ticket_user",     table_name="messages")
