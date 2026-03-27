"""Initial schema

Revision ID: 001
Create Date: 2026-01-01
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = '001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table('users',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('email', sa.String(), nullable=False),
        sa.Column('password_hash', sa.String(), nullable=False),
        sa.Column('org_name', sa.String(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('email'),
    )

    op.create_table('runs',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('user_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('status', sa.String(), nullable=True),
        sa.Column('days_back', sa.Integer(), nullable=False),
        sa.Column('tickets_total', sa.Integer(), nullable=True),
        sa.Column('tickets_done', sa.Integer(), nullable=True),
        sa.Column('churn_count', sa.Integer(), nullable=True),
        sa.Column('error', sa.Text(), nullable=True),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('finished_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_runs_user_id_created_at', 'runs', ['user_id', 'created_at'])

    op.create_table('tickets',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('user_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('subject', sa.Text(), nullable=True),
        sa.Column('agent_name', sa.String(), nullable=True),
        sa.Column('group_name', sa.String(), nullable=True),
        sa.Column('status', sa.Integer(), nullable=True),
        sa.Column('priority', sa.Integer(), nullable=True),
        sa.Column('csat', sa.Integer(), nullable=True),
        sa.Column('tags', postgresql.ARRAY(sa.String()), nullable=True),
        sa.Column('fr_escalated', sa.Boolean(), nullable=True),
        sa.Column('nr_escalated', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('resolved_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id', 'user_id'),
    )
    op.create_index('ix_tickets_user_id_created_at', 'tickets', ['user_id', 'created_at'])

    op.create_table('messages',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('ticket_id', sa.String(), nullable=False),
        sa.Column('user_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('role', sa.String(), nullable=False),
        sa.Column('ts', sa.DateTime(timezone=True), nullable=True),
        sa.Column('body', sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(['ticket_id', 'user_id'], ['tickets.id', 'tickets.user_id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_messages_ticket_id', 'messages', ['ticket_id', 'user_id'])

    op.create_table('evaluations',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('ticket_id', sa.String(), nullable=False),
        sa.Column('user_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('run_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('total_score', sa.Numeric(5, 2), nullable=True),
        sa.Column('complexity', sa.String(), nullable=True),
        sa.Column('sentiment_start', sa.String(), nullable=True),
        sa.Column('sentiment_end', sa.String(), nullable=True),
        sa.Column('summary', sa.Text(), nullable=True),
        sa.Column('churn_risk_flag', sa.Boolean(), nullable=True),
        sa.Column('churn_risk_reason', sa.Text(), nullable=True),
        sa.Column('contact_problem_flag', sa.Boolean(), nullable=True),
        sa.Column('coaching_tip', sa.Text(), nullable=True),
        sa.Column('strengths', postgresql.ARRAY(sa.String()), nullable=True),
        sa.Column('improvements', postgresql.ARRAY(sa.String()), nullable=True),
        sa.Column('scores', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['run_id'], ['runs.id']),
        sa.ForeignKeyConstraint(['ticket_id', 'user_id'], ['tickets.id', 'tickets.user_id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_evaluations_run_id', 'evaluations', ['run_id'])
    op.create_index('ix_evaluations_ticket_user', 'evaluations', ['ticket_id', 'user_id'])


def downgrade() -> None:
    op.drop_table('evaluations')
    op.drop_table('messages')
    op.drop_table('tickets')
    op.drop_table('runs')
    op.drop_table('users')
