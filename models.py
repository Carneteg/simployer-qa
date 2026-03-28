import uuid
from datetime import datetime, timezone
from typing import Optional, List
from sqlalchemy import (
    String, Integer, Boolean, Text, Numeric,
    ForeignKey, ARRAY, DateTime, ForeignKeyConstraint, Index
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from database import Base


def utcnow():
    return datetime.now(timezone.utc)


class User(Base):
    __tablename__ = "users"

    id:            Mapped[uuid.UUID]     = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email:         Mapped[str]           = mapped_column(String, unique=True, nullable=False)
    password_hash: Mapped[str]           = mapped_column(String, nullable=False)
    org_name:      Mapped[Optional[str]] = mapped_column(String)
    created_at:    Mapped[datetime]      = mapped_column(DateTime(timezone=True), default=utcnow)

    runs:    Mapped[List["Run"]]    = relationship("Run",    back_populates="user")
    tickets: Mapped[List["Ticket"]] = relationship("Ticket", back_populates="user")


class Run(Base):
    __tablename__ = "runs"

    id:             Mapped[uuid.UUID]          = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id:        Mapped[uuid.UUID]          = mapped_column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"))
    status:         Mapped[str]                = mapped_column(String, default="queued")
    days_back:      Mapped[int]                = mapped_column(Integer, nullable=False)
    tickets_total:  Mapped[int]                = mapped_column(Integer, default=0)
    tickets_done:   Mapped[int]                = mapped_column(Integer, default=0)
    churn_count:    Mapped[int]                = mapped_column(Integer, default=0)
    error:          Mapped[Optional[str]]      = mapped_column(Text)
    started_at:     Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    finished_at:    Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at:     Mapped[datetime]           = mapped_column(DateTime(timezone=True), default=utcnow)

    user:        Mapped["User"]             = relationship("User", back_populates="runs")
    evaluations: Mapped[List["Evaluation"]] = relationship("Evaluation", back_populates="run")


class Ticket(Base):
    __tablename__ = "tickets"
    __table_args__ = (
        # ── Performance indexes ───────────────────────────────────────────────
        # Primary filter: list tickets by user
        Index("ix_tickets_user_id", "user_id"),
        # Agents aggregation + agent filter: GROUP BY (user_id, agent_name, group_name)
        Index("ix_tickets_user_agent_group", "user_id", "agent_name", "group_name"),
        # Ticket detail lookups
        Index("ix_tickets_user_id_id", "user_id", "id"),
    )

    id:           Mapped[str]           = mapped_column(String, primary_key=True)
    user_id:      Mapped[uuid.UUID]     = mapped_column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    subject:      Mapped[Optional[str]] = mapped_column(Text)
    agent_name:   Mapped[Optional[str]] = mapped_column(String)
    group_name:   Mapped[Optional[str]] = mapped_column(String)
    status:       Mapped[Optional[int]] = mapped_column(Integer)
    priority:     Mapped[Optional[int]] = mapped_column(Integer)
    csat:         Mapped[Optional[int]] = mapped_column(Integer)
    tags:         Mapped[Optional[List[str]]] = mapped_column(ARRAY(String))
    fr_escalated: Mapped[bool]          = mapped_column(Boolean, default=False)
    nr_escalated: Mapped[bool]          = mapped_column(Boolean, default=False)
    created_at:   Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    resolved_at:  Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    updated_at:   Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    # ── Company / ARR fields (from Freshdesk company record) ─────────────────
    company_id:          Mapped[Optional[str]]   = mapped_column(String)
    company_name:        Mapped[Optional[str]]   = mapped_column(String)
    arr:                 Mapped[Optional[float]]  = mapped_column(Numeric(12, 2))
    planhat_phase:       Mapped[Optional[str]]   = mapped_column(String)
    planhat_health:      Mapped[Optional[int]]   = mapped_column(Integer)
    planhat_segmentation: Mapped[Optional[str]]  = mapped_column(String)

    user:        Mapped["User"]             = relationship("User", back_populates="tickets")
    messages:    Mapped[List["Message"]]    = relationship("Message",    primaryjoin="and_(Message.ticket_id==Ticket.id, Message.user_id==Ticket.user_id)", foreign_keys="[Message.ticket_id, Message.user_id]")
    evaluations: Mapped[List["Evaluation"]] = relationship("Evaluation", primaryjoin="and_(Evaluation.ticket_id==Ticket.id, Evaluation.user_id==Ticket.user_id)", foreign_keys="[Evaluation.ticket_id, Evaluation.user_id]")


class Message(Base):
    __tablename__ = "messages"
    __table_args__ = (
        ForeignKeyConstraint(["ticket_id", "user_id"], ["tickets.id", "tickets.user_id"], ondelete="CASCADE"),
        # Scorecard: load messages for a ticket fast
        Index("ix_messages_ticket_user", "ticket_id", "user_id"),
    )

    id:        Mapped[uuid.UUID]          = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    ticket_id: Mapped[str]                = mapped_column(String, nullable=False)
    user_id:   Mapped[uuid.UUID]          = mapped_column(UUID(as_uuid=True), nullable=False)
    role:      Mapped[str]                = mapped_column(String, nullable=False)
    ts:        Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    body:      Mapped[str]                = mapped_column(Text, nullable=False)


class Evaluation(Base):
    __tablename__ = "evaluations"
    __table_args__ = (
        ForeignKeyConstraint(["ticket_id", "user_id"], ["tickets.id", "tickets.user_id"], ondelete="CASCADE"),
        # ── Performance indexes ───────────────────────────────────────────────
        # Ticket list JOIN: (ticket_id, user_id) — most common join condition
        Index("ix_evals_ticket_user", "ticket_id", "user_id"),
        # Agents aggregation: covering index for GROUP BY + aggregates
        Index("ix_evals_user_score_churn", "user_id", "total_score", "churn_risk_flag"),
        # Run filter: filter evaluations by run
        Index("ix_evals_run_id", "run_id"),
        # Churn-only filter
        Index("ix_evals_user_churn", "user_id", "churn_risk_flag"),
    )

    id:                    Mapped[uuid.UUID]          = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    ticket_id:             Mapped[str]                = mapped_column(String, nullable=False)
    user_id:               Mapped[uuid.UUID]          = mapped_column(UUID(as_uuid=True), nullable=False)
    run_id:                Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("runs.id"))
    total_score:           Mapped[Optional[float]]    = mapped_column(Numeric(5, 2))
    complexity:            Mapped[Optional[str]]      = mapped_column(String)
    sentiment_start:       Mapped[Optional[str]]      = mapped_column(String)
    sentiment_end:         Mapped[Optional[str]]      = mapped_column(String)
    summary:               Mapped[Optional[str]]      = mapped_column(Text)
    churn_risk_flag:       Mapped[bool]               = mapped_column(Boolean, default=False)
    churn_risk_reason:     Mapped[Optional[str]]      = mapped_column(Text)
    churn_confirmed:       Mapped[bool]               = mapped_column(Boolean, default=False)
    contact_problem_flag:  Mapped[bool]               = mapped_column(Boolean, default=False)
    coaching_tip:          Mapped[Optional[str]]      = mapped_column(Text)
    strengths:             Mapped[Optional[List[str]]] = mapped_column(ARRAY(String))
    improvements:          Mapped[Optional[List[str]]] = mapped_column(ARRAY(String))
    scores:                Mapped[Optional[dict]]     = mapped_column(JSONB)
    # ── CX proxy fields ───────────────────────────────────────────────────────
    # Computed at evaluation time from thread length + churn + contact signals.
    # Bad CX = contact_problem_flag OR msg_count > 2 OR churn_risk_flag
    msg_count:             Mapped[Optional[int]]      = mapped_column(Integer)
    cx_bad:                Mapped[bool]               = mapped_column(Boolean, default=False)
    cx_signals:            Mapped[Optional[List[str]]] = mapped_column(ARRAY(String))
    created_at:            Mapped[datetime]           = mapped_column(DateTime(timezone=True), default=utcnow)

    run: Mapped["Run"] = relationship("Run", back_populates="evaluations")
