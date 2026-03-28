import uuid
from typing import Optional, List

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import Ticket, Evaluation, Message, User
from routers.auth import current_user

router = APIRouter()


class TicketOut(BaseModel):
    ticket_id: str
    subject: Optional[str]
    agent_name: Optional[str]
    group_name: Optional[str]
    tags: Optional[List[str]]
    csat: Optional[int]
    created_at: Optional[str]
    resolved_at: Optional[str]
    total_score: Optional[float]
    complexity: Optional[str]
    churn_risk_flag: bool
    churn_risk_reason: Optional[str]
    churn_confirmed: bool          # True when salesforce tag present — contract terminated
    contact_problem_flag: bool
    coaching_tip: Optional[str]
    summary: Optional[str]
    scores: Optional[dict]
    strengths: Optional[List[str]]
    improvements: Optional[List[str]]
    # CX proxy fields
    msg_count: Optional[int]       # total messages in thread
    cx_bad: bool                   # bad CX = contact_problem OR high_msgs OR churn_flag
    cx_signals: Optional[List[str]] # which signals fired


@router.get("/", response_model=List[TicketOut])
async def list_tickets(
    run_id: Optional[uuid.UUID] = Query(None),
    churn_only: bool = Query(False),
    cx_bad_only: bool = Query(False),
    limit: int = Query(100, le=1000),
    offset: int = Query(0),
    user: User = Depends(current_user),
    db: AsyncSession = Depends(get_db),
):
    q = (
        select(
            Ticket.id.label("ticket_id"),
            Ticket.subject,
            Ticket.agent_name,
            Ticket.group_name,
            Ticket.tags,
            Ticket.csat,
            Ticket.created_at,
            Ticket.resolved_at,
            Evaluation.total_score,
            Evaluation.complexity,
            Evaluation.churn_risk_flag,
            Evaluation.churn_risk_reason,
            Evaluation.contact_problem_flag,
            Evaluation.coaching_tip,
            Evaluation.summary,
            Evaluation.scores,
            Evaluation.strengths,
            Evaluation.improvements,
            Evaluation.msg_count,
            Evaluation.cx_bad,
            Evaluation.cx_signals,
        )
        .join(Evaluation, and_(
            Evaluation.ticket_id == Ticket.id,
            Evaluation.user_id == Ticket.user_id,
        ))
        .where(Ticket.user_id == user.id)
    )

    if run_id:
        q = q.where(Evaluation.run_id == run_id)
    if churn_only:
        q = q.where(Evaluation.churn_risk_flag == True)
    if cx_bad_only:
        q = q.where(Evaluation.cx_bad == True)

    q = q.order_by(Evaluation.total_score.asc()).limit(limit).offset(offset)

    result = await db.execute(q)
    rows = result.mappings().all()

    return [
        TicketOut(
            ticket_id=str(r["ticket_id"]),
            subject=r["subject"],
            agent_name=r["agent_name"],
            group_name=r["group_name"],
            tags=r["tags"] or [],
            csat=r["csat"],
            created_at=str(r["created_at"] or "")[:10],
            resolved_at=str(r["resolved_at"] or "")[:10],
            total_score=r["total_score"],
            complexity=r["complexity"],
            churn_risk_flag=bool(r["churn_risk_flag"]),
            churn_risk_reason=r["churn_risk_reason"],
            contact_problem_flag=bool(r["contact_problem_flag"]),
            coaching_tip=r["coaching_tip"],
            summary=r["summary"],
            scores=r["scores"],
            strengths=r["strengths"],
            improvements=r["improvements"],
            msg_count=r["msg_count"],
            cx_bad=bool(r["cx_bad"]) if r["cx_bad"] is not None else False,
            cx_signals=r["cx_signals"] or [],
        )
        for r in rows
    ]
    await cache_set(cache_key, [t.model_dump() for t in tickets_out], TTL_TICKETS)
    return tickets_out


@router.get("/{ticket_id}/messages")
async def get_messages(
    ticket_id: str,
    user: User = Depends(current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Message)
        .where(Message.ticket_id == ticket_id, Message.user_id == user.id)
        .order_by(Message.ts.asc())
    )
    msgs = result.scalars().all()
    return [
        {"role": m.role, "ts": str(m.ts or "")[:16].replace("T", " "), "body": m.body}
        for m in msgs
    ]


@router.get("/debug-freshdesk")
async def debug_freshdesk():
    """Debug: show raw Freshdesk ticket fields + agent/group cache status."""
    import httpx
    import asyncio
    from config import settings
    from services.freshdesk import _load_agents, _load_groups
    try:
        agents, groups = await asyncio.gather(_load_agents(), _load_groups())
        url = (
            f"https://{settings.freshdesk_domain}/api/v2/tickets"
            f"?per_page=1&include=stats&include=requester&order_by=updated_at&order_type=desc"
        )
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(url, auth=(settings.freshdesk_api_key, "X"))
            data = r.json() if r.status_code == 200 else []
        t = data[0] if data else {}
        rid = t.get("responder_id")
        return {
            "status": r.status_code,
            "ticket_id": t.get("id"),
            "responder_id": rid,
            "agent_name_resolved": agents.get(rid) if rid else None,
            "group_id": t.get("group_id"),
            "group_name_resolved": groups.get(t.get("group_id")) if t.get("group_id") else None,
            "agent_cache_size": len(agents),
            "group_cache_size": len(groups),
            "ticket_keys": sorted(t.keys()) if t else [],
        }
    except Exception as e:
        return {"error": str(e)}
