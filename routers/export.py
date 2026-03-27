import uuid
import json
import csv
import io
from typing import Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import Ticket, Evaluation, Message, User
from routers.auth import current_user
from services.exporter import build_excel

router = APIRouter()


async def _load_data(db, user_id, run_id=None):
    q = (
        select(
            Ticket.id.label("ticket_id"),
            Ticket.subject, Ticket.agent_name, Ticket.group_name,
            Ticket.csat, Ticket.created_at, Ticket.resolved_at,
            Evaluation.total_score, Evaluation.complexity,
            Evaluation.churn_risk_flag, Evaluation.churn_risk_reason,
            Evaluation.contact_problem_flag, Evaluation.coaching_tip,
            Evaluation.summary, Evaluation.scores,
            Evaluation.strengths, Evaluation.improvements,
        )
        .join(Evaluation, and_(
            Evaluation.ticket_id == Ticket.id,
            Evaluation.user_id == Ticket.user_id,
        ))
        .where(Ticket.user_id == user_id)
    )
    if run_id:
        q = q.where(Evaluation.run_id == run_id)

    result = await db.execute(q.order_by(Ticket.created_at.desc()))
    evals = [dict(r) for r in result.mappings().all()]

    msg_result = await db.execute(
        select(Message)
        .where(Message.user_id == user_id)
        .order_by(Message.ticket_id, Message.ts)
    )
    messages = [
        {"ticket_id": m.ticket_id, "role": m.role, "ts": m.ts, "body": m.body}
        for m in msg_result.scalars().all()
    ]
    return evals, messages


@router.get("/xlsx")
async def export_xlsx(
    run_id: Optional[uuid.UUID] = Query(None),
    user: User = Depends(current_user),
    db: AsyncSession = Depends(get_db),
):
    evals, messages = await _load_data(db, user.id, run_id)
    xlsx_bytes = build_excel(evals, messages)
    filename = f"simployer_qa_{str(user.id)[:8]}.xlsx"
    return StreamingResponse(
        io.BytesIO(xlsx_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/json")
async def export_json(
    run_id: Optional[uuid.UUID] = Query(None),
    user: User = Depends(current_user),
    db: AsyncSession = Depends(get_db),
):
    evals, _ = await _load_data(db, user.id, run_id)
    # Make serialisable
    for e in evals:
        e["ticket_id"] = str(e["ticket_id"])
        e["created_at"] = str(e.get("created_at") or "")
        e["resolved_at"] = str(e.get("resolved_at") or "")
    return StreamingResponse(
        io.BytesIO(json.dumps(evals, indent=2, default=str).encode()),
        media_type="application/json",
        headers={"Content-Disposition": 'attachment; filename="simployer_qa.json"'},
    )


@router.get("/csv")
async def export_csv(
    run_id: Optional[uuid.UUID] = Query(None),
    user: User = Depends(current_user),
    db: AsyncSession = Depends(get_db),
):
    evals, _ = await _load_data(db, user.id, run_id)
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=[
        "ticket_id", "subject", "agent_name", "group_name", "csat",
        "total_score", "churn_risk_flag", "churn_risk_reason",
        "coaching_tip", "summary",
    ])
    writer.writeheader()
    for e in evals:
        writer.writerow({
            "ticket_id": str(e["ticket_id"]),
            "subject": e.get("subject", ""),
            "agent_name": e.get("agent_name", ""),
            "group_name": e.get("group_name", ""),
            "csat": e.get("csat", ""),
            "total_score": e.get("total_score", ""),
            "churn_risk_flag": e.get("churn_risk_flag", False),
            "churn_risk_reason": e.get("churn_risk_reason", ""),
            "coaching_tip": e.get("coaching_tip", ""),
            "summary": e.get("summary", ""),
        })
    return StreamingResponse(
        io.BytesIO(buf.getvalue().encode()),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="simployer_qa.csv"'},
    )
