import logging
import time
from typing import List, Optional
from fastapi import APIRouter, Depends
from sqlalchemy import select, and_, func, case
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import Ticket, Evaluation, User
from routers.auth import current_user
from services.cache import get as cache_get, set as cache_set, key_agents, key_agent_detail, TTL_AGENTS, TTL_AGENT_DETAIL

router = APIRouter()
logger = logging.getLogger("simployer.agents")

CATS = [
    "clarity_structure", "tone_professionalism", "empathy", "accuracy",
    "resolution_quality", "efficiency", "ownership", "commercial_awareness",
]


@router.get("/")
async def list_agents(
    user: User = Depends(current_user),
    db: AsyncSession = Depends(get_db),
):
    cache_key = key_agents(str(user.id))
    cached = await cache_get(cache_key)
    if cached is not None:
        logger.debug(f"agents cache HIT for user {user.id}")
        return cached

    t0 = time.time()
    result = await db.execute(
        select(
            Ticket.agent_name,
            Ticket.group_name,
            func.count(Evaluation.id).label("ticket_count"),
            func.round(func.avg(Evaluation.total_score), 1).label("avg_score"),
            func.sum(
                case((Evaluation.churn_risk_flag == True, 1), else_=0)
            ).label("churn_count"),
            func.sum(
                case((Evaluation.cx_bad == True, 1), else_=0)
            ).label("cx_bad_count"),
        )
        .join(Evaluation, and_(
            Evaluation.ticket_id == Ticket.id,
            Evaluation.user_id == Ticket.user_id,
        ))
        .where(Ticket.user_id == user.id)
        .group_by(Ticket.agent_name, Ticket.group_name)
        .order_by(func.avg(Evaluation.total_score).asc())  # index ix_evals_user_score_churn covers this
    )
    rows = result.mappings().all()
    ms = round((time.time() - t0) * 1000)
    logger.info(f"agents aggregation: {len(rows)} agents in {ms}ms (cache MISS)")
    result_list = [
        {
            "agent_name": r["agent_name"],
            "group_name": r["group_name"],
            "ticket_count": r["ticket_count"],
            "avg_score": float(r["avg_score"] or 0),
            "churn_count": int(r["churn_count"] or 0),
            "cx_bad_count": int(r["cx_bad_count"] or 0),
            "cx_bad_pct": round(
                int(r["cx_bad_count"] or 0) / int(r["ticket_count"] or 1) * 100, 1
            ),
            "tier": (
                "🟢 Top" if float(r["avg_score"] or 0) >= 75
                else "🟡 Solid" if float(r["avg_score"] or 0) >= 65
                else "🟠 Needs Improvement" if float(r["avg_score"] or 0) >= 55
                else "🔴 At Risk"
            ),
        }
        for r in rows
    ]
    await cache_set(cache_key, result_list, TTL_AGENTS)
    return result_list


@router.get("/{agent_name}")
async def get_agent(
    agent_name: str,
    user: User = Depends(current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(
            Ticket.id.label("ticket_id"),
            Ticket.subject,
            Ticket.group_name,
            Ticket.csat,
            Evaluation.total_score,
            Evaluation.churn_risk_flag,
            Evaluation.coaching_tip,
            Evaluation.scores,
            Evaluation.summary,
        )
        .join(Evaluation, and_(
            Evaluation.ticket_id == Ticket.id,
            Evaluation.user_id == Ticket.user_id,
        ))
        .where(Ticket.user_id == user.id, Ticket.agent_name == agent_name)
        .order_by(Evaluation.total_score.asc())
        .limit(50)
    )
    tickets = result.mappings().all()

    if not tickets:
        return {"agent_name": agent_name, "tickets": [], "cat_averages": {}}

    cat_avgs = {}
    for cat in CATS:
        scores = []
        for t in tickets:
            sc = t["scores"] or {}
            v = (sc.get(cat) or {}).get("score")
            if v is not None:
                scores.append(v)
        cat_avgs[cat] = round(sum(scores) / len(scores), 1) if scores else None

    avg = round(sum(t["total_score"] or 0 for t in tickets) / len(tickets), 1)
    churn_count = sum(1 for t in tickets if t["churn_risk_flag"])

    return {
        "agent_name": agent_name,
        "group_name": tickets[0]["group_name"],
        "ticket_count": len(tickets),
        "avg_score": avg,
        "churn_count": churn_count,
        "cat_averages": cat_avgs,
        "tier": (
            "🟢 Top" if avg >= 75
            else "🟡 Solid" if avg >= 65
            else "🟠 Needs Improvement" if avg >= 55
            else "🔴 At Risk"
        ),
        "tickets": [
            {
                "ticket_id": str(t["ticket_id"]),
                "subject": t["subject"],
                "total_score": t["total_score"],
                "churn_risk_flag": t["churn_risk_flag"],
                "coaching_tip": t["coaching_tip"],
                "csat": t["csat"],
            }
            for t in tickets
        ],
    }
