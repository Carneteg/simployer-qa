"""
GET /categories/        — tag-level churn risk + ARR analysis
GET /categories/groups  — group-level churn risk (same metrics, grouped by group_name)

Both endpoints aggregate across all evaluated tickets for the current user.
Risk levels:
  High  — churn rate >= 30%
  Med   — churn rate 15–29.9%
  Low   — churn rate < 15%
ARR: shown only when Ticket.arr is populated; null rows reported as [ARR unknown].
Minimum 5 tickets per category required to be included (statistical threshold).
"""

import logging
import time
from fastapi import APIRouter, Depends
from sqlalchemy import select, func, case, and_, text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import Ticket, Evaluation, User
from routers.auth import current_user
from services.cache import get as cache_get, set as cache_set

router = APIRouter()
logger = logging.getLogger("simployer.categories")

# Cache TTL — same as agents (5 min); category data changes only after new runs
TTL_CATEGORIES = 300
MIN_TICKETS    = 5   # minimum tickets per tag/group to be included


def _risk_level(churn_pct: float) -> str:
    if churn_pct >= 30:
        return "High"
    if churn_pct >= 15:
        return "Med"
    return "Low"


def _format_row(tag: str, volume: int, churn_count: int,
                arr_at_risk, companies_at_risk: int) -> dict:
    churn_pct = round(churn_count / volume * 100, 1) if volume else 0.0
    return {
        "category":          tag,
        "volume":            volume,
        "churn_count":       churn_count,
        "churn_pct":         churn_pct,
        "risk_level":        _risk_level(churn_pct),
        # ARR: None means the field is present but zero; -1 sentinel means unknown
        "arr_at_risk":       float(arr_at_risk) if arr_at_risk is not None else None,
        "companies_at_risk": int(companies_at_risk or 0),
        "arr_label":         (
            "[ARR unknown]"
            if arr_at_risk is None
            else f"NOK {float(arr_at_risk):,.0f}"
        ),
    }


@router.get("/")
async def list_categories(
    user: User = Depends(current_user),
    db:   AsyncSession = Depends(get_db),
):
    """
    Unnest ticket tags, aggregate churn flags and ARR per tag.
    Returns all tags with >= MIN_TICKETS tickets, sorted by churn_pct desc.
    """
    cache_key = f"categories:{user.id}"
    cached = await cache_get(cache_key)
    if cached is not None:
        logger.debug(f"categories cache HIT for user {user.id}")
        return cached

    t0 = time.time()

    # PostgreSQL unnest() expands the tags array into individual rows.
    # We join Ticket → Evaluation and aggregate per unnested tag.
    sql = text("""
        SELECT
            tag,
            COUNT(DISTINCT t.id)                                      AS volume,
            SUM(CASE WHEN e.churn_risk_flag THEN 1 ELSE 0 END)       AS churn_count,
            SUM(CASE
                    WHEN e.churn_risk_flag AND t.arr IS NOT NULL
                    THEN t.arr ELSE 0
                END)                                                  AS arr_at_risk_sum,
            SUM(CASE
                    WHEN e.churn_risk_flag AND t.arr IS NOT NULL
                    AND t.company_id IS NOT NULL THEN 1 ELSE 0
                END)                                                  AS companies_at_risk,
            -- flag: is ANY arr populated at all for churn tickets in this tag?
            BOOL_OR(
                e.churn_risk_flag AND t.arr IS NOT NULL
            )                                                         AS has_arr_data
        FROM tickets t
        JOIN evaluations e ON e.ticket_id = t.id AND e.user_id = t.user_id
        CROSS JOIN LATERAL UNNEST(COALESCE(t.tags, ARRAY[]::text[])) AS tag
        WHERE t.user_id = :user_id
        GROUP BY tag
        HAVING COUNT(DISTINCT t.id) >= :min_tickets
        ORDER BY (SUM(CASE WHEN e.churn_risk_flag THEN 1 ELSE 0 END)::float /
                  NULLIF(COUNT(DISTINCT t.id), 0)) DESC
    """)

    result = await db.execute(sql, {"user_id": str(user.id), "min_tickets": MIN_TICKETS})
    rows = result.mappings().all()
    ms = round((time.time() - t0) * 1000)
    logger.info(f"categories aggregation: {len(rows)} tags in {ms}ms")

    out = []
    for r in rows:
        # If no ARR data at all for churn tickets in this tag → None (→ [ARR unknown])
        arr_val = float(r["arr_at_risk_sum"]) if r["has_arr_data"] else None
        out.append(_format_row(
            tag=r["tag"],
            volume=int(r["volume"]),
            churn_count=int(r["churn_count"]),
            arr_at_risk=arr_val,
            companies_at_risk=int(r["companies_at_risk"] or 0),
        ))

    # Top 5 by churn_pct (already sorted) — mark them
    for i, row in enumerate(out):
        row["top5"] = i < 5

    payload = {"categories": out, "min_tickets": MIN_TICKETS}
    await cache_set(cache_key, payload, TTL_CATEGORIES)
    return payload


@router.get("/groups")
async def list_groups(
    user: User = Depends(current_user),
    db:   AsyncSession = Depends(get_db),
):
    """
    Group-level churn risk — same metrics as tag view but aggregated by group_name.
    No minimum ticket threshold for groups (groups can be small).
    """
    cache_key = f"categories:groups:{user.id}"
    cached = await cache_get(cache_key)
    if cached is not None:
        return cached

    t0 = time.time()

    result = await db.execute(
        select(
            func.coalesce(Ticket.group_name, "Unknown").label("group_name"),
            func.count(Ticket.id.distinct()).label("volume"),
            func.sum(
                case((Evaluation.churn_risk_flag == True, 1), else_=0)
            ).label("churn_count"),
            func.sum(
                case(
                    (and_(Evaluation.churn_risk_flag == True,
                          Ticket.arr != None), Ticket.arr),
                    else_=0,
                )
            ).label("arr_at_risk_sum"),
            func.bool_or(
                and_(Evaluation.churn_risk_flag == True, Ticket.arr != None)
            ).label("has_arr_data"),
        )
        .join(Evaluation, and_(
            Evaluation.ticket_id == Ticket.id,
            Evaluation.user_id   == Ticket.user_id,
        ))
        .where(Ticket.user_id == user.id)
        .group_by(func.coalesce(Ticket.group_name, "Unknown"))
        .order_by(
            (func.sum(case((Evaluation.churn_risk_flag == True, 1), else_=0))
             / func.nullif(func.count(Ticket.id.distinct()), 0)
            ).desc()
        )
    )
    rows = result.mappings().all()
    ms = round((time.time() - t0) * 1000)
    logger.info(f"groups aggregation: {len(rows)} groups in {ms}ms")

    out = []
    for r in rows:
        arr_val = float(r["arr_at_risk_sum"]) if r["has_arr_data"] else None
        row = _format_row(
            tag=r["group_name"],
            volume=int(r["volume"]),
            churn_count=int(r["churn_count"]),
            arr_at_risk=arr_val,
            companies_at_risk=0,
        )
        out.append(row)

    payload = {"groups": out}
    await cache_set(cache_key, payload, TTL_CATEGORIES)
    return payload
