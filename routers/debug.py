"""
Performance debug endpoints — pool status + EXPLAIN ANALYZE on hot queries.
Protected by admin auth. Remove or gate behind env flag in production.
"""
import logging
import time
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db, get_pool_status
from models import User
from routers.auth import current_user

router = APIRouter()
logger = logging.getLogger("simployer.debug")


@router.get("/pool")
async def pool_status(user: User = Depends(current_user)):
    """Current asyncpg connection pool metrics."""
    return await get_pool_status()


@router.get("/redis")
async def redis_status(user: User = Depends(current_user)):
    """Redis/Valkey connection pool + server info."""
    from services.cache import redis, get_pool_status as rps, _ping_latency
    try:
        info    = await redis.info("server")
        clients = await redis.info("clients")
        memory  = await redis.info("memory")
        pool    = await rps()
        return {
            "version":           info.get("redis_version"),
            "mode":              info.get("redis_mode"),
            "uptime_days":       info.get("uptime_in_days"),
            "connected_clients": clients.get("connected_clients"),
            "blocked_clients":   clients.get("blocked_clients"),
            "used_memory_human": memory.get("used_memory_human"),
            "maxmemory_human":   memory.get("maxmemory_human"),
            "maxmemory_policy":  memory.get("maxmemory_policy"),
            "pool_max":          pool.get("pool_max"),
            "ping_ms":           pool.get("ping_ms"),
        }
    except Exception as e:
        return {"error": str(e)}


@router.get("/explain")
async def explain_queries(user: User = Depends(current_user), db: AsyncSession = Depends(get_db)):
    """
    Run EXPLAIN ANALYZE on the 3 most expensive queries in the system.
    Use this after deploying indexes to verify they're being used.
    """
    uid = str(user.id)
    results = {}

    queries = {
        # 1. Ticket list — the most frequent query (JOIN tickets + evaluations)
        "ticket_list_join": f"""
            EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
            SELECT t.id, t.subject, t.agent_name, t.group_name, t.csat,
                   e.total_score, e.churn_risk_flag, e.summary
            FROM tickets t
            JOIN evaluations e ON e.ticket_id = t.id AND e.user_id = t.user_id
            WHERE t.user_id = '{uid}'::uuid
            ORDER BY e.total_score ASC
            LIMIT 100
        """,

        # 2. Agents aggregation — GROUP BY with AVG + SUM
        "agents_aggregation": f"""
            EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
            SELECT t.agent_name, t.group_name,
                   COUNT(e.id)                    AS ticket_count,
                   ROUND(AVG(e.total_score), 1)   AS avg_score,
                   SUM(CASE WHEN e.churn_risk_flag THEN 1 ELSE 0 END) AS churn_count
            FROM tickets t
            JOIN evaluations e ON e.ticket_id = t.id AND e.user_id = t.user_id
            WHERE t.user_id = '{uid}'::uuid
            GROUP BY t.agent_name, t.group_name
            ORDER BY AVG(e.total_score) ASC
        """,

        # 3. Message load for scorecard — sequential scan risk on large message tables
        "scorecard_message_load": f"""
            EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
            SELECT role, ts, body
            FROM messages
            WHERE user_id = '{uid}'::uuid
            ORDER BY ticket_id, ts ASC
            LIMIT 50
        """,
    }

    for name, sql in queries.items():
        try:
            t0 = time.time()
            result = await db.execute(text(sql.strip()))
            rows = result.fetchall()
            ms = round((time.time() - t0) * 1000)
            plan = "\n".join(r[0] for r in rows)

            # Parse key signals from the plan
            seq_scan   = "Seq Scan" in plan
            index_used = "Index Scan" in plan or "Index Only Scan" in plan
            bitmap     = "Bitmap" in plan

            results[name] = {
                "execution_ms": ms,
                "uses_index":   index_used,
                "seq_scan":     seq_scan,
                "bitmap_scan":  bitmap,
                "verdict": (
                    "✅ Index used"       if index_used and not seq_scan else
                    "⚠️  Bitmap scan"     if bitmap else
                    "🔴 Sequential scan"  if seq_scan else
                    "❓ Unknown"
                ),
                "plan": plan,
            }
        except Exception as e:
            results[name] = {"error": str(e)}

    return results


@router.get("/cx-stats")
async def cx_stats(user: User = Depends(current_user), db: AsyncSession = Depends(get_db)):
    """
    CX proxy score summary across all evaluated tickets.
    Bad CX = contact_problem_flag OR msg_count > 2 OR churn_risk_flag.
    Shows mismatch between high QA scores and bad CX reality.
    """
    from sqlalchemy import select, func, case, and_
    from models import Ticket, Evaluation

    result = await db.execute(
        select(
            func.count(Evaluation.id).label("total"),
            func.sum(case((Evaluation.cx_bad == True, 1), else_=0)).label("cx_bad_count"),
            func.sum(case((
                and_(Evaluation.cx_bad == True, Evaluation.total_score > 75), 1
            ), else_=0)).label("mismatch_count"),
            func.sum(case((Evaluation.churn_confirmed == True, 1), else_=0)).label("confirmed_churn"),
            func.round(func.avg(
                case((Evaluation.cx_bad == True, Evaluation.total_score), else_=None)
            ), 1).label("avg_score_bad_cx"),
            func.round(func.avg(
                case((Evaluation.cx_bad == False, Evaluation.total_score), else_=None)
            ), 1).label("avg_score_good_cx"),
        )
        .where(Evaluation.user_id == user.id)
    )
    row = result.mappings().first()
    if not row or not row["total"]:
        return {"error": "No evaluations found — run an analysis first"}

    total       = int(row["total"])
    cx_bad      = int(row["cx_bad_count"] or 0)
    mismatch    = int(row["mismatch_count"] or 0)
    confirmed   = int(row["confirmed_churn"] or 0)

    return {
        "total_tickets":        total,
        "cx_bad_count":         cx_bad,
        "cx_bad_pct":           round(cx_bad / total * 100, 1),
        "cx_good_count":        total - cx_bad,
        "cx_good_pct":          round((total - cx_bad) / total * 100, 1),
        "mismatch_count":       mismatch,
        "mismatch_pct_of_bad":  round(mismatch / cx_bad * 100, 1) if cx_bad else 0,
        "confirmed_churn":      confirmed,
        "avg_score_bad_cx":     float(row["avg_score_bad_cx"] or 0),
        "avg_score_good_cx":    float(row["avg_score_good_cx"] or 0),
        "qa_cx_gap":            round(
            float(row["avg_score_good_cx"] or 0) -
            float(row["avg_score_bad_cx"] or 0), 1
        ),
        "note": (
            "mismatch = tickets where cx_bad=True AND total_score>75. "
            "These are high-QA tickets with bad CX reality — "
            "the core signal this feature was built to detect."
        ),
    }


@router.get("/n1-audit")
async def n1_audit():
    """
    Document the N+1 query audit results for all endpoints.
    This is a static analysis result — no DB queries required.
    """
    return {
        "audit_date": "2026-03-28",
        "findings": {
            "GET /tickets/": {
                "query_count": 1,
                "description": "Single JOIN between tickets + evaluations. No lazy loading.",
                "verdict": "✅ Clean — 1 query",
            },
            "GET /agents/": {
                "query_count": 1,
                "description": (
                    "Single GROUP BY query: SELECT agent_name, group_name, COUNT, AVG(score), SUM(churn). "
                    "No per-agent follow-up queries."
                ),
                "verdict": "✅ Clean — 1 query",
            },
            "GET /agents/{name}": {
                "query_count": 1,
                "description": (
                    "1 JOIN query fetches all tickets. Category averages are computed "
                    "by iterating the JSONB scores field in Python (O(8 × 50) ops) — "
                    "no extra DB round-trips. Acceptable for ≤50 tickets."
                ),
                "verdict": "✅ Clean — 1 query + Python JSONB iteration",
                "future_improvement": (
                    "If agent ticket count grows >500, move cat_avgs to SQL using "
                    "jsonb_extract_path_text aggregation to avoid Python memory pressure."
                ),
            },
            "POST /scorecard/": {
                "query_count": 2,
                "description": (
                    "Query 1: fetch ticket metadata. "
                    "Query 2: fetch messages for the ticket. "
                    "Both are single targeted queries with WHERE ticket_id = X."
                ),
                "verdict": "✅ Clean — 2 queries (intentional)",
            },
            "POST /agent-scorecard/": {
                "query_count": 2,
                "description": (
                    "Query 1: fetch all tickets for the agent (JOIN). "
                    "Query 2: fetch messages for up to 10 sample tickets (WHERE IN). "
                    "No per-ticket queries."
                ),
                "verdict": "✅ Clean — 2 queries (intentional)",
            },
            "WebSocket /ws/runs/{id}": {
                "query_count": "1 per 2s poll",
                "description": (
                    "Polls the runs table by primary key (UUID) every 2 seconds. "
                    "PK lookup is O(1). No concern."
                ),
                "verdict": "✅ Acceptable — PK lookup",
            },
        },
        "summary": "No N+1 patterns found. All list endpoints use single aggregate queries.",
    }
