"""
POST /improvements/  — AI-generated improvement recommendations

Head of Customer Care role. Separates findings into:
  - Agent behaviour issues
  - Knowledge / process issues
  - System / operational issues

Output:
  - Priority table (High×2 / Medium×2 / Low×1 minimum, up to 8 rows total)
  - STOP / START / CONTINUE framework
  - Leadership commentary (5–8 sentences on scalability, trust, quality discipline)

Only uses observed data. No generic advice. No assumptions.
"""

import asyncio
import json
import re
import time
import logging
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, func, case, and_, text
from sqlalchemy.ext.asyncio import AsyncSession
from anthropic import AsyncAnthropic

from config import settings
from database import get_db
from models import Ticket, Evaluation, User
from routers.auth import current_user
from services.cache import get as cache_get, set as cache_set

router  = APIRouter()
logger  = logging.getLogger("simployer.improvements")
TTL     = 600   # 10 min cache
_client = AsyncAnthropic(api_key=settings.anthropic_api_key, timeout=45.0)

SYSTEM = (
    "You are a Head of Customer Care reviewing support ticket quality data. "
    "Identify concrete, evidence-based improvements only. "
    "No generic advice. No assumptions beyond what the data shows. "
    "Separate findings by type: agent behaviour, knowledge/process, system/operational. "
    "Return ONLY valid JSON — no markdown, no preamble."
)

PROMPT = """\
### Role
You are a Head of Customer Care reviewing ticket quality.

### Rules
- Only use observed issues in the data below
- No generic advice — every recommendation must cite a specific number
- Separate: agent behavior issues / knowledge-process issues / system-operational issues
- Prioritize based on impact (ARR, churn volume, ticket volume)

### Dataset

**Overall quality**
- Tickets evaluated: {total}
- Avg QA score: {avg_score}/100  (team ceiling: {pct_high}% ≥75, {pct_low}% <55)
- Churn rate: {churn_pct}% — confirmed exits: {confirmed_churn}
- Contact problem rate: {contact_pct}%  (repeat contacts / unresolved first time)
- Bad CX rate: {cx_bad_pct}%
- QA/CX mismatch: {mismatch_count} tickets score well but produce bad CX outcomes
- ARR at risk: NOK {arr_at_risk:,.0f} across {companies_at_risk} companies

**Weakest QA dimensions (1–5 scale)**
{dim_table}

**High-risk agents (churn ≥18.6%, QA <59, ≥3 tickets)**
{high_risk_list}

**Priority coaching agents (≥10 tickets, churn >25%)**
{coaching_list}

**High-churn categories (tags)**
{cat_table}

**Group-level risk**
{group_table}

Return this EXACT JSON:
{{
  "improvements": [
    {{
      "priority": "High",
      "type": "agent_behavior",
      "area": "",
      "what_should_change": "",
      "evidence": "",
      "impact": "",
      "effort": "Low|Medium|High"
    }}
  ],
  "stop": [""],
  "start": [""],
  "continue_doing": [""],
  "leadership_commentary": ""
}}

Rules:
- improvements: 5–8 rows total. Minimum: 2 High, 2 Medium, 1 Low.
- type must be one of: agent_behavior | knowledge_process | system_operational
- what_should_change: one specific, actionable sentence
- evidence: cite a number from the dataset (e.g. "7 agents show churn >25%")
- impact: one sentence on business/customer consequence if not fixed
- effort: Low (days) / Medium (weeks) / High (months)
- stop: 2–4 bullets — specific behaviors observed across tickets that must stop
- start: 2–4 bullets — specific practices missing from the data that should begin
- continue_doing: 2–3 bullets — observed strengths that should be protected
- leadership_commentary: 5–8 sentences on scalability, trust, and quality discipline
  (must reference ≥3 specific numbers from the data)
"""


def _repair(txt: str) -> str:
    txt = txt.strip()
    txt = re.sub(r"^```json\s*", "", txt)
    txt = re.sub(r"```\s*$", "", txt)
    return txt.strip()


@router.post("/")
async def generate_improvements(
    user: User = Depends(current_user),
    db:   AsyncSession = Depends(get_db),
):
    cache_key = f"improvements:{user.id}"
    cached = await cache_get(cache_key)
    if cached is not None:
        logger.debug(f"improvements cache HIT for {user.id}")
        return cached

    t0 = time.time()

    # ── 1. Core ticket metrics (shared with qa_summary) ───────────────────────
    ticket_sql = text("""
        WITH latest AS (
            SELECT DISTINCT ON (ticket_id, user_id)
                ticket_id, user_id, total_score,
                churn_risk_flag, contact_problem_flag, cx_bad, scores
            FROM evaluations WHERE user_id = :uid
            ORDER BY ticket_id, user_id, id DESC
        )
        SELECT
            COUNT(*)                                                   AS total,
            ROUND(AVG(total_score)::numeric,1)                        AS avg_score,
            SUM(CASE WHEN total_score >= 75 THEN 1 ELSE 0 END)        AS high_count,
            SUM(CASE WHEN total_score < 55  THEN 1 ELSE 0 END)        AS low_count,
            SUM(CASE WHEN churn_risk_flag   THEN 1 ELSE 0 END)        AS churn_count,
            SUM(CASE WHEN contact_problem_flag THEN 1 ELSE 0 END)     AS contact_count,
            SUM(CASE WHEN cx_bad            THEN 1 ELSE 0 END)        AS cx_bad_count
        FROM latest
    """)
    tr = (await db.execute(ticket_sql, {"uid": str(user.id)})).mappings().first() or {}
    total         = int(tr.get("total") or 0)
    if not total:
        raise HTTPException(422, "No evaluated tickets found. Run an analysis first.")
    avg_score     = float(tr.get("avg_score") or 0)
    pct_high      = round(int(tr.get("high_count") or 0) / total * 100, 1)
    pct_low       = round(int(tr.get("low_count")  or 0) / total * 100, 1)
    churn_count   = int(tr.get("churn_count")   or 0)
    churn_pct     = round(churn_count / total * 100, 1)
    contact_pct   = round(int(tr.get("contact_count") or 0) / total * 100, 1)
    cx_bad_pct    = round(int(tr.get("cx_bad_count")  or 0) / total * 100, 1)

    # ── 2. ARR + confirmed churn ──────────────────────────────────────────────
    arr_sql = text("""
        WITH latest AS (
            SELECT DISTINCT ON (e.ticket_id, e.user_id)
                e.churn_risk_flag, e.churn_confirmed, t.arr, t.company_id
            FROM evaluations e
            JOIN tickets t ON t.id=e.ticket_id AND t.user_id=e.user_id
            WHERE e.user_id=:uid
            ORDER BY e.ticket_id, e.user_id, e.id DESC
        )
        SELECT
            SUM(CASE WHEN churn_confirmed THEN 1 ELSE 0 END)          AS confirmed_churn,
            SUM(CASE WHEN churn_risk_flag AND arr IS NOT NULL
                     THEN arr ELSE 0 END)                              AS arr_at_risk,
            COUNT(DISTINCT CASE WHEN churn_risk_flag AND company_id IS NOT NULL
                           THEN company_id END)                        AS companies_at_risk
        FROM latest
    """)
    ar = (await db.execute(arr_sql, {"uid": str(user.id)})).mappings().first() or {}
    confirmed_churn  = int(ar.get("confirmed_churn")  or 0)
    arr_at_risk      = float(ar.get("arr_at_risk")     or 0)
    companies_at_risk = int(ar.get("companies_at_risk") or 0)

    # ── 3. Mismatch ───────────────────────────────────────────────────────────
    mismatch_sql = text("""
        WITH latest AS (
            SELECT DISTINCT ON (ticket_id, user_id) cx_bad, total_score
            FROM evaluations WHERE user_id=:uid
            ORDER BY ticket_id, user_id, id DESC
        )
        SELECT
            SUM(CASE WHEN cx_bad THEN 1 ELSE 0 END)                   AS cx_bad_total,
            SUM(CASE WHEN cx_bad AND total_score>=75 THEN 1 ELSE 0 END) AS mismatch
        FROM latest
    """)
    mr = (await db.execute(mismatch_sql, {"uid": str(user.id)})).mappings().first() or {}
    mismatch_count = int(mr.get("mismatch") or 0)

    # ── 4. QA dimension averages ──────────────────────────────────────────────
    dim_sql = text("""
        WITH latest AS (SELECT DISTINCT ON (ticket_id,user_id) scores
            FROM evaluations WHERE user_id=:uid ORDER BY ticket_id,user_id,id DESC)
        SELECT scores FROM latest WHERE scores IS NOT NULL
    """)
    dim_acc: dict = {}
    for row in (await db.execute(dim_sql, {"uid": str(user.id)})).scalars():
        if not isinstance(row, dict): continue
        for k, v in row.items():
            s = (v or {}).get("score") if isinstance(v, dict) else v
            if isinstance(s, (int, float)): dim_acc.setdefault(k, []).append(float(s))
    dim_avgs = sorted([(k, round(sum(v)/len(v), 2)) for k, v in dim_acc.items()], key=lambda x: x[1])
    dim_table = "\n".join(f"  {k}: {avg}/5" for k, avg in dim_avgs[:6]) or "  No data"

    # ── 5. Agents ─────────────────────────────────────────────────────────────
    agent_sql = text("""
        WITH latest AS (
            SELECT DISTINCT ON (e.ticket_id,e.user_id)
                t.agent_name, e.total_score, e.churn_risk_flag
            FROM evaluations e JOIN tickets t ON t.id=e.ticket_id AND t.user_id=e.user_id
            WHERE e.user_id=:uid AND t.agent_name IS NOT NULL
            ORDER BY e.ticket_id,e.user_id,e.id DESC
        )
        SELECT agent_name,
               COUNT(*)                                               AS tc,
               ROUND(AVG(total_score)::numeric,1)                    AS qa,
               SUM(CASE WHEN churn_risk_flag THEN 1 ELSE 0 END)      AS ch
        FROM latest GROUP BY agent_name HAVING COUNT(*)>=3
    """)
    agents = []
    for r in (await db.execute(agent_sql, {"uid": str(user.id)})).mappings():
        tc  = int(r["tc"]); qa = float(r["qa"] or 0); ch = int(r["ch"] or 0)
        agents.append({"name":r["agent_name"],"tickets":tc,"qa":qa,"churn_pct":round(ch/tc*100,1)})

    high_risk  = [a for a in agents if a["churn_pct"]>=18.6 and a["qa"]<59]
    coaching   = [a for a in agents if a["tickets"]>=10 and a["churn_pct"]>25]
    def _astr(lst): return "\n  ".join(f"{a['name']}: QA={a['qa']} churn={a['churn_pct']}% ({a['tickets']}t)" for a in lst) or "  None"

    # ── 6. Category + group tables ────────────────────────────────────────────
    cat_sql = text("""
        WITH latest AS (
            SELECT DISTINCT ON (e.ticket_id,e.user_id)
                e.ticket_id,e.user_id,e.churn_risk_flag,t.tags
            FROM evaluations e JOIN tickets t ON t.id=e.ticket_id AND t.user_id=e.user_id
            WHERE e.user_id=:uid ORDER BY e.ticket_id,e.user_id,e.id DESC
        )
        SELECT tag,
               COUNT(DISTINCT ticket_id)                              AS vol,
               SUM(CASE WHEN churn_risk_flag THEN 1 ELSE 0 END)      AS ch
        FROM latest
        CROSS JOIN LATERAL UNNEST(COALESCE(tags,ARRAY[]::text[])) AS tag
        GROUP BY tag HAVING COUNT(DISTINCT ticket_id)>=5
        ORDER BY (SUM(CASE WHEN churn_risk_flag THEN 1 ELSE 0 END)::float/NULLIF(COUNT(DISTINCT ticket_id),0)) DESC
        LIMIT 12
    """)
    cat_lines = []
    for r in (await db.execute(cat_sql, {"uid": str(user.id)})).mappings():
        vol=int(r["vol"]); ch=int(r["ch"] or 0)
        pct=round(ch/vol*100,1) if vol else 0
        risk="High" if pct>=30 else "Med" if pct>=15 else "Low"
        cat_lines.append(f"  {r['tag']}: {pct}% churn (vol={vol}, {risk})")

    grp_sql = text("""
        WITH latest AS (
            SELECT DISTINCT ON (e.ticket_id,e.user_id)
                COALESCE(t.group_name,'Unknown') AS grp,
                e.churn_risk_flag
            FROM evaluations e JOIN tickets t ON t.id=e.ticket_id AND t.user_id=e.user_id
            WHERE e.user_id=:uid ORDER BY e.ticket_id,e.user_id,e.id DESC
        )
        SELECT grp, COUNT(*) AS vol,
               SUM(CASE WHEN churn_risk_flag THEN 1 ELSE 0 END) AS ch
        FROM latest GROUP BY grp ORDER BY (SUM(CASE WHEN churn_risk_flag THEN 1 ELSE 0 END)::float/NULLIF(COUNT(*),0)) DESC
    """)
    grp_lines = []
    for r in (await db.execute(grp_sql, {"uid": str(user.id)})).mappings():
        vol=int(r["vol"]); ch=int(r["ch"] or 0)
        pct=round(ch/vol*100,1) if vol else 0
        grp_lines.append(f"  {r['grp']}: {pct}% churn (vol={vol})")

    ms = round((time.time()-t0)*1000)
    logger.info(f"improvements aggregation: {ms}ms, {total} tickets")

    # ── 7. Prompt + Sonnet ────────────────────────────────────────────────────
    prompt = PROMPT.format(
        total=total, avg_score=avg_score, pct_high=pct_high, pct_low=pct_low,
        churn_pct=churn_pct, confirmed_churn=confirmed_churn,
        contact_pct=contact_pct, cx_bad_pct=cx_bad_pct,
        mismatch_count=mismatch_count, arr_at_risk=arr_at_risk,
        companies_at_risk=companies_at_risk,
        dim_table=dim_table,
        high_risk_list=_astr(high_risk),
        coaching_list=_astr(coaching),
        cat_table="\n".join(cat_lines) or "  No data",
        group_table="\n".join(grp_lines) or "  No data",
    )

    t1 = time.time()
    try:
        resp = await asyncio.wait_for(
            _client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=2500,
                temperature=0,
                system=SYSTEM,
                messages=[{"role":"user","content":prompt}],
            ),
            timeout=45.0,
        )
    except asyncio.TimeoutError:
        raise HTTPException(504, "Improvements generation timed out. Retry.")

    data = json.loads(_repair(resp.content[0].text))
    logger.info(f"improvements Sonnet: {round((time.time()-t1)*1000)}ms")

    # Attach metadata for frontend KPIs
    data["meta"] = {
        "total": total, "avg_score": avg_score,
        "churn_pct": churn_pct, "confirmed_churn": confirmed_churn,
        "contact_pct": contact_pct, "cx_bad_pct": cx_bad_pct,
        "mismatch_count": mismatch_count, "arr_at_risk": arr_at_risk,
        "companies_at_risk": companies_at_risk,
        "high_risk_agents": len(high_risk), "coaching_agents": len(coaching),
        "dim_worst": dim_avgs[:3] if dim_avgs else [],
    }

    await cache_set(cache_key, data, TTL)
    return data
