"""
POST /qa-summary/  — AI-generated leadership QA summary

Aggregates live DB data (tickets, agents, categories, CX metrics, ARR),
sends it to Claude Sonnet as a structured dataset, and returns a JSON
leadership report matching the exact template spec:
  - 7-row QA Summary table
  - Executive Summary (max 5 bullets)
  - Management Verdict (3-4 sentences)

Rules enforced in prompt:
  - Only available data used
  - No assumptions
  - Focus on patterns and risks
  - Operational and actionable
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

router = APIRouter()
logger = logging.getLogger("simployer.qa_summary")
TTL = 600  # 10 min — heavy Sonnet call, cache aggressively

_client = AsyncAnthropic(api_key=settings.anthropic_api_key, timeout=55.0)

SYSTEM = (
    "You are generating a leadership QA summary for a B2B SaaS support team. "
    "Use ONLY the data provided. No assumptions. Focus on patterns and risks. "
    "Be operational and actionable. Return ONLY valid JSON — no markdown, no preamble."
)

PROMPT = """\
### Role
You are generating a leadership QA summary.

### Rules
- Only use available data
- No assumptions
- Focus on patterns and risks
- Keep it operational and actionable

### Input Dataset

**Overall (500 loaded tickets)**
- Avg QA score: {avg_score}/100
- Score distribution: {pct_high}% ≥75 ("high") · {pct_mid}% 55–74 ("mid") · {pct_low}% <55 ("low")
- Churn risk rate: {churn_pct}% ({churn_count} tickets) — confirmed terminations: {confirmed_churn}
- Contact problem flags: {contact_pct}% of tickets
- Bad CX rate: {cx_bad_pct}%
- QA/CX mismatch tickets (high QA, bad CX outcome): {mismatch_count} ({mismatch_pct}% of bad CX)
- ARR at risk: NOK {arr_at_risk:,.0f} across {companies_at_risk} companies

**QA Dimension averages (1–5 scale)**
{dim_table}

**Agent risk breakdown (≥3 tickets)**
Total agents: {agent_total}
High risk (churn ≥18.6% AND QA <59): {high_risk_count} agents
  {high_risk_list}
QA/Churn mismatch (churn ≥18.6% AND QA ≥70): {mismatch_agent_count} agents
  {mismatch_agent_list}
Low risk (churn <18.6% AND QA ≥65): {low_risk_count} agents
  {low_risk_list}
Priority coaching (≥10 tickets, churn >25%):
  {coaching_list}

**Category churn (tags with ≥5 tickets)**
{cat_table}

Return this EXACT JSON (no other text):
{{
  "qa_table": {{
    "overall_quality_level":  "",
    "main_strengths":         "",
    "main_weaknesses":        "",
    "customer_risk":          "",
    "operational_risk":       "",
    "coaching_need":          "",
    "recommended_next_step":  ""
  }},
  "executive_summary": ["", "", "", "", ""],
  "management_verdict": ""
}}

Field rules:
- qa_table values: 1–3 sentences each, data-grounded, no fluff
- executive_summary: exactly 5 bullets, each one sentence, must cite a specific number from the data
- management_verdict: 3–4 sentences assessing whether quality is scalable or fragile
  (reference at least 2 specific data points)
"""


def _repair(txt: str) -> str:
    txt = txt.strip()
    txt = re.sub(r"^```json\s*", "", txt)
    txt = re.sub(r"```\s*$", "", txt)
    return txt.strip()


@router.post("/")
async def generate_qa_summary(
    user: User = Depends(current_user),
    db: AsyncSession = Depends(get_db),
):
    cache_key = f"qa_summary:{user.id}"
    cached = await cache_get(cache_key)
    if cached is not None:
        logger.debug(f"qa_summary cache HIT for user {user.id}")
        return cached

    t0 = time.time()

    # ── 1+2+3. Run ticket metrics, ARR, and mismatch in parallel ─────────────
    t_res, arr_res_p, mismatch_res_p = await asyncio.gather(
        db.execute(text("""
            WITH latest AS (
                SELECT DISTINCT ON (ticket_id, user_id)
                    ticket_id, user_id, total_score, churn_risk_flag,
                    contact_problem_flag, cx_bad, scores
                FROM evaluations
                WHERE user_id = :uid
                ORDER BY ticket_id, user_id, id DESC
            )
            SELECT
                COUNT(*)                                                   AS total,
                ROUND(AVG(total_score)::numeric, 1)                       AS avg_score,
                SUM(CASE WHEN total_score >= 75 THEN 1 ELSE 0 END)        AS high_count,
                SUM(CASE WHEN total_score >= 55 AND total_score < 75
                         THEN 1 ELSE 0 END)                               AS mid_count,
                SUM(CASE WHEN total_score < 55  THEN 1 ELSE 0 END)        AS low_count,
                SUM(CASE WHEN churn_risk_flag   THEN 1 ELSE 0 END)        AS churn_count,
                SUM(CASE WHEN contact_problem_flag THEN 1 ELSE 0 END)     AS contact_count,
                SUM(CASE WHEN cx_bad            THEN 1 ELSE 0 END)        AS cx_bad_count
            FROM latest
        """), {"uid": str(user.id)}),
        db.execute(text("""
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
        """), {"uid": str(user.id)}),
        db.execute(text("""
            WITH latest AS (
                SELECT DISTINCT ON (ticket_id, user_id) cx_bad, total_score
                FROM evaluations WHERE user_id=:uid
                ORDER BY ticket_id, user_id, id DESC
            )
            SELECT
                SUM(CASE WHEN cx_bad THEN 1 ELSE 0 END)                   AS cx_bad_total,
                SUM(CASE WHEN cx_bad AND total_score>=75 THEN 1 ELSE 0 END) AS mismatch
            FROM latest
        """), {"uid": str(user.id)}),
    )
    tr = t_res.mappings().first() or {}

    total        = int(tr.get("total") or 0)
    avg_score    = float(tr.get("avg_score") or 0)
    high_count   = int(tr.get("high_count") or 0)
    mid_count    = int(tr.get("mid_count") or 0)
    low_count    = int(tr.get("low_count") or 0)
    churn_count  = int(tr.get("churn_count") or 0)
    contact_count = int(tr.get("contact_count") or 0)
    cx_bad_count = int(tr.get("cx_bad_count") or 0)

    if not total:
        raise HTTPException(422, "No evaluated tickets found. Run an analysis first.")

    pct_high    = round(high_count   / total * 100, 1)
    pct_mid     = round(mid_count    / total * 100, 1)
    pct_low     = round(low_count    / total * 100, 1)
    churn_pct   = round(churn_count  / total * 100, 1)
    contact_pct = round(contact_count / total * 100, 1)
    cx_bad_pct  = round(cx_bad_count  / total * 100, 1)

    ar = arr_res_p.mappings().first() or {}
    confirmed_churn  = int(ar.get("confirmed_churn") or 0)
    arr_at_risk      = float(ar.get("arr_at_risk") or 0)
    companies_at_risk = int(ar.get("companies_at_risk") or 0)

    mr = mismatch_res_p.mappings().first() or {}
    cx_bad_total  = int(mr.get("cx_bad_total") or 0)
    mismatch_count = int(mr.get("mismatch") or 0)
    mismatch_pct   = round(mismatch_count / cx_bad_total * 100, 1) if cx_bad_total else 0

    # ── 4. QA dimension averages ──────────────────────────────────────────────
    dim_sql = text("""
        WITH latest AS (
            SELECT DISTINCT ON (ticket_id, user_id) scores
            FROM evaluations WHERE user_id = :uid
            ORDER BY ticket_id, user_id, id DESC
        )
        SELECT scores FROM latest WHERE scores IS NOT NULL
    """)
    dim_res = await db.execute(dim_sql, {"uid": str(user.id)})
    dim_acc: dict[str, list[float]] = {}
    for row in dim_res.scalars():
        if not isinstance(row, dict):
            continue
        for k, v in row.items():
            s = (v or {}).get("score") if isinstance(v, dict) else v
            if isinstance(s, (int, float)):
                dim_acc.setdefault(k, []).append(float(s))

    dim_avgs = sorted(
        [(k, round(sum(v) / len(v), 2)) for k, v in dim_acc.items()],
        key=lambda x: x[1]
    )
    dim_table = "\n".join(f"  {k}: {avg}/5" for k, avg in dim_avgs) or "  No dimension data"

    # ── 5. Agent aggregates ───────────────────────────────────────────────────
    agent_sql = text("""
        WITH latest AS (
            SELECT DISTINCT ON (e.ticket_id, e.user_id)
                t.agent_name, e.total_score, e.churn_risk_flag
            FROM evaluations e
            JOIN tickets t ON t.id = e.ticket_id AND t.user_id = e.user_id
            WHERE e.user_id = :uid AND t.agent_name IS NOT NULL
            ORDER BY e.ticket_id, e.user_id, e.id DESC
        )
        SELECT
            agent_name,
            COUNT(*)                                                AS ticket_count,
            ROUND(AVG(total_score)::numeric, 1)                    AS avg_score,
            SUM(CASE WHEN churn_risk_flag THEN 1 ELSE 0 END)       AS churn_count
        FROM latest
        GROUP BY agent_name
        HAVING COUNT(*) >= 3
        ORDER BY agent_name
    """)
    ag_res = await db.execute(agent_sql, {"uid": str(user.id)})
    agents = []
    for r in ag_res.mappings():
        tc  = int(r["ticket_count"])
        qa  = float(r["avg_score"] or 0)
        ch  = int(r["churn_count"] or 0)
        chp = round(ch / tc * 100, 1)
        agents.append({"name": r["agent_name"], "tickets": tc, "qa": qa, "churn_pct": chp})

    GLOBAL_CHURN = 18.6
    high_risk   = [a for a in agents if a["churn_pct"] >= GLOBAL_CHURN and a["qa"] < 59]
    mismatch_ag = [a for a in agents if a["churn_pct"] >= GLOBAL_CHURN and a["qa"] >= 70]
    low_risk    = [a for a in agents if a["churn_pct"] < GLOBAL_CHURN and a["qa"] >= 65]
    coaching    = [a for a in agents if a["tickets"] >= 10 and a["churn_pct"] > 25]

    def _ag_str(ag_list): return "\n  ".join(
        f"{a['name']}: QA={a['qa']} churn={a['churn_pct']}% ({a['tickets']}t)"
        for a in ag_list
    ) or "  None"

    # ── 6. Category table ─────────────────────────────────────────────────────
    cat_sql = text("""
        WITH latest AS (
            SELECT DISTINCT ON (e.ticket_id, e.user_id)
                e.ticket_id, e.user_id, e.churn_risk_flag, t.tags
            FROM evaluations e
            JOIN tickets t ON t.id = e.ticket_id AND t.user_id = e.user_id
            WHERE e.user_id = :uid
            ORDER BY e.ticket_id, e.user_id, e.id DESC
        )
        SELECT
            tag,
            COUNT(DISTINCT ticket_id)                              AS volume,
            SUM(CASE WHEN churn_risk_flag THEN 1 ELSE 0 END)      AS churn_count
        FROM latest
        CROSS JOIN LATERAL UNNEST(COALESCE(tags, ARRAY[]::text[])) AS tag
        GROUP BY tag
        HAVING COUNT(DISTINCT ticket_id) >= 5
        ORDER BY (SUM(CASE WHEN churn_risk_flag THEN 1 ELSE 0 END)::float
                  / NULLIF(COUNT(DISTINCT ticket_id), 0)) DESC
    """)
    cat_res = await db.execute(cat_sql, {"uid": str(user.id)})
    cat_lines = []
    for r in cat_res.mappings():
        vol = int(r["volume"])
        ch  = int(r["churn_count"] or 0)
        pct = round(ch / vol * 100, 1) if vol else 0
        risk = "High" if pct >= 30 else "Med" if pct >= 15 else "Low"
        cat_lines.append(f"  {r['tag']}: {pct}% churn (vol={vol}, {risk})")
    cat_table = "\n".join(cat_lines[:15]) or "  No tag data"

    ms = round((time.time() - t0) * 1000)
    logger.info(f"qa_summary data aggregation: {ms}ms, {total} tickets, {len(agents)} agents")

    # ── 7. Build prompt ───────────────────────────────────────────────────────
    prompt = PROMPT.format(
        avg_score=avg_score,
        pct_high=pct_high, pct_mid=pct_mid, pct_low=pct_low,
        churn_pct=churn_pct, churn_count=churn_count,
        confirmed_churn=confirmed_churn,
        contact_pct=contact_pct,
        cx_bad_pct=cx_bad_pct,
        mismatch_count=mismatch_count, mismatch_pct=mismatch_pct,
        arr_at_risk=arr_at_risk, companies_at_risk=companies_at_risk,
        dim_table=dim_table,
        agent_total=len(agents),
        high_risk_count=len(high_risk), high_risk_list=_ag_str(high_risk),
        mismatch_agent_count=len(mismatch_ag), mismatch_agent_list=_ag_str(mismatch_ag),
        low_risk_count=len(low_risk), low_risk_list=_ag_str(low_risk),
        coaching_list=_ag_str(coaching),
        cat_table=cat_table,
    )

    # ── 8. Call Claude Sonnet ─────────────────────────────────────────────────
    t1 = time.time()
    try:
        response = await asyncio.wait_for(
            _client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=2000,
                temperature=0,
                system=SYSTEM,
                messages=[{"role": "user", "content": prompt}],
            ),
            timeout=55.0,
        )
    except asyncio.TimeoutError:
        raise HTTPException(504, "QA Summary generation timed out. Retry.")

    raw  = response.content[0].text
    data = json.loads(_repair(raw))
    gen_ms = round((time.time() - t1) * 1000)
    logger.info(f"qa_summary Sonnet: {gen_ms}ms")

    # Attach metadata
    data["meta"] = {
        "total_tickets":     total,
        "avg_score":         avg_score,
        "pct_high":          pct_high,
        "pct_mid":           pct_mid,
        "pct_low":           pct_low,
        "churn_pct":         churn_pct,
        "churn_count":       churn_count,
        "confirmed_churn":   confirmed_churn,
        "cx_bad_pct":        cx_bad_pct,
        "mismatch_count":    mismatch_count,
        "arr_at_risk":       arr_at_risk,
        "companies_at_risk": companies_at_risk,
        "agent_total":       len(agents),
        "high_risk_agents":  len(high_risk),
        "coaching_agents":   len(coaching),
        "dim_lowest":        dim_avgs[:3] if dim_avgs else [],
        "dim_highest":       dim_avgs[-3:] if dim_avgs else [],
    }

    await cache_set(cache_key, data, TTL)
    return data
