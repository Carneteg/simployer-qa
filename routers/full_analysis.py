"""
POST /full-analysis/

Combined QA engine producing all 4 sections in one Claude Sonnet call:
  1. Ticket QA Scorecard  (single ticket — 10 categories + verdict + strengths/weaknesses)
  2. Agent QA Scorecard   (all agent tickets as dataset — 10 categories + performance level)
  3. Summary              (7-area table + max 5 executive bullets)
  4. Improvements         (priority table + STOP/START/CONTINUE + leadership comment)

Request:
  ticket_id   (required) — the focal ticket; its agent drives section 2
  run_id      (optional) — filter agent tickets to a specific run

All four sections in one Sonnet call at temperature=0.
"""

import asyncio
import json
import re
import time
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, and_, func, case, text
from sqlalchemy.ext.asyncio import AsyncSession
from anthropic import AsyncAnthropic

from config import settings
from database import get_db
from models import User, Ticket, Evaluation, Message
from routers.auth import current_user

router  = APIRouter()
logger  = logging.getLogger("simployer.full_analysis")
_client = AsyncAnthropic(api_key=settings.anthropic_api_key, timeout=60.0)

SYSTEM = (
    "You are a QA engine for Customer Care acting simultaneously as: "
    "QA analyst, performance reviewer, and Customer Care leader. "
    "Use ONLY the data provided. Do not invent or assume missing information. "
    "Treat multiple tickets as a dataset — identify patterns, not single-ticket anomalies. "
    "If data is insufficient for any field → write 'Insufficient data'. "
    "Return ONLY valid JSON — no markdown, no preamble."
)

PROMPT = """\
### Role
You are a QA engine for Customer Care acting as: QA analyst, performance reviewer, and Customer Care leader.

### Data Context
- Use ONLY the data below
- Treat multiple tickets as a dataset; identify recurring patterns
- Do NOT over-index on a single ticket
- If data is insufficient → state it clearly

### Scoring Scale
1 = poor | 3 = acceptable | 5 = excellent

---
## SECTION A — Focal Ticket

Subject: {subject}
Agent: {agent_name}
Group: {group_name}
Tags: {tags}
CSAT: {csat}
Created: {created_at}
Resolved: {resolved_at}
Churn flagged: {churn_flag}
Contact problem: {contact_problem}
QA score (automated): {total_score}/100
Coaching tip (automated): {coaching_tip}

Conversation thread ({msg_count} messages):
{thread}

---
## SECTION B — Agent Dataset

Agent: {agent_name}
Total tickets in dataset: {agent_ticket_count}
Avg automated QA score: {agent_avg_score}/100
Churn flags: {agent_churn_count} ({agent_churn_pct}%)

{agent_dataset}

---
## SECTION C — Team Context

Total evaluated tickets: {total_tickets}
Team avg QA score: {team_avg_score}/100
Team churn rate: {team_churn_pct}%
Bad CX rate: {cx_bad_pct}%
ARR at risk: NOK {arr_at_risk:,.0f} ({companies_at_risk} companies)

Weakest QA dimensions (1-5):
{dim_table}

High-risk agents (churn ≥18.6%, QA <59):
{high_risk_agents}

---
Return this EXACT JSON:
{{
  "ticket_scorecard": {{
    "categories": {{
      "understanding":         {{"score": 0, "evidence": ""}},
      "accuracy":              {{"score": 0, "evidence": ""}},
      "completeness":          {{"score": 0, "evidence": ""}},
      "clarity":               {{"score": 0, "evidence": ""}},
      "tone":                  {{"score": 0, "evidence": ""}},
      "efficiency":            {{"score": 0, "evidence": ""}},
      "ownership":             {{"score": 0, "evidence": ""}},
      "proactivity":           {{"score": 0, "evidence": ""}},
      "customer_confidence":   {{"score": 0, "evidence": ""}},
      "repeat_contact_risk":   {{"score": 0, "evidence": ""}}
    }},
    "average_score": 0.0,
    "verdict": "",
    "strengths": ["", "", ""],
    "weaknesses": ["", "", ""]
  }},
  "agent_scorecard": {{
    "categories": {{
      "consistency":               {{"score": 0, "pattern": ""}},
      "product_knowledge":         {{"score": 0, "pattern": ""}},
      "communication":             {{"score": 0, "pattern": ""}},
      "tone":                      {{"score": 0, "pattern": ""}},
      "ownership":                 {{"score": 0, "pattern": ""}},
      "efficiency":                {{"score": 0, "pattern": ""}},
      "problem_solving":           {{"score": 0, "pattern": ""}},
      "repeat_prevention":         {{"score": 0, "pattern": ""}},
      "trust_building":            {{"score": 0, "pattern": ""}},
      "maturity":                  {{"score": 0, "pattern": ""}}
    }},
    "overall_score": 0.0,
    "performance_level": "Good",
    "coaching_priority": "Medium"
  }},
  "summary": {{
    "quality":           "",
    "strengths":         "",
    "weaknesses":        "",
    "customer_risk":     "",
    "operational_risk":  "",
    "coaching_need":     "",
    "next_step":         "",
    "executive_bullets": ["", "", "", "", ""]
  }},
  "improvements": [
    {{"priority": "High",   "area": "", "change": "", "evidence": "", "impact": "", "effort": ""}},
    {{"priority": "High",   "area": "", "change": "", "evidence": "", "impact": "", "effort": ""}},
    {{"priority": "Medium", "area": "", "change": "", "evidence": "", "impact": "", "effort": ""}},
    {{"priority": "Medium", "area": "", "change": "", "evidence": "", "impact": "", "effort": ""}},
    {{"priority": "Low",    "area": "", "change": "", "evidence": "", "impact": "", "effort": ""}}
  ],
  "stop":     ["", ""],
  "start":    ["", ""],
  "continue_doing": ["", ""],
  "leadership_comment": ""
}}

Rules:
- ticket_scorecard: score 1-5 per category; evidence from THIS ticket only
- repeat_contact_risk: 5=very low risk, 1=near-certain repeat
- agent_scorecard: pattern from ALL agent tickets in dataset; do NOT over-index on focal ticket
- performance_level: High / Good / Mixed / Risk / Critical
- coaching_priority: Low / Medium / High
- summary quality/strengths/weaknesses: 1-2 sentences each, cite numbers
- executive_bullets: exactly 5, each must cite a specific number
- improvements: exactly 5 rows (2 High, 2 Medium, 1 Low); evidence must cite a number
- effort: Low (days) / Medium (weeks) / High (months)
- stop/start/continue_doing: 2-3 bullets each, pattern-based not single-ticket
- leadership_comment: 5-8 sentences assessing if quality is acceptable at scale
"""


def _repair(txt: str) -> str:
    txt = txt.strip()
    txt = re.sub(r"^```json\s*", "", txt)
    txt = re.sub(r"```\s*$", "", txt)
    return txt.strip()


def _build_thread(messages: list) -> str:
    if not messages:
        return "No conversation messages available."
    lines = []
    for m in messages[:12]:   # cap at 12 turns — keeps prompt manageable
        ts   = str(m.ts or "")[:16].replace("T", " ")
        body = (m.body or "").strip()[:600]
        lines.append(f"[{m.role}] {ts}\n{body}")
    return "\n\n---\n\n".join(lines)


def _build_agent_dataset(tickets: list, messages_map: dict) -> str:
    lines = [f"Dataset: {len(tickets)} tickets | showing up to 25 (lowest-scoring first)\n"]
    for i, t in enumerate(tickets[:25]):
        tid        = str(t["ticket_id"])
        agent_msgs = [m for m in messages_map.get(tid, []) if m.role == "AGENT"][:2]
        sample     = " | ".join(m.body[:120].replace("\n", " ") for m in agent_msgs) or "No messages"
        lines.append(
            f"[{i+1}] #{tid} | Score:{t['total_score']} | Churn:{'Y' if t['churn_risk_flag'] else 'N'} | "
            f"Contact:{'Y' if t['contact_problem_flag'] else 'N'}\n"
            f"  Summary: {(t['summary'] or 'N/A')[:180]}\n"
            f"  Coaching: {(t['coaching_tip'] or 'N/A')[:120]}\n"
            f"  Sample: {sample}"
        )
    return "\n\n".join(lines)


class FullAnalysisRequest(BaseModel):
    ticket_id: str
    run_id: Optional[str] = None


@router.post("/")
async def generate_full_analysis(
    body: FullAnalysisRequest,
    user: User = Depends(current_user),
    db:   AsyncSession = Depends(get_db),
):
    t0 = time.time()

    # ── 1. Load ticket + messages + eval in parallel ─────────────────────────
    tkt_res, msg_res, eval_res = await asyncio.gather(
        db.execute(select(Ticket).where(Ticket.id == body.ticket_id, Ticket.user_id == user.id)),
        db.execute(
            select(Message).where(
                Message.ticket_id == body.ticket_id,
                Message.user_id   == user.id,
            ).order_by(Message.ts.asc())
        ),
        db.execute(
            select(Evaluation)
            .where(Evaluation.ticket_id == body.ticket_id, Evaluation.user_id == user.id)
            .order_by(Evaluation.id.desc()).limit(1)
        ),
    )
    ticket = tkt_res.scalar_one_or_none()
    if not ticket:
        raise HTTPException(404, f"Ticket {body.ticket_id} not found")
    messages = msg_res.scalars().all()
    focal_eval = eval_res.scalar_one_or_none()

    agent_name = ticket.agent_name or "Unknown"

    # ── 2. Load all agent tickets (for agent scorecard) ───────────────────────
    agent_q = (
        select(
            Ticket.id.label("ticket_id"),
            Ticket.subject,
            Ticket.csat,
            Evaluation.total_score,
            Evaluation.churn_risk_flag,
            Evaluation.contact_problem_flag,
            Evaluation.summary,
            Evaluation.coaching_tip,
        )
        .join(Evaluation, and_(
            Evaluation.ticket_id == Ticket.id,
            Evaluation.user_id   == Ticket.user_id,
        ))
        .where(Ticket.user_id == user.id, Ticket.agent_name == agent_name)
        .order_by(Evaluation.total_score.asc())
    )
    if body.run_id:
        agent_q = agent_q.where(Evaluation.run_id == body.run_id)
    agent_tkt_res = await db.execute(agent_q)
    agent_tickets = agent_tkt_res.mappings().all()

    if len(agent_tickets) < 1:
        raise HTTPException(422, f"No evaluated tickets found for agent: {agent_name}")

    # Load messages for agent's tickets (first 15)
    sample_ids = [str(t["ticket_id"]) for t in agent_tickets[:15]]
    amsg_res = await db.execute(
        select(Message).where(
            Message.ticket_id.in_(sample_ids),
            Message.user_id == user.id,
        ).order_by(Message.ticket_id, Message.ts)
    )
    agent_msg_map: dict = {}
    for m in amsg_res.scalars().all():
        agent_msg_map.setdefault(m.ticket_id, []).append(m)

    agent_scores    = [float(t["total_score"] or 0) for t in agent_tickets]
    agent_avg       = round(sum(agent_scores) / len(agent_scores), 1) if agent_scores else 0
    agent_churn_c   = sum(1 for t in agent_tickets if t["churn_risk_flag"])
    agent_churn_pct = round(agent_churn_c / len(agent_tickets) * 100, 1)

    # ── 3. Team context (DISTINCT ON latest eval) ─────────────────────────────
    team_sql = text("""
        WITH latest AS (
            SELECT DISTINCT ON (e.ticket_id, e.user_id)
                e.total_score, e.churn_risk_flag, e.cx_bad, e.scores,
                t.arr, t.company_id
            FROM evaluations e
            JOIN tickets t ON t.id=e.ticket_id AND t.user_id=e.user_id
            WHERE e.user_id=:uid
            ORDER BY e.ticket_id, e.user_id, e.id DESC
        )
        SELECT
            COUNT(*)                                                   AS total,
            ROUND(AVG(total_score)::numeric,1)                        AS avg_score,
            SUM(CASE WHEN churn_risk_flag THEN 1 ELSE 0 END)          AS churn_count,
            SUM(CASE WHEN cx_bad THEN 1 ELSE 0 END)                   AS cx_bad_count,
            SUM(CASE WHEN churn_risk_flag AND arr IS NOT NULL
                     THEN arr ELSE 0 END)                              AS arr_at_risk,
            COUNT(DISTINCT CASE WHEN churn_risk_flag AND company_id IS NOT NULL
                           THEN company_id END)                        AS companies_at_risk
        FROM latest
    """)
    tr = (await db.execute(team_sql, {"uid": str(user.id)})).mappings().first() or {}
    total_tix       = int(tr.get("total") or 0)
    team_avg        = float(tr.get("avg_score") or 0)
    team_churn_pct  = round(int(tr.get("churn_count") or 0) / max(total_tix, 1) * 100, 1)
    cx_bad_pct      = round(int(tr.get("cx_bad_count") or 0) / max(total_tix, 1) * 100, 1)
    arr_at_risk     = float(tr.get("arr_at_risk") or 0)
    companies_at_risk = int(tr.get("companies_at_risk") or 0)

    # QA dim averages
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
    dim_table = "\n".join(f"  {k}: {avg}/5" for k, avg in dim_avgs[:5]) or "  No data"

    # High-risk agents
    ag_sql = text("""
        WITH latest AS (
            SELECT DISTINCT ON (e.ticket_id,e.user_id)
                t.agent_name, e.total_score, e.churn_risk_flag
            FROM evaluations e JOIN tickets t ON t.id=e.ticket_id AND t.user_id=e.user_id
            WHERE e.user_id=:uid AND t.agent_name IS NOT NULL
            ORDER BY e.ticket_id,e.user_id,e.id DESC
        )
        SELECT agent_name, COUNT(*) AS tc,
               ROUND(AVG(total_score)::numeric,1) AS qa,
               SUM(CASE WHEN churn_risk_flag THEN 1 ELSE 0 END) AS ch
        FROM latest GROUP BY agent_name HAVING COUNT(*)>=3
    """)
    hr_agents = []
    for r in (await db.execute(ag_sql, {"uid": str(user.id)})).mappings():
        tc=int(r["tc"]); qa=float(r["qa"] or 0); ch=int(r["ch"] or 0)
        chp=round(ch/tc*100,1)
        if chp>=18.6 and qa<59:
            hr_agents.append(f"  {r['agent_name']}: QA={qa} churn={chp}% ({tc}t)")
    high_risk_str = "\n".join(hr_agents) or "  None"

    logger.info(f"full_analysis data: {round((time.time()-t0)*1000)}ms")

    # ── 4. Build prompt ───────────────────────────────────────────────────────
    prompt = PROMPT.format(
        subject        = ticket.subject or "N/A",
        agent_name     = agent_name,
        group_name     = ticket.group_name or "N/A",
        tags           = ", ".join(ticket.tags or []) or "none",
        csat           = ticket.csat or "N/A",
        created_at     = str(ticket.created_at or "")[:10],
        resolved_at    = str(ticket.resolved_at or "")[:10],
        churn_flag     = "Yes" if (focal_eval and focal_eval.churn_risk_flag) else "No",
        contact_problem= "Yes" if (focal_eval and focal_eval.contact_problem_flag) else "No",
        total_score    = focal_eval.total_score if focal_eval else "Not evaluated",
        coaching_tip   = (focal_eval.coaching_tip or "N/A") if focal_eval else "N/A",
        msg_count      = len(messages),
        thread         = _build_thread(messages)[:4000],
        agent_ticket_count = len(agent_tickets),
        agent_avg_score    = agent_avg,
        agent_churn_count  = agent_churn_c,
        agent_churn_pct    = agent_churn_pct,
        agent_dataset      = _build_agent_dataset(list(agent_tickets), agent_msg_map)[:2500],
        total_tickets      = total_tix,
        team_avg_score     = team_avg,
        team_churn_pct     = team_churn_pct,
        cx_bad_pct         = cx_bad_pct,
        arr_at_risk        = arr_at_risk,
        companies_at_risk  = companies_at_risk,
        dim_table          = dim_table,
        high_risk_agents   = high_risk_str,
    )

    # ── 5. Sonnet call ────────────────────────────────────────────────────────
    t1 = time.time()
    try:
        resp = await asyncio.wait_for(
            _client.messages.create(
                model       = "claude-sonnet-4-20250514",
                max_tokens  = 2800,
                temperature = 0,
                system      = SYSTEM,
                messages    = [{"role": "user", "content": prompt}],
            ),
            timeout=60.0,
        )
    except asyncio.TimeoutError:
        raise HTTPException(504, "Full analysis timed out (>60s). Retry.")

    data = json.loads(_repair(resp.content[0].text))

    # Server-side recalculate averages
    if "ticket_scorecard" in data and "categories" in data["ticket_scorecard"]:
        scores = [v["score"] for v in data["ticket_scorecard"]["categories"].values()]
        data["ticket_scorecard"]["average_score"] = round(sum(scores)/len(scores), 1) if scores else 0

    if "agent_scorecard" in data and "categories" in data["agent_scorecard"]:
        scores = [v["score"] for v in data["agent_scorecard"]["categories"].values()]
        data["agent_scorecard"]["overall_score"] = round(sum(scores)/len(scores), 1) if scores else 0
        avg = data["agent_scorecard"]["overall_score"]
        if avg >= 4.5:   data["agent_scorecard"]["performance_level"] = "High"
        elif avg >= 3.8: data["agent_scorecard"]["performance_level"] = "Good"
        elif avg >= 2.8: data["agent_scorecard"]["performance_level"] = "Mixed"
        elif avg >= 2.0: data["agent_scorecard"]["performance_level"] = "Risk"
        else:            data["agent_scorecard"]["performance_level"] = "Critical"

    data["meta"] = {
        "ticket_id": body.ticket_id,
        "agent_name": agent_name,
        "agent_tickets": len(agent_tickets),
        "agent_avg_score": agent_avg,
        "agent_churn_pct": agent_churn_pct,
        "total_team_tickets": total_tix,
        "team_avg_score": team_avg,
        "arr_at_risk": arr_at_risk,
        "gen_ms": round((time.time()-t1)*1000),
    }

    logger.info(f"full_analysis Sonnet: {data['meta']['gen_ms']}ms")
    return data
