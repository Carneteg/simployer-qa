import asyncio
"""
Agent QA Scorecard — evaluates an agent across ALL their tickets as a dataset.
Uses Claude Sonnet with strict data-context rules (no invented facts).
"""
import json
import re
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import AsyncSession
from anthropic import AsyncAnthropic

from config import settings
from database import get_db
from models import User, Ticket, Evaluation, Message
from routers.auth import current_user
from services.cache import get as cache_get, set as cache_set

router = APIRouter()
TTL_AGREV = 300   # 5 min cache — agent pattern analysis is stable

# 55s hard timeout — Sonnet on large agent datasets needs breathing room
_client = AsyncAnthropic(
    api_key=settings.anthropic_api_key,
    timeout=55.0,
)

SYSTEM = (
    "You are a senior QA analyst reviewing agent performance across multiple support tickets. "
    "Evaluate ONLY based on the ticket dataset provided — never invent or assume facts. "
    "Treat all tickets as a dataset and identify RECURRING behaviors across the full set. "
    "Do NOT over-index on a single ticket — a pattern must appear in multiple tickets to be noted. "
    "If data is insufficient for any category → state it clearly in the pattern field. "
    "Return ONLY valid JSON — no markdown, no preamble, no commentary."
)

PROMPT = """### Role
You are analyzing agent performance across multiple support tickets.

### Data Context
The ticket dataset below is the ONLY source of truth.
- Treat all tickets as a dataset
- Identify recurring behaviors and patterns
- Do NOT over-index on a single ticket
- Only use available data
- If data is insufficient → state it clearly

### Agent
Name: {agent_name}
Group: {group_name}
Total tickets in dataset: {ticket_count}
Average QA score (automated): {avg_score}/100
Churn flags: {churn_count} ({churn_pct}%)
CSAT ratings available: {csat_count}
Average CSAT: {avg_csat}

### Ticket Dataset (summary + conversation samples)
{dataset}

Return this EXACT JSON (no other text):
{{
  "categories": {{
    "consistency":               {{"score": 0, "pattern": ""}},
    "product_knowledge":         {{"score": 0, "pattern": ""}},
    "communication_quality":     {{"score": 0, "pattern": ""}},
    "tone_and_empathy":          {{"score": 0, "pattern": ""}},
    "ownership":                 {{"score": 0, "pattern": ""}},
    "efficiency":                {{"score": 0, "pattern": ""}},
    "problem_solving":           {{"score": 0, "pattern": ""}},
    "repeat_contact_prevention": {{"score": 0, "pattern": ""}},
    "trust_building":            {{"score": 0, "pattern": ""}},
    "customer_care_maturity":    {{"score": 0, "pattern": ""}}
  }},
  "average_score": 0.0,
  "performance_level": "Good",
  "coaching_priority": "Medium",
  "pattern_summary": [""],
  "strength_areas": [""],
  "improvement_areas": [""],
  "leadership_comment": ""
}}

Scoring rules:
- Scores 1–5: 1=poor, 3=acceptable, 5=excellent
- "pattern" field: describe the recurring behavior observed across tickets (not just one)
- performance_level: one of High / Good / Mixed / Risk / Critical
- coaching_priority: one of Low / Medium / High
- average_score = mean of all 10 category scores, 1 decimal
- pattern_summary: 3–5 bullet points of patterns across the dataset
- strength_areas: 2–4 repeated strengths seen across multiple tickets
- improvement_areas: 2–4 repeated gaps seen across multiple tickets
- leadership_comment: 3–5 sentences covering scalability, reliability, and readiness for complex cases
"""


def _repair(txt: str) -> str:
    txt = txt.strip()
    txt = re.sub(r"^```json\s*", "", txt)
    txt = re.sub(r"```\s*$", "", txt)
    return txt.strip()


def _build_dataset(tickets_data: list, messages_map: dict) -> str:
    """Build a compact dataset string from tickets + their conversation samples."""
    lines = []
    total = len(tickets_data)
    used  = min(total, 20)
    lines.append(
        f"Dataset: {total} total tickets | {used} included below | "
        f"{'ALL tickets included' if total <= 20 else f'20 of {total} shown (lowest-scoring first)'}"
    )
    lines.append("")
    for i, t in enumerate(tickets_data[:20]):  # cap at 20 — faster generation
        tid = str(t["ticket_id"])
        msgs = messages_map.get(tid, [])
        # Take first 2 agent messages as sample
        agent_msgs = [m for m in msgs if m.role == "AGENT"][:2]
        sample = " | ".join(m.body[:80].replace("\n", " ") for m in agent_msgs) or "No messages available"

        lines.append(
            f"[{i+1}] #{tid} | Subject: {t['subject'] or 'N/A'} | "
            f"Score: {t['total_score']} | CSAT: {t['csat'] or 'N/A'} | "
            f"Churn: {'Yes' if t['churn_risk_flag'] else 'No'} | "
            f"Contact problem: {'Yes' if t['contact_problem_flag'] else 'No'}\n"
            f"    Agent sample: {sample}\n"
            f"    Summary: {(t['summary'] or 'N/A')[:100]}\n"
            f"    Coaching: {(t['coaching_tip'] or 'N/A')[:80]}"
        )
    return "\n\n".join(lines)


class AgentScorecardRequest(BaseModel):
    agent_name: str
    run_id: str | None = None


@router.post("/")
async def generate_agent_scorecard(
    body: AgentScorecardRequest,
    user: User = Depends(current_user),
    db: AsyncSession = Depends(get_db),
):
    # ── Cache check ──────────────────────────────────────────────────────────
    cache_key = f"agrev:{user.id}:{body.agent_name}:{body.run_id or 'all'}"
    cached = await cache_get(cache_key)
    if cached is not None:
        return cached

    # Load agent's tickets
    q = (
        select(
            Ticket.id.label("ticket_id"),
            Ticket.subject,
            Ticket.group_name,
            Ticket.csat,
            Evaluation.total_score,
            Evaluation.churn_risk_flag,
            Evaluation.contact_problem_flag,
            Evaluation.summary,
            Evaluation.coaching_tip,
            Evaluation.scores,
        )
        .join(Evaluation, and_(
            Evaluation.ticket_id == Ticket.id,
            Evaluation.user_id == Ticket.user_id,
        ))
        .where(
            Ticket.user_id == user.id,
            Ticket.agent_name == body.agent_name,
        )
        .order_by(Evaluation.total_score.asc())
    )
    if body.run_id:
        q = q.where(Evaluation.run_id == body.run_id)

    result = await db.execute(q)
    tickets = result.mappings().all()

    if not tickets:
        raise HTTPException(404, f"No evaluated tickets found for agent: {body.agent_name}")

    if len(tickets) < 2:
        raise HTTPException(422, "At least 2 evaluated tickets required for agent pattern analysis.")

    # Load messages for sample tickets (first 10 for efficiency)
    sample_ids = [str(t["ticket_id"]) for t in tickets[:20]]  # message samples for richer patterns
    msg_result = await db.execute(
        select(Message).where(
            Message.ticket_id.in_(sample_ids),
            Message.user_id == user.id,
        ).order_by(Message.ticket_id, Message.ts)
    )
    messages_map: dict[str, list] = {}
    for m in msg_result.scalars().all():
        messages_map.setdefault(m.ticket_id, []).append(m)

    # Compute aggregate stats
    ticket_count = len(tickets)
    scores = [float(t["total_score"] or 0) for t in tickets]
    avg_score = round(sum(scores) / ticket_count, 1) if scores else 0
    churn_count = sum(1 for t in tickets if t["churn_risk_flag"])
    churn_pct = round(churn_count / ticket_count * 100)
    csat_vals = [t["csat"] for t in tickets if t["csat"]]
    avg_csat = round(sum(csat_vals) / len(csat_vals), 1) if csat_vals else None
    group_name = tickets[0]["group_name"] or "N/A"

    # Build dataset
    dataset = _build_dataset(list(tickets), messages_map)

    prompt = PROMPT.format(
        agent_name=body.agent_name,
        group_name=group_name,
        ticket_count=ticket_count,
        avg_score=avg_score,
        churn_count=churn_count,
        churn_pct=churn_pct,
        csat_count=len(csat_vals),
        avg_csat=avg_csat or "Not available",
        dataset=dataset[:5000],  # Sonnet 200K context — safe at 8K chars
    )

    try:
        response = await asyncio.wait_for(
            _client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=1800,
                temperature=0,
                system=SYSTEM,
                messages=[{"role": "user", "content": prompt}],
            ),
            timeout=55.0,
        )
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=504,
            detail="Agent scorecard timed out. The agent has many tickets — retry or try filtering by run."
        )
    raw = response.content[0].text
    data = json.loads(_repair(raw))

    # Recalculate average server-side
    cat_scores = [v["score"] for v in data["categories"].values()]
    avg = round(sum(cat_scores) / len(cat_scores), 1)
    data["average_score"] = avg

    # Set performance level from score
    if avg >= 4.5:   data["performance_level"] = "High"
    elif avg >= 3.8: data["performance_level"] = "Good"
    elif avg >= 2.8: data["performance_level"] = "Mixed"
    elif avg >= 2.0: data["performance_level"] = "Risk"
    else:            data["performance_level"] = "Critical"

    # Set coaching priority
    if avg >= 4.0 and churn_pct <= 10:  data["coaching_priority"] = "Low"
    elif avg >= 3.0:                     data["coaching_priority"] = "Medium"
    else:                                data["coaching_priority"] = "High"

    # Include metadata
    data["agent_meta"] = {
        "agent_name": body.agent_name,
        "group_name": group_name,
        "ticket_count": ticket_count,
        "avg_qa_score": avg_score,
        "churn_count": churn_count,
        "churn_pct": churn_pct,
        "avg_csat": avg_csat,
    }

    await cache_set(cache_key, data, TTL_AGREV)
    return data
