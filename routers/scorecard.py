import asyncio
"""
Ticket scorecard — works from tickets already stored in the DB.
Uses the stored conversation thread + ticket metadata as the ONLY source of truth.
"""
import json
import re
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession
from anthropic import AsyncAnthropic

from config import settings
from database import get_db
from models import User, Ticket, Message, Evaluation
from routers.auth import current_user
from services.cache import get as cache_get, set as cache_set

router = APIRouter()
TTL_SC = 300   # 5 min cache — manual scorecard result is stable

# 55s hard timeout — Sonnet needs breathing room on complex tickets
_client = AsyncAnthropic(
    api_key=settings.anthropic_api_key,
    timeout=55.0,
)

SYSTEM = (
    "You are a senior Quality Assurance analyst in Customer Care. "
    "Evaluate the support ticket interaction STRICTLY based on the provided ticket data. "
    "Do NOT assume, infer, or invent any information not present in the ticket. "
    "If something is missing or unclear, state: 'Not enough data in ticket'. "
    "Return ONLY valid JSON — no markdown, no preamble, no trailing text."
)

PROMPT = """### Role
You are a senior QA analyst reviewing support tickets.

### Data Context (Critical Instruction)
The support ticket below is the ONLY source of truth.
- ONLY use the provided ticket data
- No assumptions outside the data
- If unclear → state "Not enough data in ticket"
- Be precise and evidence-based

### Scoring Scale
1 = poor | 3 = acceptable | 5 = excellent

### Ticket Metadata
Subject: {subject}
Agent: {agent}
Group: {group}
CSAT: {csat}
Created: {created}
Resolved: {resolved}
Tags: {tags}
SLA breached: {sla}

### Conversation Thread ({msg_count} messages)
{thread}

Return this EXACT JSON (no other text):
{{
  "categories": {{
    "understanding_of_issue":       {{"score": 0, "rationale": ""}},
    "accuracy_of_solution":         {{"score": 0, "rationale": ""}},
    "completeness_of_response":     {{"score": 0, "rationale": ""}},
    "clarity_of_communication":     {{"score": 0, "rationale": ""}},
    "tone_and_empathy":             {{"score": 0, "rationale": ""}},
    "efficiency":                   {{"score": 0, "rationale": ""}},
    "ownership":                    {{"score": 0, "rationale": ""}},
    "proactivity":                  {{"score": 0, "rationale": ""}},
    "customer_confidence_created":  {{"score": 0, "rationale": ""}},
    "risk_of_repeat_contact":       {{"score": 0, "rationale": ""}}
  }},
  "average_score": 0.0,
  "verdict": "Acceptable",
  "strengths": [""],
  "weaknesses": [""],
  "final_comment": ""
}}

Scoring rules:
- Scores 1–5 only (1=poor, 3=acceptable, 5=excellent)
- risk_of_repeat_contact: 5=very low risk, 1=near-certain repeat
- average_score = mean of all 10 scores, 1 decimal
- strengths/weaknesses: 2–4 items, strictly from ticket content
- If information is missing → state "Not enough data in ticket" in rationale
- final_comment: 3–5 sentences covering quality, scalability, and customer trust impact
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
    for m in messages:
        ts = str(m.ts or "")[:16].replace("T", " ")
        body = (m.body or "").strip()[:800]
        lines.append(f"[{m.role}] {ts}\n{body}")
    return "\n\n---\n\n".join(lines)


class ScorecardRequest(BaseModel):
    ticket_id: str


@router.post("/")
async def generate_scorecard(
    body: ScorecardRequest,
    user: User = Depends(current_user),
    db: AsyncSession = Depends(get_db),
):
    # Cache check
    _cache_key = f"sc:{user.id}:{body.ticket_id}"
    _cached = await cache_get(_cache_key)
    if _cached is not None:
        return _cached

    # Load ticket
    ticket_result = await db.execute(
        select(Ticket).where(
            Ticket.id == body.ticket_id,
            Ticket.user_id == user.id
        )
    )
    ticket = ticket_result.scalar_one_or_none()
    if not ticket:
        raise HTTPException(404, "Ticket not found")

    # Load messages
    msg_result = await db.execute(
        select(Message).where(
            Message.ticket_id == body.ticket_id,
            Message.user_id == user.id
        ).order_by(Message.ts.asc())
    )
    messages = msg_result.scalars().all()

    if not messages:
        raise HTTPException(422, "No conversation data available for this ticket. Run analysis first to load messages.")

    thread = _build_thread(messages)

    prompt = PROMPT.format(
        subject=ticket.subject or "Not available",
        agent=ticket.agent_name or "Not available",
        group=ticket.group_name or "Not available",
        csat=str(ticket.csat) if ticket.csat else "Not available",
        created=str(ticket.created_at or "")[:10] or "Not available",
        resolved=str(ticket.resolved_at or "")[:10] or "Not available",
        tags=", ".join(ticket.tags or []) or "None",
        sla="Yes" if ticket.fr_escalated else "No",
        msg_count=len(messages),
        thread=thread[:5000],
    )

    try:
        response = await asyncio.wait_for(
            _client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=2000,
                temperature=0,
                system=SYSTEM,
                messages=[{"role": "user", "content": prompt}],
            ),
            timeout=55.0,
        )
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=504,
            detail="Scorecard generation timed out. Retry."
        )
    raw = response.content[0].text
    data = json.loads(_repair(raw))

    # Recalculate average server-side
    scores = [v["score"] for v in data["categories"].values()]
    avg = round(sum(scores) / len(scores), 1)
    data["average_score"] = avg

    # Set verdict from score
    if avg >= 4.5:   data["verdict"] = "Excellent"
    elif avg >= 3.8: data["verdict"] = "Strong"
    elif avg >= 2.8: data["verdict"] = "Acceptable"
    elif avg >= 2.0: data["verdict"] = "Weak"
    else:            data["verdict"] = "Poor"

    # Include ticket metadata in response
    data["ticket_meta"] = {
        "id": body.ticket_id,
        "subject": ticket.subject,
        "agent": ticket.agent_name,
        "group": ticket.group_name,
        "csat": ticket.csat,
        "msg_count": len(messages),
    }

    await cache_set(_cache_key, data, TTL_SC)
    return data
