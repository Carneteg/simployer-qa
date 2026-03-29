"""
POST /qa-framework/

Simployer Customer Care QA Framework v1.0
==========================================

7-dimension per-AGENT-ANSWER evaluation framework.
Each agent message is scored individually — not the ticket outcome.

Dimensions:
  D1  Communication Quality          1–5
  D2  Early Churn Signal Detection  −1–5
  D3  Churn Risk Reduction          −2–5
  D4  Holdback / Save Attempt       −2–5  (N/A when no retention context)
  D5  Termination Handling          −1–5  (N/A when no cancellation request)
  D6  Winback Potential              0–5   (0 = N/A, neutral interaction)
  D7  Service Excellence             1–5   (standalone, scored independently)

Score range per response: −7 to +35
Negative total = net harmful response → mandatory coaching flag

Key design rules:
  - Score each AGENT message separately, NOT the case outcome
  - Retained customer ≠ good agent. Churned customer ≠ bad agent.
  - Negative scores exist to accurately reflect agent impact
  - N/A dimensions excluded from total (not scored as 3)
"""

import asyncio
import json
import re
import time
import logging
from datetime import datetime, timezone
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from anthropic import AsyncAnthropic

from config import settings
from database import get_db
from models import User, Ticket, Message, Evaluation
from routers.auth import current_user
from services.cache import get as cache_get, set as cache_set

router  = APIRouter()
logger  = logging.getLogger("simployer.qa_framework")
_client = AsyncAnthropic(api_key=settings.anthropic_api_key, timeout=60.0)

TTL_QAF = 600  # 10 min — per-message scoring is expensive, cache aggressively

# ── Score bands ────────────────────────────────────────────────────────────────
BANDS = [
    (32, 35, "Exceptional",  "Model response. Every dimension executed at the highest level."),
    (25, 31, "Strong",       "Solid across all dimensions. Minor gaps that don't affect customer outcome."),
    (18, 24, "Acceptable",   "Adequate. Opportunities missed. Coaching recommended."),
    (10, 17, "Needs Work",   "Noticeable gaps in multiple dimensions. Customer relationship at risk."),
    (0,   9, "Poor",         "Significant failures. Customer experience damaged. Immediate coaching required."),
    (-99,-1, "Net Harmful",  "This response made the situation worse. Escalation and coaching mandatory."),
]

def score_band(total: int) -> dict:
    for lo, hi, label, desc in BANDS:
        if lo <= total <= hi:
            return {"band": label, "description": desc}
    return {"band": "Net Harmful", "description": BANDS[-1][3]}

# ── System prompt ──────────────────────────────────────────────────────────────
SYSTEM = """\
You are a senior QA analyst for Simployer Customer Care.
You evaluate individual AGENT MESSAGES — not case outcomes.
Critical rules:
  - A retained customer does NOT mean the agent performed well
  - A churned customer does NOT mean the agent performed poorly
  - Score ONLY what is in the agent message being evaluated
  - Negative scores are REQUIRED when agent behaviour was harmful
  - Use N/A (null) for D4/D5 when not applicable to this message
  - Return ONLY valid JSON — no markdown, no preamble
"""

MESSAGE_PROMPT = """\
## Context: Full conversation thread (for background only)
{thread_context}

## Agent message to evaluate (index {msg_index} of {total_agent_msgs})
Role: AGENT
Timestamp: {ts}
Text:
{agent_message}

## Task
Score this single agent message against all 7 QA dimensions.

### D1 — Communication Quality (1 to 5)
Was this response clear, well-structured, appropriately toned, and professional?
1 = confusing/inappropriate | 3 = adequate | 5 = model response

### D2 — Early Churn Signal Detection (−1 to 5)
Did the agent spot and meaningfully respond to early warning signs (frustration, repeated contact, declining trust, hedging language)?
−1 = actively trivialised a stated frustration | 1 = clear signal missed | 3 = partial acknowledgement | 5 = proactively named and addressed

### D3 — Churn Risk Reduction (−2 to 5)
Did this specific response lower or raise the probability of the customer leaving?
−2 = pushed customer toward leaving (false promise, blame, dismissal) | −1 = made situation worse | 3 = neutral | 5 = directly reduced root dissatisfaction

### D4 — Holdback / Save Attempt Quality (−2 to 5, or null if N/A)
If the customer indicated intent to leave, how well did the agent attempt retention?
Use null if no retention context is present.
−2 = coercive/dishonest tactics | 1 = token gesture | 3 = generic script | 5 = genuine, personalised, respectful

### D5 — Termination Handling (−1 to 5, or null if N/A)
If a cancellation was explicitly requested, how well was it handled?
Use null if no termination request was made.
−1 = obstructed or ignored the request | 1 = poor | 3 = adequate | 5 = exemplary (acknowledged, explained, respectful close)

### D6 — Winback Potential (0 to 5)
Does this response preserve or build future relationship capital?
0 = neutral/N/A | 1 = very low | 3 = neutral-high | 5 = maximised (warm, genuine, door open)

### D7 — Service Excellence (1 to 5) — STANDALONE
Evaluate independently: ownership, genuine empathy (with action), clarity, trust-building, professionalism.
1 = below standard | 3 = competent | 5 = outstanding

Return this EXACT JSON:
{{
  "message_index": {msg_index},
  "message_preview": "",
  "dimensions": {{
    "d1_communication": {{"score": 0, "rationale": "", "evidence": ""}},
    "d2_churn_signal":  {{"score": 0, "rationale": "", "evidence": ""}},
    "d3_churn_risk":    {{"score": 0, "rationale": "", "evidence": ""}},
    "d4_holdback":      {{"score": null, "rationale": "", "evidence": "", "na_reason": ""}},
    "d5_termination":   {{"score": null, "rationale": "", "evidence": "", "na_reason": ""}},
    "d6_winback":       {{"score": 0, "rationale": "", "evidence": ""}},
    "d7_excellence":    {{"score": 0, "rationale": "", "evidence": ""}}
  }},
  "total_score": 0,
  "applicable_max": 0,
  "coaching_note": "",
  "negative_flag": false
}}

Rules:
- message_preview: first 80 chars of the message
- evidence: quote ≤15 words from the message to support your score
- total_score = sum of all non-null dimension scores
- applicable_max = sum of max possible score for each non-null dimension
  (D1=5, D2=5, D3=5, D4=5 if scored, D5=5 if scored, D6=5, D7=5)
- coaching_note: most important single improvement for this response (1 sentence)
- negative_flag: true if total_score < 0
"""

# ── Helpers ────────────────────────────────────────────────────────────────────

def _repair(txt: str) -> str:
    txt = txt.strip()
    txt = re.sub(r"^```json\s*", "", txt)
    txt = re.sub(r"```\s*$", "", txt)
    return txt.strip()

def _build_thread_context(messages: list) -> str:
    if not messages:
        return "No conversation data available."
    lines = []
    for m in messages[:20]:  # cap context
        ts = str(m.ts or "")[:16].replace("T", " ")
        body = (m.body or "").strip()[:300]
        lines.append(f"[{m.role}] {ts}\n{body}")
    return "\n\n---\n\n".join(lines)

# ── Request model ──────────────────────────────────────────────────────────────

class QAFrameworkRequest(BaseModel):
    ticket_id: str

# ── Endpoint ───────────────────────────────────────────────────────────────────

@router.post("/")
async def run_qa_framework(
    body: QAFrameworkRequest,
    user: User = Depends(current_user),
    db:   AsyncSession = Depends(get_db),
):
    t0 = time.time()

    # Cache
    cache_key = f"qaf:{user.id}:{body.ticket_id}"
    cached = await cache_get(cache_key)
    if cached is not None:
        logger.debug(f"qaf cache HIT ticket={body.ticket_id}")
        return cached

    # Load ticket + messages in parallel
    tkt_res, msg_res = await asyncio.gather(
        db.execute(select(Ticket).where(Ticket.id == body.ticket_id, Ticket.user_id == user.id)),
        db.execute(
            select(Message)
            .where(Message.ticket_id == body.ticket_id, Message.user_id == user.id)
            .order_by(Message.ts.asc())
        ),
    )
    ticket   = tkt_res.scalar_one_or_none()
    messages = msg_res.scalars().all()

    if not ticket:
        raise HTTPException(404, f"Ticket {body.ticket_id} not found")

    # Separate agent messages — these are what we score
    agent_messages = [m for m in messages if m.role == "AGENT"]
    if not agent_messages:
        raise HTTPException(422, "No agent messages found in this ticket. Nothing to score.")

    thread_context = _build_thread_context(list(messages))
    total_agent = len(agent_messages)

    # Score each agent message independently
    message_results = []
    for idx, msg in enumerate(agent_messages, start=1):
        prompt = MESSAGE_PROMPT.format(
            thread_context  = thread_context[:4000],
            msg_index       = idx,
            total_agent_msgs= total_agent,
            ts              = str(msg.ts or "")[:16].replace("T", " "),
            agent_message   = (msg.body or "").strip()[:1200],
        )
        try:
            resp = await asyncio.wait_for(
                _client.messages.create(
                    model       = "claude-sonnet-4-20250514",
                    max_tokens  = 1200,
                    temperature = 0,
                    system      = SYSTEM,
                    messages    = [{"role": "user", "content": prompt}],
                ),
                timeout=55.0,
            )
            data = json.loads(_repair(resp.content[0].text))
            # Validate & compute totals server-side
            dims = data.get("dimensions", {})
            scored = {k: v for k, v in dims.items() if v.get("score") is not None}
            total  = sum(v["score"] for v in scored.values())
            appmax = sum(5 for _ in scored)  # max 5 per applicable dimension
            data["total_score"]    = total
            data["applicable_max"] = appmax
            data["negative_flag"]  = total < 0
            data["band"]           = score_band(total)
            message_results.append(data)

        except asyncio.TimeoutError:
            message_results.append({
                "message_index": idx,
                "message_preview": (msg.body or "")[:80],
                "error": "Timed out scoring this message",
                "total_score": None,
            })
        except Exception as e:
            logger.warning(f"qaf msg {idx} error: {e}")
            message_results.append({
                "message_index": idx,
                "message_preview": (msg.body or "")[:80],
                "error": str(e),
                "total_score": None,
            })

    # Aggregate across all scored messages
    valid = [r for r in message_results if r.get("total_score") is not None]
    agg_total  = sum(r["total_score"] for r in valid)
    agg_appmax = sum(r.get("applicable_max", 35) for r in valid)
    agg_avg    = round(agg_total / len(valid), 1) if valid else None
    any_negative = any(r.get("negative_flag") for r in valid)
    any_net_harm = any(r.get("total_score", 0) < 0 for r in valid)

    gen_ms = round((time.time() - t0) * 1000)
    logger.info(
        f"qa_framework ticket={body.ticket_id} msgs={total_agent} "
        f"agg={agg_total}/{agg_appmax} net_harm={any_net_harm} {gen_ms}ms"
    )

    result = {
        "ticket": {
            "id":           body.ticket_id,
            "subject":      ticket.subject,
            "agent":        ticket.agent_name,
            "group":        ticket.group_name,
            "priority":     ticket.priority,
            "tags":         ticket.tags or [],
            "csat":         ticket.csat,
            "created_at":   str(ticket.created_at or "")[:16],
            "resolved_at":  str(ticket.resolved_at or "")[:16],
            "total_messages": len(messages),
            "agent_messages": total_agent,
            "arr":           float(ticket.arr) if ticket.arr else None,
        },
        "message_scores":   message_results,
        "aggregate": {
            "total_score":    agg_total,
            "applicable_max": agg_appmax,
            "avg_per_message": agg_avg,
            "messages_scored": len(valid),
            "any_negative_flag": any_negative,
            "net_harmful_response": any_net_harm,
            "band": score_band(round(agg_avg) if agg_avg else 0),
        },
        "framework_version": "1.0",
        "meta": {"gen_ms": gen_ms},
    }
    await cache_set(cache_key, result, TTL_QAF)
    return result
