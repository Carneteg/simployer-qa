"""
POST /qa-scorecard-v2/

Weighted QA Scoring Engine v2 — Priority-Aware SLA + Confidence Model
======================================================================

SCORECARD WEIGHTS (sums to 100%):
  Accuracy / Resolution   35%
  Process & Compliance    30%  (10% is SLA adherence)
  Communication           20%  (5% is response discipline/speed)
  Customer Experience     20%  (5% is speed perception)
  Documentation            5%

Speed is embedded — never a standalone category:
  5% in Customer Experience   (speed perception)
  5% in Communication         (response discipline)
  10% in Process & Compliance (SLA adherence)

PRIORITY-BASED SLA THRESHOLDS (Freshdesk integers: 1=Low, 2=Medium, 3=High, 4=Urgent):
  Urgent: FRT 2h  / every response 6h  / resolution 1d
  High:   FRT 4h  / every response 8h  / resolution 1d
  Medium: FRT 1d  / every response 1d  / resolution 2d
  Low:    FRT 1d  / every response 1d  / resolution 3d

SLA PENALTY SCHEDULE (deducted from SLA sub-score, floor 0):
  Urgent breach  → −40 pts
  High breach    → −25 pts
  Medium breach  → −15 pts
  Low breach     → −8 pts
  Urgent + resolution breach → additional −20 pts (critical_failure flag)

CONFIDENCE MODEL (mandatory, never mixed into QA score):
  Confidence is computed independently and controls automation trust level.
  It does NOT change the QA score — it determines how the score is used.

  Category-level confidence signals:
    - Message count (low data = lower confidence)
    - Conversation clarity (ambiguity, politeness markers)
    - Resolution certainty (explicit resolved vs ambiguous)
    - SLA data completeness (missing timestamps)
    - Customer confirmation present ("thanks", "solved")

  Overall confidence aggregation:
    Overall = 0.35 * data_completeness
            + 0.30 * ai_signal_strength
            + 0.20 * resolution_certainty
            + 0.15 * sla_data_quality

  Confidence action logic:
    ≥85  → Accept automatically
    70–84 → Accept, flag for sampling
    50–69 → Require human QA review
    <50  → Force manual evaluation, do not auto-score

FINAL SCORE FORMULA:
  process_score = ai_process * (20/30) + sla_sub * (10/30)
  comm_score    = ai_comm    * (15/20) + sla_sub * (5/20)
  cx_score      = ai_cx      * (15/20) + sla_sub * (5/20)
  final = acc*0.35 + process*0.30 + comm*0.20 + cx*0.20 + doc*0.05
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

router  = APIRouter()
logger  = logging.getLogger("simployer.qa_scorecard_v2")
_client = AsyncAnthropic(api_key=settings.anthropic_api_key, timeout=55.0)

# ── SLA configuration ─────────────────────────────────────────────────────────
PRIORITY_LABEL = {4: "Urgent", 3: "High", 2: "Medium", 1: "Low", None: "Unknown"}
SLA = {
    4: {"frt": 120,  "response": 360,  "resolution": 1440},
    3: {"frt": 240,  "response": 480,  "resolution": 1440},
    2: {"frt": 1440, "response": 1440, "resolution": 2880},
    1: {"frt": 1440, "response": 1440, "resolution": 4320},
}
SLA_DEFAULT   = {"frt": 1440, "response": 1440, "resolution": 4320}
SLA_PENALTY   = {4: 40, 3: 25, 2: 15, 1: 8, None: 10}
CRITICAL_PRI  = 4

# ── Scorecard weights ─────────────────────────────────────────────────────────
WEIGHTS = {
    "accuracy_resolution": 0.35,
    "process_compliance":  0.30,
    "communication":       0.20,
    "customer_experience": 0.20,
    "documentation":       0.05,
}

# ── Confidence weights ────────────────────────────────────────────────────────
CONF_WEIGHTS = {
    "data_completeness":    0.35,
    "ai_signal_strength":   0.30,
    "resolution_certainty": 0.20,
    "sla_data_quality":     0.15,
}

# ── Confidence action thresholds ──────────────────────────────────────────────
CONF_HIGH   = 85
CONF_MEDIUM = 70
CONF_LOW    = 50

# ── Reason codes ─────────────────────────────────────────────────────────────
RC = {
    "FEW_MESSAGES":        "Fewer than 3 messages — limited context",
    "NO_MESSAGES":         "No conversation messages available",
    "SHORT_THREAD":        "Very short thread (3–5 messages) — reduced context",
    "RICH_THREAD":         "Rich thread (10+ messages) — strong context",
    "NO_FRT_TIMESTAMP":    "First response timestamp missing — SLA confidence reduced",
    "NO_RESOLUTION_TS":    "Resolution timestamp missing — resolution certainty reduced",
    "NO_PRIORITY":         "Ticket priority not set — SLA evaluation degraded",
    "EXPLICIT_RESOLUTION": "Customer explicitly confirmed resolution",
    "AMBIGUOUS_OUTCOME":   "Resolution outcome unclear from conversation",
    "UNRESOLVED_SIGNALS":  "Conversation contains unresolved escalation signals",
    "CUSTOMER_CONFIRMED":  "Customer sent positive confirmation message",
    "NO_CUSTOMER_CONFIRM": "No explicit customer confirmation found",
    "HIGH_COMPLEXITY":     "Technical/complex language detected — AI interpretation harder",
    "CLEAR_LANGUAGE":      "Clear, direct communication — AI interpretation reliable",
    "CSAT_AVAILABLE":      "CSAT score available — CX confidence boosted",
    "NO_CSAT":             "No CSAT rating — CX confidence reduced",
    "MULTI_ESCALATION":    "Multiple escalations detected — scoring complexity increased",
    "CLEAN_RESOLUTION":    "Single clean resolution path — high scoring confidence",
    "CHURN_SIGNALS":       "Churn/cancellation signals present — outcome uncertainty raised",
}


# ── Claude prompts ────────────────────────────────────────────────────────────
SYSTEM_PROMPT = (
    "You are a senior QA analyst scoring a B2B SaaS support ticket. "
    "Score each category 0–100 based ONLY on the provided data. "
    "Also assess confidence signals that indicate how reliable the scoring is. "
    "Return ONLY valid JSON — no markdown, no preamble."
)

SCORING_PROMPT = """\
### QA Scorecard + Confidence Assessment

Score each category 0–100. Also assess confidence signals per category.
Evidence must come strictly from the conversation.

### Ticket Context
ID:        {ticket_id}
Priority:  {priority_label}
Agent:     {agent_name}
Group:     {group_name}
Subject:   {subject}
Tags:      {tags}
CSAT:      {csat}
Created:   {created_at}
Resolved:  {resolved_at}
Messages:  {msg_count}
FRT:       {frt_actual} min  (target: {frt_target} min)
Resolution:{resolution_actual} min  (target: {resolution_target} min)
SLA events:{sla_breach_summary}

### Conversation
{thread}

### Scoring Instructions

Score 0–100 per category. Include rationale + evidence + confidence signal.

**1. Accuracy / Resolution (35%)**
Problem correctly identified? Solution accurate and complete? Root cause resolved?

**2. Process & Compliance (30% — score only the NON-SLA portion = 20/30)**
Procedure adherence, escalation handling, knowledge base use, ticket hygiene.
Do NOT score SLA adherence — that is computed from timestamps.

**3. Communication (20% — score only NON-speed portion = 15/20)**
Clarity, structure, professional tone, proactive updates.
Do NOT score response time — that is computed from timestamps.

**4. Customer Experience (20% — score only NON-speed portion = 15/20)**
Empathy, ownership, confidence created, repeat contact risk.
Do NOT score speed — that is computed from timestamps.

**5. Documentation (5%)**
Internal notes, correct tagging, resolution documentation.

### Confidence Signal Instructions

For each category, assess:
- clarity: how clear and unambiguous was the relevant behaviour? (0–100)
- data_sufficiency: was there enough data to score this reliably? (0–100)
- ai_certainty: how confident are you in YOUR score? (0–100)

Also assess overall:
- resolution_certainty: was the outcome explicitly clear? (0–100)
- customer_confirmation: did the customer confirm resolution? (0–100, 0 if silent/absent)
- language_complexity: 0=highly technical/complex, 100=clear/simple (inverted: harder = lower)

Return this EXACT JSON:
{{
  "scores": {{
    "accuracy_resolution": {{"score": 0, "rationale": "", "evidence": "", "clarity": 0, "data_sufficiency": 0, "ai_certainty": 0}},
    "process_compliance":  {{"score": 0, "rationale": "", "evidence": "", "clarity": 0, "data_sufficiency": 0, "ai_certainty": 0}},
    "communication":       {{"score": 0, "rationale": "", "evidence": "", "clarity": 0, "data_sufficiency": 0, "ai_certainty": 0}},
    "customer_experience": {{"score": 0, "rationale": "", "evidence": "", "clarity": 0, "data_sufficiency": 0, "ai_certainty": 0}},
    "documentation":       {{"score": 0, "rationale": "", "evidence": "", "clarity": 0, "data_sufficiency": 0, "ai_certainty": 0}}
  }},
  "overall_signals": {{
    "resolution_certainty":    0,
    "customer_confirmation":   0,
    "language_complexity":     0,
    "conversation_coherence":  0,
    "escalation_complexity":   0
  }},
  "strengths":             ["", ""],
  "weaknesses":            ["", ""],
  "coaching_focus":        "",
  "critical_observations": ""
}}
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _repair(txt: str) -> str:
    txt = txt.strip()
    txt = re.sub(r"^```json\s*", "", txt)
    txt = re.sub(r"```\s*$", "", txt)
    return txt.strip()


def _minutes_between(dt1: Optional[datetime], dt2: Optional[datetime]) -> Optional[int]:
    if dt1 is None or dt2 is None:
        return None
    try:
        if not dt1.tzinfo: dt1 = dt1.replace(tzinfo=timezone.utc)
        if not dt2.tzinfo: dt2 = dt2.replace(tzinfo=timezone.utc)
        return max(0, int((dt2 - dt1).total_seconds() / 60))
    except Exception:
        return None


def _build_thread(messages: list) -> str:
    if not messages:
        return "No conversation messages available."
    lines = []
    for m in messages[:15]:
        ts   = str(m.ts or "")[:16].replace("T", " ")
        body = (m.body or "").strip()[:600]
        lines.append(f"[{m.role}] {ts}\n{body}")
    return "\n\n---\n\n".join(lines)


def _first_agent_reply(messages: list) -> Optional[datetime]:
    for m in messages:
        if m.role == "AGENT" and m.ts:
            return m.ts
    return None


def _max_inter_response_gap(messages: list) -> Optional[int]:
    gaps = []
    for i, m in enumerate(messages):
        if m.role == "CUSTOMER" and m.ts:
            for j in range(i + 1, len(messages)):
                if messages[j].role == "AGENT" and messages[j].ts:
                    gap = _minutes_between(m.ts, messages[j].ts)
                    if gap is not None:
                        gaps.append(gap)
                    break
    return max(gaps) if gaps else None


# ── SLA evaluation ────────────────────────────────────────────────────────────

def evaluate_sla(
    priority:          Optional[int],
    frt_actual:        Optional[int],
    resolution_actual: Optional[int],
    max_response_gap:  Optional[int],
) -> dict:
    p    = priority if priority in SLA else None
    thrs = SLA.get(p, SLA_DEFAULT)
    lbl  = PRIORITY_LABEL.get(p, "Unknown")
    pen  = SLA_PENALTY.get(p, 10)
    events        = []
    total_penalty = 0

    frt_breach = False
    if frt_actual is not None:
        if frt_actual > thrs["frt"]:
            frt_breach = True
            pct = round((frt_actual - thrs["frt"]) / thrs["frt"] * 100, 1)
            events.append({"type": "FRT_BREACH",
                "detail": f"FRT {frt_actual}min vs {thrs['frt']}min (+{pct}%)",
                "penalty": pen, "priority": lbl})
            total_penalty += pen
        else:
            events.append({"type": "FRT_PASS",
                "detail": f"FRT {frt_actual}min ≤ {thrs['frt']}min",
                "penalty": 0, "priority": lbl})

    response_breach = False
    if max_response_gap is not None:
        if max_response_gap > thrs["response"]:
            response_breach = True
            pct = round((max_response_gap - thrs["response"]) / thrs["response"] * 100, 1)
            resp_pen = max(1, pen // 2)
            events.append({"type": "RESPONSE_BREACH",
                "detail": f"Max gap {max_response_gap}min vs {thrs['response']}min (+{pct}%)",
                "penalty": resp_pen, "priority": lbl})
            total_penalty += resp_pen
        else:
            events.append({"type": "RESPONSE_PASS",
                "detail": f"Max gap {max_response_gap}min ≤ {thrs['response']}min",
                "penalty": 0, "priority": lbl})

    resolution_breach = False
    critical_failure  = False
    if resolution_actual is not None:
        if resolution_actual > thrs["resolution"]:
            resolution_breach = True
            pct = round((resolution_actual - thrs["resolution"]) / thrs["resolution"] * 100, 1)
            res_pen = pen
            if p == CRITICAL_PRI:
                critical_failure = True
                res_pen = min(100, pen + 20)
            events.append({"type": "RESOLUTION_BREACH",
                "detail": f"Resolution {resolution_actual}min vs {thrs['resolution']}min (+{pct}%)",
                "penalty": res_pen, "priority": lbl, "critical": critical_failure})
            total_penalty += res_pen
        else:
            events.append({"type": "RESOLUTION_PASS",
                "detail": f"Resolution {resolution_actual}min ≤ {thrs['resolution']}min",
                "penalty": 0, "priority": lbl})

    sla_sub_score = max(0, 100 - total_penalty)
    return {
        "sla_events": events, "frt_breach": frt_breach,
        "resolution_breach": resolution_breach, "response_breach": response_breach,
        "total_penalty": total_penalty, "sla_sub_score": sla_sub_score,
        "critical_failure": critical_failure, "thresholds_used": thrs,
        "priority_label": lbl,
    }


# ── Confidence computation ────────────────────────────────────────────────────

def compute_confidence(
    messages:          list,
    ticket:            object,
    frt_actual:        Optional[int],
    resolution_actual: Optional[int],
    ai_signals:        dict,       # from Claude's overall_signals output
    ai_cat_signals:    dict,       # per-category clarity/data_sufficiency/ai_certainty
    priority:          Optional[int],
) -> dict:
    """
    Computes confidence scores at category and overall level.
    Confidence is NEVER mixed into QA score — it controls trust/automation.

    Returns:
      category_confidence      — dict of category → 0–100
      overall_confidence       — 0–100
      confidence_level         — High / Medium / Low / Very Low
      confidence_action        — what to do with this score
      reason_codes             — list of RC keys explaining confidence
      confidence_breakdown     — detailed factor scores
    """
    reason_codes = []
    n_msgs = len(messages)

    # ── Factor 1: Data completeness (0–100) ──────────────────────────────────
    dc_score = 100

    if n_msgs == 0:
        dc_score -= 50
        reason_codes.append("NO_MESSAGES")
    elif n_msgs < 3:
        dc_score -= 30
        reason_codes.append("FEW_MESSAGES")
    elif n_msgs < 6:
        dc_score -= 10
        reason_codes.append("SHORT_THREAD")
    elif n_msgs >= 10:
        reason_codes.append("RICH_THREAD")

    if frt_actual is None:
        dc_score -= 15
        reason_codes.append("NO_FRT_TIMESTAMP")

    if resolution_actual is None:
        dc_score -= 15
        reason_codes.append("NO_RESOLUTION_TS")

    if priority is None or priority not in SLA:
        dc_score -= 10
        reason_codes.append("NO_PRIORITY")

    if ticket.csat:
        dc_score = min(100, dc_score + 5)
        reason_codes.append("CSAT_AVAILABLE")
    else:
        dc_score -= 5
        reason_codes.append("NO_CSAT")

    dc_score = max(0, min(100, dc_score))

    # ── Factor 2: AI signal strength (0–100) ─────────────────────────────────
    # Aggregate AI certainty across categories
    certainties = [v.get("ai_certainty", 70) for v in ai_cat_signals.values() if isinstance(v, dict)]
    ai_signal = round(sum(certainties) / len(certainties), 1) if certainties else 70

    lang_complexity = ai_signals.get("language_complexity", 70)
    coherence       = ai_signals.get("conversation_coherence", 70)
    ai_signal_score = round((ai_signal * 0.5 + lang_complexity * 0.3 + coherence * 0.2), 1)
    ai_signal_score = max(0, min(100, ai_signal_score))

    if lang_complexity < 50:
        reason_codes.append("HIGH_COMPLEXITY")
    elif lang_complexity >= 75:
        reason_codes.append("CLEAR_LANGUAGE")

    # ── Factor 3: Resolution certainty (0–100) ───────────────────────────────
    res_cert     = ai_signals.get("resolution_certainty", 60)
    cust_confirm = ai_signals.get("customer_confirmation", 0)

    if cust_confirm >= 70:
        reason_codes.append("CUSTOMER_CONFIRMED")
    else:
        reason_codes.append("NO_CUSTOMER_CONFIRM")

    if res_cert >= 75:
        reason_codes.append("EXPLICIT_RESOLUTION")
    elif res_cert < 50:
        reason_codes.append("AMBIGUOUS_OUTCOME")

    resolution_certainty_score = round(res_cert * 0.65 + cust_confirm * 0.35, 1)
    resolution_certainty_score = max(0, min(100, resolution_certainty_score))

    # Check for unresolved signals in existing eval
    escalation_complexity = ai_signals.get("escalation_complexity", 50)
    if escalation_complexity < 40:
        reason_codes.append("MULTI_ESCALATION")
    elif escalation_complexity >= 75:
        reason_codes.append("CLEAN_RESOLUTION")

    # ── Factor 4: SLA data quality (0–100) ──────────────────────────────────
    sla_dq = 100
    if frt_actual is None:       sla_dq -= 30
    if resolution_actual is None: sla_dq -= 30
    if priority is None:         sla_dq -= 20
    if n_msgs < 2:               sla_dq -= 20
    sla_dq = max(0, min(100, sla_dq))

    # ── Overall confidence ────────────────────────────────────────────────────
    overall = round(
        dc_score                  * CONF_WEIGHTS["data_completeness"]    +
        ai_signal_score           * CONF_WEIGHTS["ai_signal_strength"]   +
        resolution_certainty_score* CONF_WEIGHTS["resolution_certainty"] +
        sla_dq                    * CONF_WEIGHTS["sla_data_quality"],
        1
    )
    overall = max(0, min(100, overall))

    # ── Per-category confidence ───────────────────────────────────────────────
    cat_conf = {}
    cat_weights = {
        "accuracy_resolution": (0.5, 0.3, 0.2),   # certainty, data_suf, clarity
        "process_compliance":  (0.4, 0.3, 0.3),
        "communication":       (0.5, 0.25, 0.25),
        "customer_experience": (0.45, 0.3, 0.25),
        "documentation":       (0.4, 0.4, 0.2),
    }
    for cat, (wc, wd, wcl) in cat_weights.items():
        sig = ai_cat_signals.get(cat, {})
        if not isinstance(sig, dict):
            cat_conf[cat] = 65
            continue
        certainty    = sig.get("ai_certainty",    70)
        data_suf     = sig.get("data_sufficiency", 70)
        clarity      = sig.get("clarity",         70)
        # Penalise if data is very thin
        if n_msgs < 3:
            certainty = certainty * 0.7
            data_suf  = data_suf  * 0.7
        cat_conf[cat] = round(
            certainty * wc + data_suf * wd + clarity * wcl, 1
        )
        cat_conf[cat] = max(0, min(100, cat_conf[cat]))

    # ── Confidence level + action ─────────────────────────────────────────────
    if overall >= CONF_HIGH:
        level  = "High"
        action = "Accept automatically"
    elif overall >= CONF_MEDIUM:
        level  = "Medium"
        action = "Accept, flag for sampling review"
    elif overall >= CONF_LOW:
        level  = "Low"
        action = "Require human QA review"
    else:
        level  = "Very Low"
        action = "Force manual evaluation — do not auto-score"

    return {
        "category_confidence":   cat_conf,
        "overall_confidence":    overall,
        "confidence_level":      level,
        "confidence_action":     action,
        "reason_codes":          list(dict.fromkeys(reason_codes)),  # deduplicate, preserve order
        "reason_descriptions":   {rc: RC[rc] for rc in reason_codes if rc in RC},
        "confidence_breakdown":  {
            "data_completeness":     dc_score,
            "ai_signal_strength":    ai_signal_score,
            "resolution_certainty":  resolution_certainty_score,
            "sla_data_quality":      sla_dq,
        },
    }


# ── Final score assembly ──────────────────────────────────────────────────────

def assemble_final_score(ai_scores: dict, sla_result: dict) -> dict:
    sla_sub = sla_result["sla_sub_score"]

    ai_process  = ai_scores.get("process_compliance",  75)
    ai_comm     = ai_scores.get("communication",       75)
    ai_cx       = ai_scores.get("customer_experience", 75)
    ai_acc      = ai_scores.get("accuracy_resolution", 75)
    ai_doc      = ai_scores.get("documentation",       75)

    process_score = ai_process * (20/30) + sla_sub * (10/30)
    comm_score    = ai_comm    * (15/20) + sla_sub * (5/20)
    cx_score      = ai_cx      * (15/20) + sla_sub * (5/20)

    final = (
        ai_acc        * WEIGHTS["accuracy_resolution"] +
        process_score * WEIGHTS["process_compliance"]  +
        comm_score    * WEIGHTS["communication"]       +
        cx_score      * WEIGHTS["customer_experience"] +
        ai_doc        * WEIGHTS["documentation"]
    )
    final = round(min(100, max(0, final)), 1)

    if   final >= 90: band = "Exceptional"
    elif final >= 75: band = "Good"
    elif final >= 60: band = "Acceptable"
    elif final >= 45: band = "Needs Improvement"
    else:             band = "Critical"

    return {
        "final_score":      final,
        "performance_band": band,
        "category_scores": {
            "accuracy_resolution": round(ai_acc, 1),
            "process_compliance":  round(process_score, 1),
            "communication":       round(comm_score, 1),
            "customer_experience": round(cx_score, 1),
            "documentation":       round(ai_doc, 1),
        },
        "sla_contribution": {
            "sla_sub_score":          sla_sub,
            "process_sla_component":  round(sla_sub * (10/30), 1),
            "comm_speed_component":   round(sla_sub * (5/20),  1),
            "cx_speed_component":     round(sla_sub * (5/20),  1),
            "total_sla_penalty":      sla_result["total_penalty"],
        },
    }


# ── Request model ─────────────────────────────────────────────────────────────

class ScorecardV2Request(BaseModel):
    ticket_id: str


# ── Endpoint ──────────────────────────────────────────────────────────────────

@router.post("/")
async def generate_qa_scorecard_v2(
    body: ScorecardV2Request,
    user: User = Depends(current_user),
    db:   AsyncSession = Depends(get_db),
):
    t0 = time.time()

    # ── Load ticket ───────────────────────────────────────────────────────────
    tkt_res = await db.execute(
        select(Ticket).where(Ticket.id == body.ticket_id, Ticket.user_id == user.id)
    )
    ticket = tkt_res.scalar_one_or_none()
    if not ticket:
        raise HTTPException(404, f"Ticket {body.ticket_id} not found")

    # ── Load messages ─────────────────────────────────────────────────────────
    msg_res = await db.execute(
        select(Message)
        .where(Message.ticket_id == body.ticket_id, Message.user_id == user.id)
        .order_by(Message.ts.asc())
    )
    messages = msg_res.scalars().all()

    # ── Compute SLA timing ────────────────────────────────────────────────────
    priority          = ticket.priority
    priority_label    = PRIORITY_LABEL.get(priority, "Unknown")
    thrs              = SLA.get(priority, SLA_DEFAULT)
    frt_actual        = _minutes_between(ticket.created_at, _first_agent_reply(messages))
    resolution_actual = _minutes_between(ticket.created_at, ticket.resolved_at)
    max_response_gap  = _max_inter_response_gap(messages)

    # ── SLA evaluation ────────────────────────────────────────────────────────
    sla_result = evaluate_sla(priority, frt_actual, resolution_actual, max_response_gap)

    breach_types   = [e["type"] for e in sla_result["sla_events"] if "BREACH" in e["type"]]
    breach_summary = ", ".join(breach_types) or "No SLA breaches"

    # ── Claude scoring + confidence signals ──────────────────────────────────
    prompt = SCORING_PROMPT.format(
        ticket_id         = body.ticket_id,
        priority_label    = priority_label,
        agent_name        = ticket.agent_name or "Unknown",
        group_name        = ticket.group_name or "Unknown",
        subject           = ticket.subject or "N/A",
        tags              = ", ".join(ticket.tags or []) or "none",
        csat              = ticket.csat or "N/A",
        created_at        = str(ticket.created_at or "")[:16],
        resolved_at       = str(ticket.resolved_at or "")[:16],
        msg_count         = len(messages),
        frt_actual        = frt_actual if frt_actual is not None else "unknown",
        frt_target        = thrs["frt"],
        resolution_actual = resolution_actual if resolution_actual is not None else "unknown",
        resolution_target = thrs["resolution"],
        sla_breach_summary= breach_summary,
        thread            = _build_thread(messages)[:4500],
    )

    try:
        resp = await asyncio.wait_for(
            _client.messages.create(
                model       = "claude-sonnet-4-20250514",
                max_tokens  = 2500,
                temperature = 0,
                system      = SYSTEM_PROMPT,
                messages    = [{"role": "user", "content": prompt}],
            ),
            timeout=50.0,
        )
    except asyncio.TimeoutError:
        raise HTTPException(504, "Scoring timed out. Retry.")

    ai_data = json.loads(_repair(resp.content[0].text))

    # Extract AI scores and confidence signals
    ai_scores_raw  = {k: v["score"]       for k, v in ai_data.get("scores", {}).items()}
    ai_cat_signals = {k: {
        "ai_certainty":    v.get("ai_certainty",    70),
        "data_sufficiency":v.get("data_sufficiency", 70),
        "clarity":         v.get("clarity",         70),
    } for k, v in ai_data.get("scores", {}).items()}
    ai_overall_signals = ai_data.get("overall_signals", {})

    # ── Assemble weighted final score ─────────────────────────────────────────
    score_result = assemble_final_score(ai_scores_raw, sla_result)

    # ── Compute confidence (independent of score) ─────────────────────────────
    confidence = compute_confidence(
        messages          = list(messages),
        ticket            = ticket,
        frt_actual        = frt_actual,
        resolution_actual = resolution_actual,
        ai_signals        = ai_overall_signals,
        ai_cat_signals    = ai_cat_signals,
        priority          = priority,
    )

    gen_ms = round((time.time() - t0) * 1000)
    logger.info(
        f"qa_scorecard_v2 ticket={body.ticket_id} "
        f"final={score_result['final_score']} "
        f"confidence={confidence['overall_confidence']} "
        f"level={confidence['confidence_level']} "
        f"priority={priority_label} {gen_ms}ms"
    )

    return {
        # ── Ticket metadata ──────────────────────────────────────────────────
        "ticket": {
            "id":             body.ticket_id,
            "subject":        ticket.subject,
            "agent":          ticket.agent_name,
            "group":          ticket.group_name,
            "priority":       priority,
            "priority_label": priority_label,
            "tags":           ticket.tags or [],
            "csat":           ticket.csat,
            "created_at":     str(ticket.created_at or "")[:16],
            "resolved_at":    str(ticket.resolved_at or "")[:16],
            "msg_count":      len(messages),
            "arr":            float(ticket.arr) if ticket.arr else None,
        },
        # ── SLA audit ────────────────────────────────────────────────────────
        "sla": {
            "priority_label":          priority_label,
            "frt_actual_min":          frt_actual,
            "frt_target_min":          thrs["frt"],
            "frt_breach":              sla_result["frt_breach"],
            "response_target_min":     thrs["response"],
            "max_response_gap_min":    max_response_gap,
            "response_breach":         sla_result["response_breach"],
            "resolution_actual_min":   resolution_actual,
            "resolution_target_min":   thrs["resolution"],
            "resolution_breach":       sla_result["resolution_breach"],
            "sla_events":              sla_result["sla_events"],
            "sla_sub_score":           sla_result["sla_sub_score"],
            "total_penalty":           sla_result["total_penalty"],
            "critical_failure":        sla_result["critical_failure"],
        },
        # ── AI category assessment ───────────────────────────────────────────
        "ai_assessment":         ai_data.get("scores", {}),
        "strengths":             ai_data.get("strengths", []),
        "weaknesses":            ai_data.get("weaknesses", []),
        "coaching_focus":        ai_data.get("coaching_focus", ""),
        "critical_observations": ai_data.get("critical_observations", ""),
        # ── Final weighted score ──────────────────────────────────────────────
        "scoring": score_result,
        # ── Confidence model (never changes score — controls trust) ──────────
        "confidence": confidence,
        # ── Metadata ──────────────────────────────────────────────────────────
        "meta": {
            "framework_version": "2.0-confidence",
            "weights":           WEIGHTS,
            "conf_weights":      CONF_WEIGHTS,
            "gen_ms":            gen_ms,
        },
    }
