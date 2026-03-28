"""
POST /customer-intelligence/

Customer Lifecycle Intelligence Engine
======================================

Goes beyond single-ticket QA to evaluate:
  1. Customer support history — volume, frequency, trends, escalations
  2. Root cause analysis — why issues keep recurring across tickets
  3. Churn / termination risk — scored from behavioral signals
  4. Lifecycle fit assessment — is the customer in the right phase?
  5. Recommendation engine — ticket / customer / process / business level

Input: company_id (groups all tickets for that customer/account)

Data sources (all from our DB, no additional Freshdesk calls needed):
  - Ticket records (priority, csat, tags, dates, escalation flags)
  - Evaluation records (QA scores, churn flags, cx_bad, summaries, coaching tips)
  - Planhat fields on Ticket (planhat_phase, planhat_health, planhat_segmentation, arr)

Lifecycle phases (from Planhat via Freshdesk company fields):
  Onboarding → Early Adoption → Mature Adoption → Tailored → Churned

Scoring:
  churn_risk_score     0–100
  termination_risk     0–100
  lifecycle_fit_score  0–100 (100 = perfect fit, 0 = severe mismatch)
  root_cause_confidence 0–100

All AI analysis via Claude Sonnet (temp=0).
"""

import asyncio
import json
import re
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession
from anthropic import AsyncAnthropic

from config import settings
from database import get_db
from models import User, Ticket, Evaluation
from routers.auth import current_user

router  = APIRouter()
logger  = logging.getLogger("simployer.customer_intelligence")
_client = AsyncAnthropic(api_key=settings.anthropic_api_key, timeout=60.0)

# ── Lifecycle phase definitions ────────────────────────────────────────────────
# Maps Planhat phase strings → expected support profile
LIFECYCLE_PROFILES = {
    "Onboarding": {
        "expected_ticket_rate": "High",   # expected: many setup/how-to tickets
        "expected_types":       ["setup","configuration","how-to","basic","access"],
        "ok_to_escalate":       True,
        "description":          "High ticket volume expected — setup, access, basic how-to",
    },
    "Early Adoption": {
        "expected_ticket_rate": "Medium",
        "expected_types":       ["feature","workflow","training","integration"],
        "ok_to_escalate":       True,
        "description":          "Moderate volume — feature questions, workflow gaps",
    },
    "Mature Adoption": {
        "expected_ticket_rate": "Low",
        "expected_types":       ["bug","advanced","optimization"],
        "ok_to_escalate":       False,
        "description":          "Low volume expected — advanced bugs, optimisation only",
    },
    "Tailored": {
        "expected_ticket_rate": "Low",
        "expected_types":       ["integration","customisation","advanced"],
        "ok_to_escalate":       False,
        "description":          "Low, specialist volume — custom integrations, advanced config",
    },
    "Churned": {
        "expected_ticket_rate": "None",
        "expected_types":       ["offboarding","billing","data-export"],
        "ok_to_escalate":       False,
        "description":          "Only offboarding/billing tickets expected",
    },
}

# ── Root cause taxonomy ────────────────────────────────────────────────────────
ROOT_CAUSE_CATEGORIES = [
    "product_defect",
    "onboarding_gap",
    "knowledge_gap",
    "process_failure",
    "expectation_mismatch",
    "lifecycle_mismatch",
    "support_quality",
    "integration_complexity",
    "configuration_error",
    "unclear_ownership",
]

# ── System prompt ──────────────────────────────────────────────────────────────
SYSTEM = (
    "You are a senior Customer Success analyst and QA intelligence specialist. "
    "You analyze customer support history to identify root causes, churn risk, "
    "lifecycle mismatches, and concrete recommendations. "
    "Use ONLY the data provided. Do not assume information not present. "
    "Be specific, evidence-based, and actionable. "
    "Return ONLY valid JSON — no markdown, no preamble."
)

PROMPT = """\
### Role
Senior Customer Success analyst reviewing full support history for one customer account.

### Customer Account
Company:            {company_name}
Company ID:         {company_id}
ARR:                {arr}
Lifecycle phase:    {lifecycle_phase}
Planhat health:     {planhat_health}
Planhat segment:    {planhat_segment}

### Support History Summary
Total tickets:          {total_tickets}
Date range:             {date_range}
Avg tickets/month:      {avg_per_month}
Avg QA score:           {avg_qa_score}/100
Churn-flagged tickets:  {churn_count} ({churn_pct}%)
Bad CX tickets:         {cx_bad_count} ({cx_bad_pct}%)
Escalated tickets:      {escalated_count}
Avg resolution time:    {avg_resolution_days} days
CSAT responses:         {csat_count} (avg: {avg_csat})
SLA breaches:           {sla_breach_count}
Contact problem flags:  {contact_problem_count}

### Ticket Timeline (most recent first, up to 20)
{ticket_timeline}

### Evaluation Summaries (AI-generated per ticket)
{eval_summaries}

### Tag Cloud
{tag_cloud}

### Lifecycle Context
Current phase profile:  {lifecycle_profile_desc}
Ticket rate vs expected:{ticket_rate_assessment}
Phase-matching tags:    {phase_matching_tags}
Phase-mismatch signals: {phase_mismatch_signals}

---

Return this EXACT JSON:
{{
  "root_cause": {{
    "primary_category":    "",
    "secondary_category":  "",
    "confidence":          0,
    "explanation":         "",
    "pattern_description": "",
    "is_isolated":         false,
    "recurring_themes":    [""],
    "affected_tickets":    0
  }},
  "churn_risk": {{
    "churn_risk_score":     0,
    "termination_risk":     0,
    "risk_level":           "Low",
    "top_drivers":          [""],
    "reason_codes":         [""],
    "risk_trajectory":      "Stable",
    "key_evidence":         ""
  }},
  "lifecycle": {{
    "lifecycle_fit_score":          0,
    "mismatch_flag":                false,
    "premature_phase_movement_flag":false,
    "current_phase":                "",
    "assessed_actual_phase":        "",
    "ticket_volume_vs_expected":    "",
    "mismatch_explanation":         "",
    "phase_recommendation":         ""
  }},
  "recommendations": {{
    "ticket_level": [
      {{"action": "", "type": "Immediate", "rationale": ""}}
    ],
    "customer_level": [
      {{"action": "", "type": "Immediate", "rationale": ""}}
    ],
    "process_level": [
      {{"action": "", "type": "Structural", "rationale": ""}}
    ]
  }},
  "executive_summary":  "",
  "confidence_overall": 0,
  "missing_data_flags": [""]
}}

Rules:
- root_cause.primary_category: one of {root_cause_categories}
- root_cause.confidence: 0–100, how certain you are
- churn_risk_score: 0–100 (0=no risk, 100=actively churning)
- termination_risk: 0–100 (specific termination/cancellation signal)
- risk_level: Low / Medium / High / Critical
- risk_trajectory: Improving / Stable / Deteriorating / Critical
- lifecycle_fit_score: 0–100 (100=perfect fit, 0=severe mismatch)
- mismatch_flag: true if ticket behaviour clearly does not match lifecycle phase
- premature_phase_movement_flag: true if evidence suggests customer moved too early
- recommendations: 1–3 per level; type = Immediate / Preventive / Structural
- executive_summary: 3–5 sentences, cite specific numbers
- confidence_overall: 0–100, based on data richness
- missing_data_flags: list any fields that would improve this analysis
"""


# ── Helpers ────────────────────────────────────────────────────────────────────

def _repair(txt: str) -> str:
    txt = txt.strip()
    txt = re.sub(r"^```json\s*", "", txt)
    txt = re.sub(r"```\s*$", "", txt)
    return txt.strip()


def _fmt_days(minutes: Optional[float]) -> str:
    if minutes is None: return "unknown"
    d = minutes / (60 * 24)
    return f"{d:.1f}d" if d >= 1 else f"{minutes:.0f}m"


def _safe_float(val) -> Optional[float]:
    try: return float(val) if val is not None else None
    except: return None


# ── Request model ──────────────────────────────────────────────────────────────

class CIRequest(BaseModel):
    company_id: str


# ── Endpoint ───────────────────────────────────────────────────────────────────

@router.post("/")
async def customer_intelligence(
    body: CIRequest,
    user: User = Depends(current_user),
    db:   AsyncSession = Depends(get_db),
):
    t0 = time.time()

    # ── 1. Load all tickets for this company ───────────────────────────────────
    tkt_res = await db.execute(
        select(Ticket)
        .where(Ticket.company_id == body.company_id, Ticket.user_id == user.id)
        .order_by(Ticket.created_at.desc())
    )
    tickets = tkt_res.scalars().all()

    if not tickets:
        raise HTTPException(404, f"No tickets found for company_id={body.company_id}")

    # ── 2. Load all evaluations for these tickets ──────────────────────────────
    ticket_ids = [t.id for t in tickets]
    eval_res = await db.execute(
        select(Evaluation)
        .where(
            Evaluation.ticket_id.in_(ticket_ids),
            Evaluation.user_id == user.id,
        )
        .order_by(Evaluation.ticket_id, Evaluation.id.desc())
    )
    all_evals = eval_res.scalars().all()

    # Latest eval per ticket
    eval_map: dict[str, Evaluation] = {}
    for ev in all_evals:
        if ev.ticket_id not in eval_map:
            eval_map[ev.ticket_id] = ev

    # ── 3. Company metadata from first ticket ──────────────────────────────────
    first = tickets[0]
    company_name     = first.company_name or body.company_id
    arr              = float(first.arr) if first.arr else None
    lifecycle_phase  = first.planhat_phase or "Unknown"
    planhat_health   = first.planhat_health
    planhat_segment  = first.planhat_segmentation or "Unknown"

    # ── 4. Compute aggregate metrics ──────────────────────────────────────────
    n = len(tickets)
    evals = [eval_map[t.id] for t in tickets if t.id in eval_map]

    # Date range
    dates = [t.created_at for t in tickets if t.created_at]
    date_min = min(dates) if dates else None
    date_max = max(dates) if dates else None
    date_range_str = f"{str(date_min)[:10]} → {str(date_max)[:10]}" if date_min and date_max else "unknown"

    # Tickets per month
    if date_min and date_max and date_min != date_max:
        months = max(1, (date_max - date_min).days / 30)
        avg_per_month = round(n / months, 1)
    else:
        avg_per_month = n

    # QA scores
    scores = [float(ev.total_score) for ev in evals if ev.total_score is not None]
    avg_qa = round(sum(scores) / len(scores), 1) if scores else None

    # Churn
    churn_count   = sum(1 for ev in evals if ev.churn_risk_flag)
    churn_pct     = round(churn_count / len(evals) * 100, 1) if evals else 0

    # CX bad
    cx_bad_count  = sum(1 for ev in evals if ev.cx_bad)
    cx_bad_pct    = round(cx_bad_count / len(evals) * 100, 1) if evals else 0

    # Escalations
    escalated = sum(1 for t in tickets if t.fr_escalated or t.nr_escalated)

    # Resolution time
    res_times = []
    for t in tickets:
        if t.created_at and t.resolved_at:
            try:
                c = t.created_at if t.created_at.tzinfo else t.created_at.replace(tzinfo=timezone.utc)
                r = t.resolved_at if t.resolved_at.tzinfo else t.resolved_at.replace(tzinfo=timezone.utc)
                res_times.append((r - c).total_seconds() / 86400)
            except: pass
    avg_res_days = round(sum(res_times)/len(res_times), 1) if res_times else None

    # CSAT
    csats = [t.csat for t in tickets if t.csat]
    avg_csat = round(sum(csats)/len(csats), 1) if csats else None

    # SLA breaches (fr_escalated = SLA breach proxy)
    sla_breaches = sum(1 for t in tickets if t.fr_escalated or t.nr_escalated)

    # Contact problem
    contact_problems = sum(1 for ev in evals if ev.contact_problem_flag)

    # ── 5. Build ticket timeline ───────────────────────────────────────────────
    timeline_lines = []
    for t in tickets[:20]:
        ev = eval_map.get(t.id)
        score_str  = f"QA={ev.total_score}" if ev and ev.total_score else "QA=?"
        churn_str  = "🚨churn" if (ev and ev.churn_risk_flag) else ""
        esc_str    = "⚡esc" if (t.fr_escalated or t.nr_escalated) else ""
        tags_str   = ",".join(t.tags or [])[:40]
        priority   = {4:"Urgent",3:"High",2:"Medium",1:"Low"}.get(t.priority,"?")
        date_str   = str(t.created_at)[:10] if t.created_at else "?"
        summary    = (ev.summary or "")[:80] if ev else ""
        timeline_lines.append(
            f"  [{date_str}] #{t.id} {priority} {score_str} {churn_str}{esc_str} tags:[{tags_str}]\n"
            f"    {summary}"
        )
    ticket_timeline = "\n".join(timeline_lines) or "No timeline data"

    # ── 6. Eval summaries ──────────────────────────────────────────────────────
    eval_lines = []
    for t in tickets[:15]:
        ev = eval_map.get(t.id)
        if ev and (ev.summary or ev.coaching_tip or ev.churn_risk_reason):
            eval_lines.append(
                f"  #{t.id}: {(ev.summary or '')[:120]}\n"
                f"    coaching: {(ev.coaching_tip or '')[:80]}\n"
                f"    churn_reason: {(ev.churn_risk_reason or '')[:80]}"
            )
    eval_summaries = "\n".join(eval_lines) or "No evaluation summaries available"

    # ── 7. Tag cloud ───────────────────────────────────────────────────────────
    tag_freq: dict[str, int] = {}
    for t in tickets:
        for tag in (t.tags or []):
            tag_freq[tag] = tag_freq.get(tag, 0) + 1
    tag_cloud = ", ".join(f"{k}({v})" for k, v in sorted(tag_freq.items(), key=lambda x: -x[1])[:20])

    # ── 8. Lifecycle analysis signals ─────────────────────────────────────────
    lc_profile = LIFECYCLE_PROFILES.get(lifecycle_phase, {})
    lc_desc    = lc_profile.get("description", "Unknown lifecycle phase")
    expected_rate = lc_profile.get("expected_ticket_rate", "Unknown")
    expected_types = lc_profile.get("expected_types", [])

    # Rate assessment
    if expected_rate == "Low" and avg_per_month > 3:
        ticket_rate_assessment = f"HIGH (avg {avg_per_month}/mo) — exceeds Low expectation for {lifecycle_phase}"
        phase_mismatch_signals = f"Ticket volume {avg_per_month}/mo too high for {lifecycle_phase} phase"
    elif expected_rate == "Medium" and avg_per_month > 8:
        ticket_rate_assessment = f"HIGH (avg {avg_per_month}/mo) — exceeds Medium expectation"
        phase_mismatch_signals = f"Support burden elevated for {lifecycle_phase}"
    elif expected_rate == "None" and avg_per_month > 0.5:
        ticket_rate_assessment = f"UNEXPECTED ({avg_per_month}/mo) — churned customers should not create tickets"
        phase_mismatch_signals = "Active ticket creation despite Churned status"
    else:
        ticket_rate_assessment = f"Normal (avg {avg_per_month}/mo for {lifecycle_phase})"
        phase_mismatch_signals = "None detected"

    # Tag matching
    matching = [t for t in tag_freq if any(e in t.lower() for e in expected_types)]
    non_matching = [t for t in tag_freq if t not in matching][:5]
    phase_matching_tags  = ", ".join(matching[:8]) or "none"

    logger.info(f"ci data built: {round((time.time()-t0)*1000)}ms, {n} tickets, {len(evals)} evals")

    # ── 9. Claude call ─────────────────────────────────────────────────────────
    prompt = PROMPT.format(
        company_name           = company_name,
        company_id             = body.company_id,
        arr                    = f"NOK {arr/1000000:.2f}M" if arr else "Unknown",
        lifecycle_phase        = lifecycle_phase,
        planhat_health         = planhat_health or "Unknown",
        planhat_segment        = planhat_segment,
        total_tickets          = n,
        date_range             = date_range_str,
        avg_per_month          = avg_per_month,
        avg_qa_score           = avg_qa or "N/A",
        churn_count            = churn_count,
        churn_pct              = churn_pct,
        cx_bad_count           = cx_bad_count,
        cx_bad_pct             = cx_bad_pct,
        escalated_count        = escalated,
        avg_resolution_days    = _fmt_days(avg_res_days * 1440 if avg_res_days else None),
        csat_count             = len(csats),
        avg_csat               = avg_csat or "N/A",
        sla_breach_count       = sla_breaches,
        contact_problem_count  = contact_problems,
        ticket_timeline        = ticket_timeline[:4000],
        eval_summaries         = eval_summaries[:3000],
        tag_cloud              = tag_cloud or "no tags",
        lifecycle_profile_desc = lc_desc,
        ticket_rate_assessment = ticket_rate_assessment,
        phase_matching_tags    = phase_matching_tags,
        phase_mismatch_signals = phase_mismatch_signals,
        root_cause_categories  = ", ".join(ROOT_CAUSE_CATEGORIES),
    )

    t1 = time.time()
    try:
        resp = await asyncio.wait_for(
            _client.messages.create(
                model       = "claude-sonnet-4-20250514",
                max_tokens  = 3500,
                temperature = 0,
                system      = SYSTEM,
                messages    = [{"role": "user", "content": prompt}],
            ),
            timeout=60.0,
        )
    except asyncio.TimeoutError:
        raise HTTPException(504, "Customer intelligence timed out. Retry.")

    data = json.loads(_repair(resp.content[0].text))
    gen_ms = round((time.time() - t1) * 1000)
    logger.info(
        f"customer_intelligence company={body.company_id} "
        f"churn={data.get('churn_risk',{}).get('churn_risk_score')} "
        f"lc_fit={data.get('lifecycle',{}).get('lifecycle_fit_score')} "
        f"{gen_ms}ms"
    )

    return {
        # ── Account context ───────────────────────────────────────────────────
        "account": {
            "company_id":       body.company_id,
            "company_name":     company_name,
            "arr":              arr,
            "lifecycle_phase":  lifecycle_phase,
            "planhat_health":   planhat_health,
            "planhat_segment":  planhat_segment,
        },
        # ── Support history metrics ───────────────────────────────────────────
        "history": {
            "total_tickets":       n,
            "evaluated_tickets":   len(evals),
            "date_range":          date_range_str,
            "avg_per_month":       avg_per_month,
            "avg_qa_score":        avg_qa,
            "churn_count":         churn_count,
            "churn_pct":           churn_pct,
            "cx_bad_count":        cx_bad_count,
            "cx_bad_pct":          cx_bad_pct,
            "escalated_count":     escalated,
            "avg_resolution_days": avg_res_days,
            "csat_count":          len(csats),
            "avg_csat":            avg_csat,
            "sla_breach_count":    sla_breaches,
            "contact_problem_count": contact_problems,
            "top_tags":            dict(sorted(tag_freq.items(), key=lambda x: -x[1])[:10]),
            "ticket_rate_assessment": ticket_rate_assessment,
        },
        # ── AI analysis ───────────────────────────────────────────────────────
        "root_cause":       data.get("root_cause", {}),
        "churn_risk":       data.get("churn_risk", {}),
        "lifecycle":        data.get("lifecycle", {}),
        "recommendations":  data.get("recommendations", {}),
        "executive_summary": data.get("executive_summary", ""),
        "confidence_overall": data.get("confidence_overall", 0),
        "missing_data_flags": data.get("missing_data_flags", []),
        # ── Meta ──────────────────────────────────────────────────────────────
        "meta": {
            "framework_version": "1.0-ci",
            "gen_ms":            gen_ms,
        },
    }
