"""
POST /qa-issue/

Senior QA Manager analysis of a reported system issue.
Takes a free-text issue description and produces a structured 7-step
QA breakdown matching the template spec exactly:

  Step 1 — Issue Breakdown (failure areas table)
  Step 2 — Reproduction Strategy
  Step 3 — Functional Test Coverage
  Step 4 — Likely Root Causes (ranked)
  Step 5 — Debugging Actions
  Step 6 — QA Verdict (severity / scope / business risk)
  Step 7 — Leadership Commentary

Rules enforced in prompt:
  - Do NOT assume missing system behavior
  - Do NOT provide generic support answers
  - Think like QA, not customer support
  - Focus on system behavior, not user error
  - If information is missing → state what is needed
"""

import asyncio
import json
import re
import logging
from pydantic import BaseModel
from fastapi import APIRouter, Depends, HTTPException
from anthropic import AsyncAnthropic

from config import settings
from models import User
from routers.auth import current_user

router  = APIRouter()
logger  = logging.getLogger("simployer.qa_issue")
_client = AsyncAnthropic(api_key=settings.anthropic_api_key, timeout=45.0)

SYSTEM = (
    "You are a Senior Quality Assurance Manager responsible for validating system "
    "functionality, identifying defects, and ensuring production stability. "
    "Analyze issues like a QA leader — think in failure modes, reproduction steps, "
    "test coverage, and root cause. Do NOT assume missing system behavior. "
    "Do NOT provide generic support answers. If information is missing → explicitly "
    "state what is needed. Return ONLY valid JSON — no markdown, no preamble."
)

PROMPT = """\
### Role
You are a Senior Quality Assurance Manager.

### Critical Rules
- Do NOT assume missing system behavior
- Do NOT provide generic support answers
- Think like QA, not customer support
- Focus on system behavior, not user error (unless clearly indicated)
- If information is missing → explicitly state what is needed

### Reported Issue
{issue}

### System Context (if available)
{system_context}

Return this EXACT JSON (no other text):
{{
  "issue_breakdown": [
    {{"area": "Authentication",          "what_could_be_wrong": ""}},
    {{"area": "Authorization",           "what_could_be_wrong": ""}},
    {{"area": "Frontend / UI",           "what_could_be_wrong": ""}},
    {{"area": "Backend / API",           "what_could_be_wrong": ""}},
    {{"area": "Database",                "what_could_be_wrong": ""}},
    {{"area": "Third-party integrations","what_could_be_wrong": ""}},
    {{"area": "Network / Latency",       "what_could_be_wrong": ""}}
  ],
  "reproduction_steps": [
    {{"step": 1, "action": "", "expected_result": "", "risk": ""}}
  ],
  "functional_coverage": [
    {{"function": "", "what_to_test": "", "assumed_risk": ""}}
  ],
  "root_causes": [
    {{"priority": "High",   "root_cause": "", "reasoning": ""}},
    {{"priority": "High",   "root_cause": "", "reasoning": ""}},
    {{"priority": "Medium", "root_cause": "", "reasoning": ""}},
    {{"priority": "Medium", "root_cause": "", "reasoning": ""}},
    {{"priority": "Low",    "root_cause": "", "reasoning": ""}}
  ],
  "debugging_actions": [
    {{"action": "", "owner": "", "why": ""}}
  ],
  "verdict": {{
    "severity":          "High",
    "scope":             "Single user",
    "business_risk":     "Medium",
    "severity_reason":   "",
    "scope_reason":      "",
    "business_reason":   ""
  }},
  "leadership_commentary": "",
  "missing_information": []
}}

Rules:
- issue_breakdown: ALL 7 areas required; write 'Not applicable' if area is irrelevant to this issue
- reproduction_steps: 3–6 steps; action/expected/risk each one sentence
- functional_coverage: 4–8 functions relevant to THIS issue; assumed_risk = Critical/High/Medium/Low
- root_causes: exactly 5 rows (2 High, 2 Medium, 1 Low); reasoning must be specific to this issue
- debugging_actions: 4–7 actions; owner = Backend/Frontend/DevOps/QA/Engineering/DBA
- verdict.severity: Critical / High / Medium / Low
- verdict.scope: Single user / Segment / System-wide
- verdict.business_risk: Low / Medium / High
- leadership_commentary: 5–8 sentences — is this a serious failure, what does it say about reliability, what should be fixed long-term
- missing_information: list any data points needed to confirm root cause (empty list if enough info provided)
"""


def _repair(txt: str) -> str:
    txt = txt.strip()
    txt = re.sub(r"^```json\s*", "", txt)
    txt = re.sub(r"```\s*$",    "", txt)
    return txt.strip()


class QaIssueRequest(BaseModel):
    issue: str
    system_context: str = ""


@router.post("/")
async def analyze_issue(
    body: QaIssueRequest,
    user: User = Depends(current_user),
):
    if not body.issue or len(body.issue.strip()) < 5:
        raise HTTPException(422, "Issue description is required (min 5 characters).")

    logger.info(f"qa_issue request from {user.id}: {body.issue[:80]}")

    prompt = PROMPT.format(
        issue          = body.issue.strip()[:2000],
        system_context = body.system_context.strip()[:500] if body.system_context else "Not provided",
    )

    try:
        resp = await asyncio.wait_for(
            _client.messages.create(
                model       = "claude-sonnet-4-20250514",
                max_tokens  = 3000,
                temperature = 0,
                system      = SYSTEM,
                messages    = [{"role": "user", "content": prompt}],
            ),
            timeout=45.0,
        )
    except asyncio.TimeoutError:
        raise HTTPException(504, "Issue analysis timed out. Retry.")

    data = json.loads(_repair(resp.content[0].text))

    # Normalise verdict enums server-side
    v = data.get("verdict", {})
    if v.get("severity") not in ("Critical","High","Medium","Low"):
        v["severity"] = "High"
    if v.get("business_risk") not in ("Low","Medium","High"):
        v["business_risk"] = "Medium"

    data["meta"] = {"issue_preview": body.issue[:120]}
    logger.info(f"qa_issue complete for {user.id}")
    return data
