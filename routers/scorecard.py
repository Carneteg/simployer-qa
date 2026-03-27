"""
Manual ticket scorecard — paste any ticket, get a structured QA evaluation.
"""
import json
import re
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from anthropic import AsyncAnthropic
from config import settings
from models import User
from routers.auth import current_user

router = APIRouter()
_client = AsyncAnthropic(api_key=settings.anthropic_api_key)

SYSTEM = (
    "You are a senior Quality Assurance analyst in Customer Care. "
    "Evaluate the provided support ticket and return ONLY valid JSON — "
    "no markdown, no preamble, no trailing text. "
    "Every field is required. Keep rationale strings concise (under 120 chars each)."
)

PROMPT = """Evaluate this support ticket on the 10 QA dimensions below.
Score each 1–5 (1=poor, 2=below expectations, 3=acceptable, 4=strong, 5=excellent).
Return this exact JSON structure filled in:

{
  "categories": {
    "understanding_of_issue":       {"score": 0, "rationale": ""},
    "accuracy_of_solution":         {"score": 0, "rationale": ""},
    "completeness_of_response":     {"score": 0, "rationale": ""},
    "clarity_of_communication":     {"score": 0, "rationale": ""},
    "tone_and_empathy":             {"score": 0, "rationale": ""},
    "efficiency":                   {"score": 0, "rationale": ""},
    "ownership":                    {"score": 0, "rationale": ""},
    "proactivity":                  {"score": 0, "rationale": ""},
    "customer_confidence_created":  {"score": 0, "rationale": ""},
    "risk_of_repeat_contact":       {"score": 0, "rationale": ""}
  },
  "average_score": 0.0,
  "verdict": "Acceptable",
  "strengths": ["", ""],
  "weaknesses": ["", ""],
  "final_comment": ""
}

Rules:
- verdict must be one of: Excellent / Strong / Acceptable / Weak / Poor
- risk_of_repeat_contact: score 5 = very low risk, score 1 = near-certain repeat
- average_score = mean of all 10 category scores, rounded to 1 decimal
- strengths/weaknesses: 2–4 items each, grounded in ticket content
- final_comment: 3–5 sentences, sharp and useful for a support leadership team

TICKET:
{ticket}"""


def _repair(txt: str) -> str:
    txt = txt.strip()
    txt = re.sub(r"^```json\s*", "", txt)
    txt = re.sub(r"```\s*$", "", txt)
    return txt.strip()


class ScorecardRequest(BaseModel):
    ticket: str


@router.post("/")
async def generate_scorecard(
    body: ScorecardRequest,
    user: User = Depends(current_user),
):
    if len(body.ticket.strip()) < 20:
        return {"error": "Ticket text is too short to evaluate."}

    response = await _client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1800,
        temperature=0,
        system=SYSTEM,
        messages=[{"role": "user", "content": PROMPT.format(ticket=body.ticket[:6000])}],
    )
    raw = response.content[0].text
    data = json.loads(_repair(raw))

    # Recalculate average server-side for safety
    scores = [v["score"] for v in data["categories"].values()]
    data["average_score"] = round(sum(scores) / len(scores), 1)

    # Map verdict from score if not set correctly
    avg = data["average_score"]
    if avg >= 4.5:
        data["verdict"] = "Excellent"
    elif avg >= 3.8:
        data["verdict"] = "Strong"
    elif avg >= 2.8:
        data["verdict"] = "Acceptable"
    elif avg >= 2.0:
        data["verdict"] = "Weak"
    else:
        data["verdict"] = "Poor"

    return data
