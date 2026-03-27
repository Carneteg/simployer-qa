import asyncio
import json
import logging
import re
from typing import Dict, List, Any, Optional

import anthropic

from config import settings

logger = logging.getLogger("simployer.claude")

_client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)

CATEGORIES = [
    "clarity_structure",
    "tone_professionalism",
    "empathy",
    "accuracy",
    "resolution_quality",
    "efficiency",
    "ownership",
    "commercial_awareness",
]

EMPTY_SCORES = {cat: {"score": 3, "reason": ""} for cat in CATEGORIES}

SYSTEM_PROMPT = (
    "You are a senior QA analyst for Simployer, a Nordic HR and payroll SaaS company. "
    "Your job is to evaluate AGENT performance only — not customer behaviour. "
    "Be strict and evidence-based. "
    "Return ONLY valid JSON — no markdown, no preamble, no trailing text. "
    "Every field is required. Keep all string values concise (under 100 chars). "
    "Never truncate the JSON — always close every brace."
)


def _build_prompt(ticket: Dict, thread: List[Dict]) -> str:
    agent_name = (ticket.get("responder") or {}).get("name", "Unknown")
    group_name = (ticket.get("group") or {}).get("name", "Unknown")
    csat = (ticket.get("satisfaction_rating") or {}).get("rating", "N/A")

    # Truncate aggressively: 6 turns, 200 chars each
    trimmed = "\n".join(
        f"{m['role'][0]}: {m['body'].replace(chr(10), ' ')[:200]}"
        for m in thread[:6]
    )

    scores_template = json.dumps(EMPTY_SCORES)

    return (
        f"Ticket:{ticket['id']} Agent:{agent_name} Group:{group_name} "
        f"CSAT:{csat} SLA:{ticket.get('fr_escalated', False)} "
        f"Reopen:{ticket.get('nr_escalated', False)}\n\n"
        f"CONVERSATION:\n{trimmed}\n\n"
        f"Score the agent 1-5 for each category. total_score = avg_of_8 × 20.\n"
        f"Return this JSON filled in:\n"
        f'{{"ticket_id":"{ticket["id"]}","agent":"{agent_name}","group":"{group_name}",'
        f'"arr":null,"complexity":"Low/Medium/High",'
        f'"sentiment":{{"start":"Neutral","end":"Neutral"}},'
        f'"scores":{scores_template},'
        f'"total_score":60,"summary":"","strengths":[""],"improvements":[""],'
        f'"churn_risk_flag":false,"churn_risk_reason":null,'
        f'"contact_problem_flag":false,"coaching_tip":""}}'
    )


def _repair_json(txt: str) -> str:
    """Attempt to close truncated JSON."""
    txt = txt.strip()
    txt = re.sub(r"^```json\s*", "", txt)
    txt = re.sub(r"^```\s*", "", txt)
    txt = re.sub(r"```\s*$", "", txt)
    txt = txt.strip()
    if not txt.endswith("}"):
        opens = txt.count("{")
        closes = txt.count("}")
        diff = opens - closes
        if 0 < diff <= 6:
            if txt.count('"') % 2 != 0:
                txt += '"'
            txt += "}" * diff
    return txt


def _normalise(ev: Dict) -> Dict:
    """Ensure all expected fields exist and compute total_score."""
    ev.setdefault("scores", {})
    for cat in CATEGORIES:
        ev["scores"].setdefault(cat, {"score": 3, "reason": ""})

    vals = [ev["scores"][cat].get("score", 3) for cat in CATEGORIES]
    ev["total_score"] = round(sum(vals) / len(vals) * 20, 1) if vals else 60

    # Aliases used by the frontend
    ev["scores"]["clarity"] = ev["scores"].get("clarity_structure")
    ev["scores"]["tone"]    = ev["scores"].get("tone_professionalism")

    ev.setdefault("summary", "")
    ev.setdefault("strengths", [])
    ev.setdefault("improvements", [])
    ev.setdefault("churn_risk_flag", False)
    ev.setdefault("churn_risk_reason", None)
    ev.setdefault("contact_problem_flag", False)
    ev.setdefault("coaching_tip", "")
    ev.setdefault("arr", None)
    ev.setdefault("complexity", "Medium")
    ev.setdefault("sentiment", {"start": "Neutral", "end": "Neutral"})
    return ev


async def eval_ticket(ticket: Dict, thread: List[Dict], retries: int = 5) -> Dict:
    """
    Call Claude Haiku to evaluate a ticket.
    Returns a normalised evaluation dict.
    """
    prompt = _build_prompt(ticket, thread)

    for attempt in range(retries):
        try:
            response = await _client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=1200,
                temperature=0,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = response.content[0].text
            txt = _repair_json(raw)
            ev = json.loads(txt)
            return _normalise(ev)

        except anthropic.RateLimitError:
            wait = 2 ** (attempt + 3)
            logger.warning(f"Claude 429 — waiting {wait}s (attempt {attempt + 1})")
            await asyncio.sleep(wait)

        except json.JSONDecodeError as e:
            logger.warning(f"JSON parse failed (attempt {attempt + 1}): {e}")
            if attempt < retries - 1:
                await asyncio.sleep(3)
            else:
                raise

        except Exception as e:
            if attempt == retries - 1:
                raise
            await asyncio.sleep(2)

    raise RuntimeError("Claude eval max retries exceeded")
