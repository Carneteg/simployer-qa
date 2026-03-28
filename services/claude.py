"""
Claude API service — ticket evaluation via Haiku.

Performance:
  - Per-call latency + token usage logged on every call
  - 25s timeout at transport level (Anthropic client) + asyncio.wait_for guard
  - Designed for concurrent use — no shared mutable state, safe for asyncio.gather
"""
import asyncio
import json
import logging
import re
import time
from typing import Dict, List

import anthropic

from config import settings

logger = logging.getLogger("simployer.claude")

# Single shared async client — thread-safe for concurrent coroutines
_client = anthropic.AsyncAnthropic(
    api_key=settings.anthropic_api_key,
    timeout=25.0,   # Hard transport-level timeout — prevents hangs
)

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
    agent_name = ticket.get("_agent_name") or (ticket.get("responder") or {}).get("name", "Unknown")
    group_name = ticket.get("_group_name") or (ticket.get("group") or {}).get("name", "Unknown")
    csat       = (ticket.get("satisfaction_rating") or {}).get("rating", "N/A")

    # Truncate aggressively: 6 turns, 200 chars each — keeps tokens predictable
    trimmed = "\n".join(
        f"{m['role'][0]}: {m['body'].replace(chr(10), ' ')[:200]}"
        for m in thread[:6]
    )

    tags            = ticket.get("tags") or []
    sf_tag          = any(t.lower().strip() == "salesforce" for t in tags)
    sf_context      = (
        "NOTE: This ticket is tagged 'salesforce' — the customer has CONFIRMED "
        "contract termination recorded in Salesforce. Set churn_risk_flag:true.\n"
        if sf_tag else ""
    )

    return (
        f"Ticket:{ticket['id']} Agent:{agent_name} Group:{group_name} "
        f"CSAT:{csat} SLA:{ticket.get('fr_escalated', False)} "
        f"Reopen:{ticket.get('nr_escalated', False)} "
        f"Tags:{','.join(tags) if tags else 'none'}\n\n"
        f"{sf_context}"
        f"CONVERSATION:\n{trimmed}\n\n"
        f"Score the agent 1-5 for each category. total_score = avg_of_8 × 20.\n\n"
        f"CHURN RISK — set churn_risk_flag:true ONLY if the customer EXPLICITLY states:\n"
        f"  - Intent to cancel their subscription or contract\n"
        f"  - They are switching to a competitor or evaluating alternatives\n"
        f"  - Severe dissatisfaction with a direct threat to leave\n"
        f"  - A repeated unresolved issue (e.g. third time contacting)\n"
        f"Do NOT flag for: complaints, confusion, pricing questions, or general support.\n"
        f"Do NOT flag based on agent quality — only explicit customer intent to leave.\n\n"
        f"CONTACT PROBLEM — set contact_problem_flag:true ONLY if the customer explicitly\n"
        f"  states this is a repeat contact for the same unresolved issue.\n\n"
        f"Return this JSON filled in:\n"
        f'{{"ticket_id":"{ticket["id"]}","agent":"{agent_name}","group":"{group_name}",'
        f'"arr":null,"complexity":"Low/Medium/High",'
        f'"sentiment":{{"start":"Neutral","end":"Neutral"}},'
        f'"scores":{json.dumps(EMPTY_SCORES)},'
        f'"total_score":60,"summary":"","strengths":[""],"improvements":[""],'
        f'"churn_risk_flag":false,"churn_risk_reason":null,'
        f'"contact_problem_flag":false,"coaching_tip":""}}'
    )


def _repair_json(txt: str) -> str:
    """Attempt to close truncated JSON."""
    txt = txt.strip()
    txt = re.sub(r"^```json\s*", "", txt)
    txt = re.sub(r"^```\s*",     "", txt)
    txt = re.sub(r"```\s*$",     "", txt)
    txt = txt.strip()
    if not txt.endswith("}"):
        opens  = txt.count("{")
        closes = txt.count("}")
        diff   = opens - closes
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

    ev.setdefault("summary",               "")
    ev.setdefault("strengths",             [])
    ev.setdefault("improvements",          [])
    ev.setdefault("churn_risk_flag",       False)
    ev.setdefault("churn_risk_reason",     None)
    ev.setdefault("contact_problem_flag",  False)
    ev.setdefault("coaching_tip",          "")
    ev.setdefault("arr",                   None)
    ev.setdefault("complexity",            "Medium")
    ev.setdefault("sentiment",             {"start": "Neutral", "end": "Neutral"})
    return ev


async def eval_ticket(ticket: Dict, thread: List[Dict], retries: int = 5) -> Dict:
    """
    Call Claude Haiku to evaluate a single ticket.
    
    - Logs latency + token usage on every call
    - 25s timeout at client level + asyncio.wait_for guard
    - Safe to call concurrently — no shared mutable state
    """
    prompt     = _build_prompt(ticket, thread)
    ticket_id  = ticket.get("id", "?")

    for attempt in range(retries):
        t0 = time.monotonic()
        try:
            response = await asyncio.wait_for(
                _client.messages.create(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=1200,
                    temperature=0,
                    system=SYSTEM_PROMPT,
                    messages=[{"role": "user", "content": prompt}],
                ),
                timeout=25.0,
            )

            latency_ms    = round((time.monotonic() - t0) * 1000)
            input_tokens  = response.usage.input_tokens
            output_tokens = response.usage.output_tokens

            logger.debug(
                f"claude haiku #{ticket_id} — "
                f"{latency_ms}ms | in={input_tokens} out={output_tokens} tokens"
            )

            # Warn on prompt bloat (input > 1500 tokens is suspicious for Haiku)
            if input_tokens > 1500:
                logger.warning(
                    f"claude #{ticket_id} — large prompt: {input_tokens} input tokens. "
                    f"Check thread truncation."
                )

            raw = response.content[0].text
            txt = _repair_json(raw)
            ev  = json.loads(txt)
            return _normalise(ev)

        except asyncio.TimeoutError:
            latency_ms = round((time.monotonic() - t0) * 1000)
            logger.warning(
                f"claude #{ticket_id} — TIMEOUT after {latency_ms}ms "
                f"(attempt {attempt + 1}/{retries})"
            )
            if attempt == retries - 1:
                logger.error(f"claude #{ticket_id} — all {retries} attempts timed out")
                return _normalise({})   # Return empty eval rather than crashing the run
            await asyncio.sleep(2 ** attempt)

        except anthropic.RateLimitError:
            wait = 2 ** (attempt + 3)
            logger.warning(
                f"claude #{ticket_id} — 429 rate limit, waiting {wait}s "
                f"(attempt {attempt + 1}/{retries})"
            )
            await asyncio.sleep(wait)

        except json.JSONDecodeError as e:
            latency_ms = round((time.monotonic() - t0) * 1000)
            logger.warning(
                f"claude #{ticket_id} — JSON parse failed in {latency_ms}ms "
                f"(attempt {attempt + 1}): {e}"
            )
            if attempt < retries - 1:
                await asyncio.sleep(3)
            else:
                raise

        except Exception as e:
            latency_ms = round((time.monotonic() - t0) * 1000)
            logger.error(
                f"claude #{ticket_id} — error in {latency_ms}ms "
                f"(attempt {attempt + 1}): {type(e).__name__}: {e}"
            )
            if attempt == retries - 1:
                raise
            await asyncio.sleep(2)

    raise RuntimeError(f"Claude eval max retries exceeded for #{ticket_id}")
