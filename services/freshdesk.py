import asyncio
import logging
import re
from asyncio import Semaphore
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

import httpx

from config import settings

logger = logging.getLogger("simployer.freshdesk")

_sem = Semaphore(5)  # max 5 concurrent Freshdesk calls

# ── Churn keyword detection ────────────────────────────────────────────────
# EVIDENCE-BASED: keyword list derived from data analysis.
#
# REMOVED — confirmed false positives:
#   "avslutte"   → means "to finish/complete" in everyday Norwegian
#                  e.g. "avslutte kurs" (complete a course) — NOT cancellation
#   "avslutning" → same root, same problem
#   "cancel"     → too broad, catches "cancel this meeting / cancel my order"
#   "switching"  → too broad, catches unrelated context
#
# KEPT — high-specificity contract/subscription cancellation signals:
CHURN_KEYWORDS = [
    # Norwegian — explicit contract termination
    "si opp avtalen",
    "sier opp",
    "oppsigelse",
    "avslutte abonnementet",
    "avslutte avtalen",
    "avslutte kundeforholdet",
    "bytte system",
    "vurderer andre",
    "ikke fornøyd",
    "tredje gang",

    # Swedish — explicit contract termination
    "säger upp",
    "säga upp",
    "avsluta abonnemanget",
    "avsluta avtalet",
    "byta system",

    # English — explicit contract/subscription signals
    "cancel my subscription",
    "cancel our contract",
    "cancel the agreement",
    "cancel our account",
    "switching to another",
    "looking for alternatives",
    "very disappointed",
    "not satisfied with",
    "third time",
]

# In-memory caches — populated once per process lifetime
_agent_cache: Dict[int, str] = {}   # agent_id → name
_group_cache: Dict[int, str] = {}   # group_id → name


async def fd_get(url: str, retries: int = 5) -> Any:
    """GET a Freshdesk URL with rate-limit retry and concurrency cap."""
    async with _sem:
        for attempt in range(retries):
            try:
                async with httpx.AsyncClient(timeout=30) as client:
                    r = await client.get(
                        url,
                        auth=(settings.freshdesk_api_key, "X"),
                    )
                    if r.status_code == 429:
                        wait = int(r.headers.get("retry-after", 60))
                        logger.warning(f"Freshdesk 429 — waiting {wait}s (attempt {attempt + 1})")
                        await asyncio.sleep(wait)
                        continue
                    r.raise_for_status()
                    return r.json()
            except httpx.HTTPStatusError as e:
                if attempt == retries - 1:
                    raise
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                if attempt == retries - 1:
                    raise
                await asyncio.sleep(2 ** attempt)
        raise RuntimeError(f"Freshdesk max retries exceeded: {url}")


async def _load_agents() -> Dict[int, str]:
    """Fetch all agents from Freshdesk and cache id → name."""
    global _agent_cache
    if _agent_cache:
        return _agent_cache
    try:
        base = f"https://{settings.freshdesk_domain}/api/v2/agents"
        page = 1
        while True:
            data = await fd_get(f"{base}?per_page=100&page={page}")
            if not data:
                break
            for a in data:
                _agent_cache[a["id"]] = (
                    a.get("contact", {}).get("name") or
                    a.get("name") or
                    f"Agent #{a['id']}"
                )
            if len(data) < 100:
                break
            page += 1
        logger.info(f"Loaded {len(_agent_cache)} agents from Freshdesk")
    except Exception as e:
        logger.warning(f"Could not load agents: {e}")
    return _agent_cache


async def _load_groups() -> Dict[int, str]:
    """Fetch all groups from Freshdesk and cache id → name."""
    global _group_cache
    if _group_cache:
        return _group_cache
    try:
        data = await fd_get(
            f"https://{settings.freshdesk_domain}/api/v2/groups?per_page=100"
        )
        for g in (data or []):
            _group_cache[g["id"]] = g.get("name") or f"Group #{g['id']}"
        logger.info(f"Loaded {len(_group_cache)} groups from Freshdesk")
    except Exception as e:
        logger.warning(f"Could not load groups: {e}")
    return _group_cache


async def fetch_all_tickets(days_back: int, since: Optional[str] = None) -> List[Dict]:
    """Fetch ALL resolved/closed tickets for the period — no cap.
    
    Freshdesk list endpoint only returns responder_id / group_id.
    We pre-load the agent + group name caches and inject them into each ticket.
    """
    if since is None:
        since = (
            datetime.now(timezone.utc) - timedelta(days=days_back)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Pre-load name caches in parallel
    agents, groups = await asyncio.gather(_load_agents(), _load_groups())

    base = f"https://{settings.freshdesk_domain}/api/v2/tickets"
    tickets: List[Dict] = []
    page = 1

    while True:
        url = (
            f"{base}?updated_since={since}&per_page=100&page={page}"
            f"&include=stats&include=requester"
            f"&order_by=updated_at&order_type=desc"
        )
        batch = await fd_get(url)
        if not batch:
            break

        resolved = [t for t in batch if t.get("status") in (4, 5)]

        # Inject human-readable names from caches
        for t in resolved:
            rid = t.get("responder_id")
            gid = t.get("group_id")
            t["_agent_name"] = agents.get(rid) if rid else None
            t["_group_name"] = groups.get(gid) if gid else None

        tickets.extend(resolved)
        logger.info(
            f"Page {page}: {len(batch)} total, {len(resolved)} resolved "
            f"(cumul: {len(tickets)})"
        )

        if len(batch) < 100:
            break
        page += 1
        await asyncio.sleep(0.2)

    return tickets


async def fetch_conversations(ticket_id: str) -> List[Dict]:
    """Fetch all conversation messages for a ticket."""
    url = f"https://{settings.freshdesk_domain}/api/v2/tickets/{ticket_id}/conversations"
    try:
        return await fd_get(url) or []
    except Exception as e:
        logger.warning(f"Conversations failed for #{ticket_id}: {e}")
        return []


def strip_html(html: str) -> str:
    """Strip HTML tags and decode entities."""
    if not html:
        return ""
    text = re.sub(r"<script[\s\S]*?</script>", "", html, flags=re.IGNORECASE)
    text = re.sub(r"<style[\s\S]*?</style>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</p>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&nbsp;", " ").replace("&quot;", '"')
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def build_thread(ticket: Dict, convs: List[Dict]) -> List[Dict]:
    """Combine ticket description + conversation replies into a thread."""
    thread = []
    desc = strip_html(ticket.get("description") or ticket.get("description_text") or "")
    if desc and len(desc) > 5:
        thread.append({
            "role": "CUSTOMER",
            "ts": ticket.get("created_at", ""),
            "body": desc,
        })
    for c in sorted(convs, key=lambda x: x.get("created_at", "")):
        body = strip_html(c.get("body", ""))
        if not body or len(body) < 5 or c.get("private"):
            continue
        role = "AGENT" if c.get("incoming") is False else "CUSTOMER"
        thread.append({
            "role": role,
            "ts": c.get("created_at", ""),
            "body": body,
        })
    return thread


def detect_churn(thread: List[Dict]) -> Optional[str]:
    """
    Return the matching churn keyword if found in CUSTOMER messages only.

    Uses phrase-level matching — all keywords are multi-word or highly specific
    to avoid false positives from common words like "avslutte" (finish/complete).

    Returns the matched keyword string, or None if no match.
    """
    # Only scan customer turns — agent language can contain these words innocuously
    customer_text = " ".join(
        m["body"] for m in thread if m["role"] == "CUSTOMER"
    ).lower()

    # Phrase matching — substring is intentional since keywords are already specific
    return next((kw for kw in CHURN_KEYWORDS if kw in customer_text), None)


def is_confirmed_churn(ticket: Dict) -> bool:
    """
    Returns True if the ticket carries a 'salesforce' tag.

    Business rule (from data analysis):
      A ticket tagged 'salesforce' that also carries churn_risk_flag=True means
      the customer has ACTUALLY terminated their contract — this is a confirmed
      termination event, not a predicted/suspected churn risk.

      The Salesforce tag is applied by the CRM integration when a contract
      termination has been recorded in Salesforce for that customer.

    This is distinct from keyword or AI-inferred churn risk:
      - Suspected churn  : keyword match or Claude inference (may be wrong)
      - Confirmed churn  : salesforce tag present = contract terminated (ground truth)
    """
    tags = [t.lower().strip() for t in (ticket.get("tags") or [])]
    return "salesforce" in tags


def frt_minutes(ticket: Dict) -> Optional[int]:
    """First response time in minutes."""
    try:
        stats = ticket.get("stats") or {}
        frt_at = stats.get("first_responded_at")
        if not frt_at:
            return None
        t0 = datetime.fromisoformat(ticket["created_at"].replace("Z", "+00:00"))
        t1 = datetime.fromisoformat(frt_at.replace("Z", "+00:00"))
        return int((t1 - t0).total_seconds() / 60)
    except Exception:
        return None
