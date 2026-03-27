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

CHURN_KEYWORDS = [
    "oppsigelse", "cancel", "avslutte", "sier opp", "avslutning",
    "switching", "bytte system", "vurderer andre", "not satisfied",
    "very disappointed", "tredje gang", "third time",
    "säger upp", "avsluta", "säga upp",
]

# In-process caches (resets between runs, that's fine)
_agent_cache: Dict[int, str] = {}
_group_cache: Dict[int, str] = {}


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


async def get_agent_name(agent_id: int) -> Optional[str]:
    """Fetch agent name by ID, with in-process caching."""
    if not agent_id:
        return None
    if agent_id in _agent_cache:
        return _agent_cache[agent_id]
    try:
        url = f"https://{settings.freshdesk_domain}/api/v2/agents/{agent_id}"
        data = await fd_get(url)
        name = (data.get("contact") or {}).get("name") or data.get("name")
        _agent_cache[agent_id] = name
        return name
    except Exception as e:
        logger.warning(f"Could not fetch agent {agent_id}: {e}")
        _agent_cache[agent_id] = None
        return None


async def get_group_name(group_id: int) -> Optional[str]:
    """Fetch group name by ID, with in-process caching."""
    if not group_id:
        return None
    if group_id in _group_cache:
        return _group_cache[group_id]
    try:
        url = f"https://{settings.freshdesk_domain}/api/v2/groups/{group_id}"
        data = await fd_get(url)
        name = data.get("name")
        _group_cache[group_id] = name
        return name
    except Exception as e:
        logger.warning(f"Could not fetch group {group_id}: {e}")
        _group_cache[group_id] = None
        return None


async def enrich_ticket(ticket: Dict) -> Dict:
    """
    Add agent_name and group_name to a ticket dict.
    Strategy:
      1. Use embedded responder.name / group.name if present
      2. Fall back to fetching by responder_id / group_id
    """
    # Agent name
    responder = ticket.get("responder") or {}
    agent_name = responder.get("name")
    if not agent_name:
        responder_id = ticket.get("responder_id")
        if responder_id:
            agent_name = await get_agent_name(int(responder_id))

    # Group name
    group = ticket.get("group") or {}
    group_name = group.get("name")
    if not group_name:
        group_id = ticket.get("group_id")
        if group_id:
            group_name = await get_group_name(int(group_id))

    ticket["_agent_name"] = agent_name
    ticket["_group_name"] = group_name
    return ticket


async def fetch_all_tickets(days_back: int, since: Optional[str] = None) -> List[Dict]:
    """Fetch ALL resolved/closed tickets for the period — with agent/group enrichment."""
    if since is None:
        since = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")

    base = f"https://{settings.freshdesk_domain}/api/v2/tickets"
    tickets: List[Dict] = []
    page = 1

    while True:
        url = (
            f"{base}?updated_since={since}&per_page=100&page={page}"
            f"&include=stats,requester,description,responder,group"
            f"&order_by=updated_at&order_type=desc"
        )
        batch = await fd_get(url)
        if not batch:
            break

        resolved = [t for t in batch if t.get("status") in (4, 5)]

        # Enrich tickets with agent/group names (concurrent, capped by semaphore)
        enriched = await asyncio.gather(*[enrich_ticket(t) for t in resolved])
        tickets.extend(enriched)

        logger.info(
            f"Page {page}: {len(batch)} total, {len(resolved)} resolved "
            f"(cumul: {len(tickets)}, sample agent: {enriched[0]['_agent_name'] if enriched else 'N/A'})"
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
    """Return the matching churn keyword if found in customer messages."""
    customer_text = " ".join(
        m["body"] for m in thread if m["role"] == "CUSTOMER"
    ).lower()
    return next((kw for kw in CHURN_KEYWORDS if kw in customer_text), None)


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
