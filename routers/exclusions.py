"""
Exclusion management — control which tickets are excluded from QA measurement.

Endpoints:
  GET  /exclusions/rules          — get current auto-exclusion rule config
  POST /exclusions/rules          — update auto-exclusion rules (tags, prefixes)
  GET  /exclusions/               — list all excluded tickets
  POST /exclusions/toggle         — manually exclude or include a specific ticket
  POST /exclusions/apply-rules    — re-scan all tickets and apply current rules
  GET  /exclusions/stats          — count excluded by reason
"""

import logging
from typing import Optional, List
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, update, text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db, AsyncSessionLocal
from models import User, Ticket
from routers.auth import current_user
from services.cache import get as cache_get, set as cache_set, redis

router = APIRouter()
logger = logging.getLogger("simployer.exclusions")

# ── Default auto-exclusion rules (stored in Redis per user) ───────────────────
DEFAULT_RULES = {
    "exclude_tags": [
        "call_finished", "talkdesk", "spam", "talkdesk-call",
        "voicemail", "missed-call", "auto-reply", "noreply",
        "no-reply", "system", "autoresponder", "out-of-office",
    ],
    "exclude_subject_prefixes": [
        "auto:", "automatic reply:", "out of office:", "autosvar:",
        "automatiskt svar:", "abwesenheitsnotiz:", "delivery failure",
        "undeliverable:", "mailer-daemon", "[spam]",
    ],
    "exclude_no_messages": True,   # exclude tickets with zero conversation messages
    "exclude_phone_source": True,  # exclude Freshdesk source=3 (phone/Talkdesk)
}

RULES_TTL = 86400 * 30  # 30 days — rules persist across restarts


def _rules_key(user_id) -> str:
    return f"excl_rules:v1:{user_id}"


async def get_rules(user_id) -> dict:
    cached = await cache_get(_rules_key(user_id))
    return cached if cached else DEFAULT_RULES


# ── Request models ─────────────────────────────────────────────────────────────

class RulesUpdate(BaseModel):
    exclude_tags:              List[str]
    exclude_subject_prefixes:  List[str]
    exclude_no_messages:       bool = True
    exclude_phone_source:      bool = True


class ToggleRequest(BaseModel):
    ticket_id: str
    excluded:  bool
    reason:    Optional[str] = None   # if None and excluded=True → "manual"


# ── Helpers ────────────────────────────────────────────────────────────────────

def _check_ticket(ticket: dict, rules: dict) -> tuple[bool, str]:
    """Apply rules to a ticket dict or ORM object. Returns (excluded, reason)."""
    tags    = {t.lower().strip() for t in (getattr(ticket, 'tags', None) or ticket.get('tags') or [])}
    subject = (getattr(ticket, 'subject', None) or ticket.get('subject') or '').lower().strip()

    # Tag rules
    for tag in rules.get("exclude_tags", []):
        if tag.lower() in tags:
            return True, f"rule:tag:{tag}"

    # Subject prefix rules
    for prefix in rules.get("exclude_subject_prefixes", []):
        if subject.startswith(prefix.lower()):
            return True, "rule:subject_prefix"

    return False, ""


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.get("/rules")
async def get_exclusion_rules(user: User = Depends(current_user)):
    """Return current auto-exclusion rules for this user."""
    return await get_rules(user.id)


@router.post("/rules")
async def update_exclusion_rules(
    body: RulesUpdate,
    user: User = Depends(current_user),
):
    """Save updated exclusion rules. Does NOT re-scan existing tickets automatically."""
    rules = {
        "exclude_tags":             [t.lower().strip() for t in body.exclude_tags if t.strip()],
        "exclude_subject_prefixes": [p.lower().strip() for p in body.exclude_subject_prefixes if p.strip()],
        "exclude_no_messages":      body.exclude_no_messages,
        "exclude_phone_source":     body.exclude_phone_source,
    }
    await cache_set(_rules_key(user.id), rules, RULES_TTL)
    return {"saved": True, "rules": rules}


@router.get("/")
async def list_excluded_tickets(
    user: User  = Depends(current_user),
    db: AsyncSession = Depends(get_db),
):
    """Return all excluded tickets for this user."""
    res = await db.execute(
        select(
            Ticket.id,
            Ticket.subject,
            Ticket.agent_name,
            Ticket.group_name,
            Ticket.tags,
            Ticket.excluded,
            Ticket.exclude_reason,
            Ticket.created_at,
            Ticket.priority,
        )
        .where(Ticket.user_id == user.id, Ticket.excluded == True)
        .order_by(Ticket.created_at.desc())
        .limit(500)
    )
    rows = res.mappings().all()
    return [
        {
            "ticket_id":      str(r["id"]),
            "subject":        r["subject"] or "",
            "agent_name":     r["agent_name"] or "",
            "group_name":     r["group_name"] or "",
            "tags":           r["tags"] or [],
            "exclude_reason": r["exclude_reason"] or "",
            "created_at":     str(r["created_at"] or "")[:10],
            "priority":       r["priority"],
        }
        for r in rows
    ]


@router.post("/toggle")
async def toggle_exclusion(
    body: ToggleRequest,
    user: User  = Depends(current_user),
    db: AsyncSession = Depends(get_db),
):
    """Manually exclude or re-include a ticket. Survives re-runs."""
    res = await db.execute(
        select(Ticket).where(Ticket.id == body.ticket_id, Ticket.user_id == user.id)
    )
    ticket = res.scalar_one_or_none()
    if not ticket:
        raise HTTPException(404, f"Ticket {body.ticket_id} not found")

    ticket.excluded       = body.excluded
    ticket.exclude_reason = (body.reason or "manual") if body.excluded else None
    await db.commit()

    logger.info(f"Ticket {body.ticket_id} {'excluded' if body.excluded else 'included'} manually by {user.email}")
    return {
        "ticket_id":      body.ticket_id,
        "excluded":       body.excluded,
        "exclude_reason": ticket.exclude_reason,
    }


@router.post("/apply-rules")
async def apply_rules_to_all(
    user: User  = Depends(current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Re-scan ALL tickets and apply current exclusion rules.
    Skips tickets that were manually excluded/included (exclude_reason='manual').
    Returns counts of newly excluded and newly included tickets.
    """
    rules = await get_rules(user.id)

    # Load all tickets for this user
    res = await db.execute(
        select(Ticket).where(Ticket.user_id == user.id)
    )
    tickets = res.scalars().all()

    newly_excluded = 0
    newly_included = 0

    for t in tickets:
        # Never touch manually-managed exclusions
        if t.exclude_reason == "manual":
            continue

        should_exclude, reason = _check_ticket(t, rules)

        # no_messages rule requires knowing msg count — use a proxy
        if not should_exclude and rules.get("exclude_no_messages"):
            # If previously auto_no_messages, keep excluded
            if t.exclude_reason == "auto_no_messages":
                should_exclude = True
                reason = "auto_no_messages"

        if should_exclude and not t.excluded:
            t.excluded = True
            t.exclude_reason = reason
            newly_excluded += 1
        elif not should_exclude and t.excluded:
            t.excluded = False
            t.exclude_reason = None
            newly_included += 1

    await db.commit()
    logger.info(f"apply-rules: +{newly_excluded} excluded, +{newly_included} included for {user.email}")
    return {
        "newly_excluded": newly_excluded,
        "newly_included": newly_included,
        "total_scanned":  len(tickets),
        "rules_applied":  rules,
    }


@router.get("/stats")
async def exclusion_stats(
    user: User  = Depends(current_user),
    db: AsyncSession = Depends(get_db),
):
    """Count of excluded tickets by reason code."""
    res = await db.execute(
        text("""
            SELECT
                COUNT(*) FILTER (WHERE excluded = false)     AS active,
                COUNT(*) FILTER (WHERE excluded = true)      AS total_excluded,
                COUNT(*) FILTER (WHERE exclude_reason LIKE 'auto_tag%')    AS auto_tag,
                COUNT(*) FILTER (WHERE exclude_reason = 'auto_no_messages') AS auto_no_msg,
                COUNT(*) FILTER (WHERE exclude_reason = 'auto_phone_source') AS auto_phone,
                COUNT(*) FILTER (WHERE exclude_reason = 'auto_subject')    AS auto_subject,
                COUNT(*) FILTER (WHERE exclude_reason = 'manual')          AS manual,
                COUNT(*)                                      AS total
            FROM tickets
            WHERE user_id = :uid
        """),
        {"uid": str(user.id)}
    )
    row = res.mappings().first() or {}
    return {k: int(v or 0) for k, v in row.items()}
