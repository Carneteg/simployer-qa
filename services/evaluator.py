"""
Evaluator — runs a full QA analysis on Freshdesk tickets.

Concurrency model:
  - asyncio.Semaphore limits concurrent Claude calls to CLAUDE_CONCURRENCY (8)
  - asyncio.gather runs BATCH_SIZE (20) tickets at a time
  - Each batch waits for all tickets to complete before the next batch starts
  - On Starter plan (0.5 CPU): 8× concurrency gives ~8× speedup
  - 890 tickets × ~5s avg / 8 concurrency ≈ ~9-10 min per run

Performance instrumentation:
  - Per-ticket wall time logged every 10 tickets with ETA
  - RSS memory sampled every 10 tickets, warns at >460 MB (512 MB limit)
  - Throughput telemetry written to run.error on completion

Reliability:
  - Stale run watchdog in main.py marks interrupted runs failed on startup
  - Per-ticket exceptions caught and skipped (run survives individual failures)
  - No keep-alive ping needed — Starter plan instances do not spin down
"""
import asyncio
import json
import logging
import resource
import time
from datetime import datetime, timezone

from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert as pg_insert

from database import AsyncSessionLocal
from models import Run, Ticket, Message, Evaluation
from services.freshdesk import (
    fetch_all_tickets, fetch_conversations,
    build_thread, detect_churn, is_confirmed_churn, fetch_company,
)
from services.claude import eval_ticket

logger = logging.getLogger("simployer.evaluator")

# ── Concurrency config (Starter plan: 0.5 CPU) ───────────────────────────────
CLAUDE_CONCURRENCY = 1    # 1 Claude call at a time — avoid all rate limit contention
BATCH_SIZE         = 20   # Tickets processed per asyncio.gather batch


# ── Helpers ───────────────────────────────────────────────────────────────────

def _dt(val) -> datetime | None:
    """Parse any datetime value → timezone-aware datetime. asyncpg requires objects."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
    try:
        s = str(val).strip()
        if not s:
            return None
        s = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _rss_mb() -> float:
    """Current process RSS memory in MB."""
    try:
        import platform
        kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return kb / 1024 if platform.system() == "Linux" else kb / (1024 * 1024)
    except Exception:
        return 0.0



# ── Exclusion rules ───────────────────────────────────────────────────────────
# Auto-exclusion is deterministic — no AI call needed. Applied before eval.
# Rules applied in order; first match wins.

# Tags that always mean "exclude from QA"
AUTO_EXCLUDE_TAGS = {
    "call_finished", "talkdesk", "spam", "talkdesk-call",
    "voicemail", "missed-call", "auto-reply", "noreply",
    "no-reply", "system", "autoresponder", "out-of-office",
}

# Subject prefixes that indicate system/automated tickets
AUTO_EXCLUDE_SUBJECT_PREFIXES = (
    "auto:", "automatic reply:", "out of office:", "autosvar:",
    "automatiskt svar:", "abwesenheitsnotiz:", "delivery failure",
    "undeliverable:", "mailer-daemon", "[spam]",
)

# Source codes Freshdesk uses for phone/call tickets (source=3 = phone)
AUTO_EXCLUDE_SOURCES = {3}  # 3 = Phone in Freshdesk


def _should_exclude(ticket: dict) -> tuple[bool, str]:
    """
    Returns (should_exclude: bool, reason_code: str).
    reason_code is empty string when not excluded.
    """
    tags = {t.lower().strip() for t in (ticket.get("tags") or [])}
    subject = (ticket.get("subject") or "").lower().strip()
    source = ticket.get("source")

    # 1. Tag-based exclusion
    hit = tags & AUTO_EXCLUDE_TAGS
    if hit:
        tag = sorted(hit)[0]
        return True, f"auto_tag:{tag}"

    # 2. Source-based exclusion (phone/Talkdesk calls)
    if source in AUTO_EXCLUDE_SOURCES:
        return True, "auto_phone_source"

    # 3. Subject prefix exclusion
    for prefix in AUTO_EXCLUDE_SUBJECT_PREFIXES:
        if subject.startswith(prefix):
            return True, "auto_subject"

    # 4. No description AND no conversations at all (pure system record)
    # This is checked after build_thread — see _process_ticket usage
    return False, ""


# ── Per-ticket worker ─────────────────────────────────────────────────────────

def _parse_csat(sr) -> "int | None":
    """Thin wrapper around _parse_csat_ratings for legacy call sites."""
    if not sr:
        return None
    from services.freshdesk import _parse_csat_ratings
    ratings = sr.get("ratings") or sr
    return _parse_csat_ratings(ratings)


async def _process_ticket(
    ticket:      dict,
    user_id:     object,
    run_id:      str,
    sem:         asyncio.Semaphore,
    counters:    dict,
    total:       int,
    run_id_uuid: object,
) -> None:
    """
    Fetch conversations, upsert ticket + messages, call Claude, upsert evaluation.
    Uses the semaphore to cap concurrent Claude calls at CLAUDE_CONCURRENCY.
    All DB work happens inside a fresh session per ticket.
    """
    ticket_id = str(ticket["id"])

    async with sem:
        t0 = time.monotonic()
        try:
            # ── Auto-exclusion check (deterministic — no Claude call) ──────────
            excluded, exclude_reason = _should_exclude(ticket)

            convs  = await fetch_conversations(ticket_id)
            thread = build_thread(ticket, convs)

            # Also exclude if ticket has no messages at all (pure system record)
            if not excluded and len(thread) == 0:
                excluded = True
                exclude_reason = "auto_no_messages"
            churn_kw        = detect_churn(thread)
            churn_confirmed = is_confirmed_churn(ticket)

            # Fetch company ARR (cached per company_id — cheap after first call)
            raw_company_id = ticket.get("company_id")
            company_data   = await fetch_company(raw_company_id) if raw_company_id else {}

            # CX proxy — computed from thread length before Claude call
            _msg_count = len(thread)

            created_at  = _dt(ticket.get("created_at"))
            updated_at  = _dt(ticket.get("updated_at"))
            resolved_at = _dt((ticket.get("stats") or {}).get("resolved_at"))

            async with AsyncSessionLocal() as db:
                stmt = pg_insert(Ticket).values(
                    id=ticket_id,
                    user_id=user_id,
                    subject=ticket.get("subject"),
                    agent_name=(
                        ticket.get("_agent_name") or
                        (ticket.get("responder") or {}).get("name")
                    ),
                    group_name=(
                        ticket.get("_group_name") or
                        (ticket.get("group") or {}).get("name")
                    ),
                    status=ticket.get("status"),
                    priority=ticket.get("priority"),
                    csat=None,  # populated by /runs/backfill-csat after run completes
                    tags=ticket.get("tags") or [],
                    fr_escalated=bool(ticket.get("fr_escalated")),
                    nr_escalated=bool(ticket.get("nr_escalated")),
                    excluded=excluded,
                    exclude_reason=exclude_reason if excluded else None,
                    created_at=created_at,
                    resolved_at=resolved_at,
                    updated_at=updated_at,
                    # Company / ARR fields
                    company_id=company_data.get("company_id") or (str(raw_company_id) if raw_company_id else None),
                    company_name=company_data.get("company_name"),
                    arr=company_data.get("arr"),
                    planhat_phase=company_data.get("planhat_phase"),
                    planhat_health=company_data.get("planhat_health"),
                    planhat_segmentation=company_data.get("planhat_segmentation"),
                ).on_conflict_do_update(
                    index_elements=["id", "user_id"],
                    set_={
                        "subject":    ticket.get("subject"),
                        "agent_name": (
                            ticket.get("_agent_name") or
                            (ticket.get("responder") or {}).get("name")
                        ),
                        "updated_at": updated_at,
                        # Always refresh company data in case ARR changed
                        "company_id":   company_data.get("company_id") or (str(raw_company_id) if raw_company_id else None),
                        "company_name": company_data.get("company_name"),
                        "arr":          company_data.get("arr"),
                        "planhat_phase":         company_data.get("planhat_phase"),
                        "planhat_health":        company_data.get("planhat_health"),
                        "planhat_segmentation":  company_data.get("planhat_segmentation"),
                        # Only update exclusion if not manually excluded
                        # (manual exclusions survive re-runs)
                    }
                )
                await db.execute(stmt)

                await db.execute(
                    delete(Message).where(
                        Message.ticket_id == ticket_id,
                        Message.user_id   == user_id,
                    )
                )
                for msg in thread:
                    db.add(Message(
                        ticket_id=ticket_id,
                        user_id=user_id,
                        role=msg["role"],
                        ts=_dt(msg["ts"]),
                        body=msg["body"],
                    ))
                await db.commit()

            # Skip AI evaluation for excluded tickets — just save the ticket record
            if excluded:
                counters["done"] += 1
                counters["excluded"] = counters.get("excluded", 0) + 1
                logger.debug(f"Excluded ticket #{ticket_id}: {exclude_reason}")
                return

            await asyncio.sleep(3)   # pace to ~20 req/min — stays within Haiku TPM
            ev = await eval_ticket(ticket, thread)

            async with AsyncSessionLocal() as db:
                await db.execute(
                    delete(Evaluation).where(
                        Evaluation.ticket_id == ticket_id,
                        Evaluation.user_id   == user_id,
                        Evaluation.run_id    == run_id_uuid,
                    )
                )
                # ── CX proxy computation ─────────────────────────────────────────
                # Requires both thread (msg_count) and eval result (churn/contact flags)
                # computed after Claude returns so all three signals are available.
                _churn_flag = bool(
                    churn_confirmed or
                    ev.get("churn_risk_flag") or
                    (churn_kw and (ev.get("total_score") or 100) < 70)
                )
                _contact_flag = bool(ev.get("contact_problem_flag"))
                _high_msgs    = _msg_count > 2

                _cx_signals = []
                if _contact_flag:            _cx_signals.append("contact_problem")
                if _high_msgs:               _cx_signals.append("high_msgs")
                if _churn_flag:              _cx_signals.append("churn_flag")

                _cx_bad = bool(_cx_signals)   # bad CX if any signal fired

                db.add(Evaluation(
                    ticket_id=ticket_id,
                    user_id=user_id,
                    run_id=run_id_uuid,
                    total_score=ev.get("total_score"),
                    complexity=ev.get("complexity"),
                    sentiment_start=(ev.get("sentiment") or {}).get("start"),
                    sentiment_end=(ev.get("sentiment") or {}).get("end"),
                    summary=ev.get("summary"),
                    # Churn detection — three tiers in priority order:
                    #
                    # TIER 1 — Confirmed (salesforce tag):
                    #   Customer has actually terminated their contract in Salesforce.
                    #   This is ground truth. Overrides all other signals. No score gate.
                    #
                    # TIER 2 — Claude explicit inference:
                    #   The AI identified clear cancellation intent in the conversation.
                    #
                    # TIER 3 — Keyword match + low score (<70):
                    #   Phrase-level keyword found AND agent quality was poor.
                    #   High-quality handling (score ≥70) suppresses keyword-only flags.
                    churn_risk_flag=bool(
                        churn_confirmed or
                        ev.get("churn_risk_flag") or
                        (churn_kw and (ev.get("total_score") or 100) < 70)
                    ),
                    churn_risk_reason=(
                        "Confirmed: Customer has terminated contract (Salesforce)"
                        if churn_confirmed else
                        ev.get("churn_risk_reason") or
                        (f'Signal: "{churn_kw}"' if churn_kw else None)
                    ),
                    contact_problem_flag=_contact_flag,
                    coaching_tip=ev.get("coaching_tip"),
                    strengths=ev.get("strengths"),
                    improvements=ev.get("improvements"),
                    scores=ev.get("scores"),
                    # CX proxy fields
                    msg_count=_msg_count,
                    cx_bad=_cx_bad,
                    cx_signals=_cx_signals if _cx_signals else None,
                    churn_confirmed=churn_confirmed,
                ))
                await db.commit()

            elapsed = time.monotonic() - t0
            counters["done"]    += 1
            counters["timings"].append(elapsed)
            # Mirror the three-tier churn logic from the DB write above
            _churn_flagged = bool(
                churn_confirmed or
                ev.get("churn_risk_flag") or
                (churn_kw and (ev.get("total_score") or 100) < 70)
            )
            if _churn_flagged:
                counters["churn"] += 1

            logger.debug(
                f"[{counters['done']}/{total}] #{ticket_id} "
                f"score={ev.get('total_score')} churn={bool(ev.get('churn_risk_flag'))} "
                f"{elapsed:.1f}s"
            )

        except Exception as e:
            elapsed = time.monotonic() - t0
            counters["errors"] += 1
            counters["done"]   += 1
            logger.error(
                f"FAILED #{ticket_id} after {elapsed:.1f}s: {type(e).__name__}: {e}"
            )


# ── Main job ──────────────────────────────────────────────────────────────────

async def evaluate_run(ctx: dict, run_id: str) -> None:
    """
    Main evaluation job. Compatible with arq and FastAPI BackgroundTasks.
    ctx["redis"] must have .publish(channel, message) — RealRedis or FakeRedis.
    """
    redis = ctx["redis"]

    async with AsyncSessionLocal() as db:
        run = await db.get(Run, run_id)
        if not run:
            logger.error(f"Run {run_id} not found")
            return

        user_id      = run.user_id
        run_id_uuid  = run.id
        run.status   = "running"
        run.started_at = datetime.now(timezone.utc)
        await db.commit()

    # ── Throughput / memory tracking ──────────────────────────────────────────
    run_start = time.monotonic()
    mem_start = _rss_mb()
    mem_peak  = mem_start
    logger.info(
        f"[run={run_id}] Starting — "
        f"concurrency={CLAUDE_CONCURRENCY} batch={BATCH_SIZE} | RSS {mem_start:.0f} MB"
    )

    async def _flush_progress(done: int, total: int, churn: int) -> None:
        try:
            async with AsyncSessionLocal() as db:
                run_obj = await db.get(Run, run_id_uuid)
                if run_obj:
                    run_obj.tickets_done = done
                    run_obj.churn_count  = churn
                    await db.commit()
        except Exception:
            pass
        try:
            await redis.publish(f"run:{run_id}", json.dumps({
                "run_id": run_id, "done": done, "total": total,
                "churn": churn,
                "pct":   round(done / total * 100) if total else 0,
                "status": "running",
            }))
        except Exception:
            pass

    try:
        # ── Step 1: Fetch all tickets ─────────────────────────────────────────
        fetch_t0 = time.monotonic()
        logger.info(f"[run={run_id}] Fetching tickets (last {run.days_back} days)...")
        tickets = await fetch_all_tickets(run.days_back)
        fetch_s = round(time.monotonic() - fetch_t0, 1)

        async with AsyncSessionLocal() as db:
            run_obj = await db.get(Run, run_id_uuid)
            run_obj.tickets_total = len(tickets)
            await db.commit()

        logger.info(
            f"[run={run_id}] {len(tickets)} tickets fetched in {fetch_s}s | "
            f"concurrency={CLAUDE_CONCURRENCY} | batch={BATCH_SIZE}"
        )

        # ── Step 2: Concurrent processing ────────────────────────────────────
        sem      = asyncio.Semaphore(CLAUDE_CONCURRENCY)
        counters = {"done": 0, "churn": 0, "errors": 0, "timings": []}

        for batch_start in range(0, len(tickets), BATCH_SIZE):
            batch = tickets[batch_start: batch_start + BATCH_SIZE]

            await asyncio.gather(*[
                _process_ticket(
                    ticket=t,
                    user_id=user_id,
                    run_id=run_id,
                    sem=sem,
                    counters=counters,
                    total=len(tickets),
                    run_id_uuid=run_id_uuid,
                )
                for t in batch
            ])

            await _flush_progress(counters["done"], len(tickets), counters["churn"])

            if counters["timings"]:
                recent    = counters["timings"][-20:]
                avg_s     = sum(recent) / len(recent)
                tpm       = round(60 / avg_s, 1) if avg_s > 0 else 0
                remaining = len(tickets) - counters["done"]
                eta_min   = round((remaining / CLAUDE_CONCURRENCY) * avg_s / 60, 1)

                mem_now  = _rss_mb()
                mem_peak = max(mem_peak, mem_now)

                logger.info(
                    f"[run={run_id}] {counters['done']}/{len(tickets)} | "
                    f"avg {avg_s:.1f}s/ticket | {tpm} t/min "
                    f"(×{CLAUDE_CONCURRENCY} concurrency) | "
                    f"ETA {eta_min}min | "
                    f"RSS {mem_now:.0f}MB (+{mem_now - mem_start:.0f}MB) | "
                    f"errors={counters['errors']}"
                )

                if mem_now > 460:   # warn at 460 MB (512 MB limit on Starter)
                    logger.warning(
                        f"[run={run_id}] ⚠️  RSS {mem_now:.0f}MB — "
                        f"approaching 512MB RAM limit"
                    )

        # ── Done ─────────────────────────────────────────────────────────────
        total_s       = time.monotonic() - run_start
        avg_s         = (
            sum(counters["timings"]) / len(counters["timings"])
            if counters["timings"] else 0
        )
        effective_tpm = round(counters["done"] / (total_s / 60), 1) if total_s > 0 else 0
        mem_final     = _rss_mb()

        telemetry = {
            "total_secs":          round(total_s),
            "avg_secs_per_ticket": round(avg_s, 2),
            "effective_tpm":       effective_tpm,
            "concurrency":         CLAUDE_CONCURRENCY,
            "tickets_processed":   counters["done"],
            "errors":              counters["errors"],
            "mem_start_mb":        round(mem_start, 1),
            "mem_peak_mb":         round(mem_peak, 1),
            "mem_final_mb":        round(mem_final, 1),
            "mem_growth_mb":       round(mem_final - mem_start, 1),
        }

        async with AsyncSessionLocal() as db:
            run_obj = await db.get(Run, run_id_uuid)
            run_obj.status       = "done"
            run_obj.tickets_done = counters["done"]
            run_obj.churn_count  = counters["churn"]
            run_obj.finished_at  = datetime.now(timezone.utc)
            run_obj.error        = json.dumps({"telemetry": telemetry})
            await db.commit()

        await redis.publish(f"run:{run_id}", json.dumps({
            "run_id": run_id, "status": "done",
            "done":  counters["done"], "total": len(tickets),
            "churn": counters["churn"], "pct": 100,
        }))

        # Auto-backfill CSAT after run completes (avoids per-ticket HTTP calls during run)
        try:
            from services.freshdesk import fetch_all_csat_ratings
            csat_map, _ = await fetch_all_csat_ratings(days_back=30)
            if csat_map:
                async with AsyncSessionLocal() as csat_db:
                    result = await csat_db.execute(
                        __import__('sqlalchemy').select(
                            __import__('models', fromlist=['Ticket']).Ticket
                        ).where(
                            __import__('models', fromlist=['Ticket']).Ticket.user_id == user_id
                        )
                    )
                    db_tickets = result.scalars().all()
                    csat_updated = 0
                    for t in db_tickets:
                        if str(t.id) in csat_map:
                            t.csat = csat_map[str(t.id)]
                            csat_updated += 1
                    await csat_db.commit()
                    logger.info(f"[run={run_id}] CSAT backfill: {csat_updated} tickets updated")
        except Exception as csat_err:
            logger.warning(f"[run={run_id}] CSAT backfill failed (non-fatal): {csat_err}")

        logger.info(
            f"[run={run_id}] ✅ DONE — "
            f"{counters['done']}/{len(tickets)} tickets | "
            f"{round(total_s/60, 1)}min | "
            f"{effective_tpm} t/min effective | "
            f"errors={counters['errors']} | "
            f"RSS {mem_final:.0f}MB (peak {mem_peak:.0f}MB, "
            f"+{mem_final - mem_start:.0f}MB growth)"
        )

    except Exception as e:
        async with AsyncSessionLocal() as db:
            run_obj = await db.get(Run, run_id_uuid)
            if run_obj:
                run_obj.status      = "failed"
                run_obj.error       = str(e)
                run_obj.finished_at = datetime.now(timezone.utc)
                await db.commit()
        try:
            await redis.publish(f"run:{run_id}", json.dumps({
                "run_id": run_id, "status": "failed", "error": str(e)
            }))
        except Exception:
            pass
        logger.error(f"[run={run_id}] ❌ FATAL: {type(e).__name__}: {e}")
        raise
