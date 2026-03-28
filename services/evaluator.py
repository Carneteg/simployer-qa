"""
Evaluator — runs a full QA analysis on Freshdesk tickets.

Performance instrumentation:
  - Per-ticket wall time + ETA logged every 10 tickets
  - RSS memory sampled every 10 tickets, warns at >400 MB
  - Throughput (tickets/min) written to run.error field as JSON telemetry when done

Reliability:
  - Stale run watchdog: see main.py lifespan — marks interrupted 'running' runs as 'failed'
  - Per-ticket exception caught and skipped (run continues even if one ticket fails)
  - Keep-alive HTTP ping every 10 min to prevent Render free tier sleep
"""
import asyncio
import json
import logging
import resource
import time
from datetime import datetime, timezone

from sqlalchemy import select, delete
from sqlalchemy.dialects.postgresql import insert as pg_insert

from database import AsyncSessionLocal
from models import Run, Ticket, Message, Evaluation
from services.freshdesk import (
    fetch_all_tickets, fetch_conversations,
    build_thread, detect_churn,
)
from services.claude import eval_ticket

logger = logging.getLogger("simployer.evaluator")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _dt(val) -> datetime | None:
    """Parse any datetime value to a timezone-aware datetime object.
    asyncpg requires datetime objects — never ISO strings."""
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
    """Current process RSS memory in MB (Linux/macOS)."""
    try:
        kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # Linux returns KB, macOS returns bytes
        import platform
        return kb / 1024 if platform.system() == "Linux" else kb / (1024 * 1024)
    except Exception:
        return 0.0


async def _keep_alive(app_url: str, stop_event: asyncio.Event) -> None:
    """Ping own health endpoint every 10 min to prevent Render free tier sleep."""
    import httpx
    while not stop_event.is_set():
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                await c.get(f"{app_url}/health")
            logger.debug("Keep-alive ping OK")
        except Exception:
            pass
        # Sleep 10 min, interruptible by stop signal
        try:
            await asyncio.wait_for(
                asyncio.shield(asyncio.sleep(600)),
                timeout=600,
            )
        except Exception:
            pass


# ── Main job ──────────────────────────────────────────────────────────────────

async def evaluate_run(ctx: dict, run_id: str) -> None:
    """
    Main evaluation job — compatible with both arq and FastAPI BackgroundTasks.
    ctx["redis"] must have a .publish(channel, message) coroutine (FakeRedis is fine).
    """
    redis = ctx["redis"]

    async with AsyncSessionLocal() as db:
        run = await db.get(Run, run_id)
        if not run:
            logger.error(f"Run {run_id} not found")
            return

        user_id     = run.user_id
        run.status  = "running"
        run.started_at = datetime.now(timezone.utc)
        await db.commit()

        # ── Keep-alive task ───────────────────────────────────────────────
        from config import settings as _cfg
        _stop      = asyncio.Event()
        _app_url   = getattr(_cfg, "app_url", "https://simployer-qa.onrender.com")
        _ka_task   = asyncio.create_task(_keep_alive(_app_url, _stop))

        # ── Throughput tracking ───────────────────────────────────────────
        run_start_ts   = time.monotonic()
        ticket_timings: list[float] = []   # wall time per ticket (seconds)
        mem_start_mb   = _rss_mb()
        mem_peak_mb    = mem_start_mb
        logger.info(f"[run={run_id}] Starting — RSS {mem_start_mb:.1f} MB")

        async def push(done: int, total: int, churn: int) -> None:
            try:
                await redis.publish(f"run:{run_id}", json.dumps({
                    "run_id": run_id,
                    "done":   done,
                    "total":  total,
                    "churn":  churn,
                    "pct":    round(done / total * 100) if total else 0,
                    "status": "running",
                }))
            except Exception:
                pass

        try:
            # ── Step 1: Fetch tickets ─────────────────────────────────────
            logger.info(f"[run={run_id}] Fetching tickets (last {run.days_back} days)...")
            fetch_start = time.monotonic()
            tickets = await fetch_all_tickets(run.days_back)
            fetch_secs = round(time.monotonic() - fetch_start, 1)

            run.tickets_total = len(tickets)
            await db.commit()
            logger.info(
                f"[run={run_id}] {len(tickets)} tickets fetched in {fetch_secs}s"
            )

            # ── Step 2: Per-ticket evaluation loop ────────────────────────
            for i, ticket in enumerate(tickets):
                ticket_id  = str(ticket["id"])
                t_start    = time.monotonic()

                try:
                    convs     = await fetch_conversations(ticket_id)
                    thread    = build_thread(ticket, convs)
                    churn_kw  = detect_churn(thread)

                    # Parse datetimes once — asyncpg rejects ISO strings
                    created_at  = _dt(ticket.get("created_at"))
                    updated_at  = _dt(ticket.get("updated_at"))
                    resolved_at = _dt((ticket.get("stats") or {}).get("resolved_at"))

                    # Upsert ticket
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
                        csat=(ticket.get("satisfaction_rating") or {}).get("rating"),
                        tags=ticket.get("tags") or [],
                        fr_escalated=bool(ticket.get("fr_escalated")),
                        nr_escalated=bool(ticket.get("nr_escalated")),
                        created_at=created_at,
                        resolved_at=resolved_at,
                        updated_at=updated_at,
                    ).on_conflict_do_update(
                        index_elements=["id", "user_id"],
                        set_={
                            "subject":    ticket.get("subject"),
                            "agent_name": (
                                ticket.get("_agent_name") or
                                (ticket.get("responder") or {}).get("name")
                            ),
                            "updated_at": updated_at,
                        }
                    )
                    await db.execute(stmt)

                    # Replace messages
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

                    # Claude evaluation
                    await asyncio.sleep(0.3)
                    ev = await eval_ticket(ticket, thread)

                    # Upsert evaluation
                    await db.execute(
                        delete(Evaluation).where(
                            Evaluation.ticket_id == ticket_id,
                            Evaluation.user_id   == user_id,
                            Evaluation.run_id    == run_id,
                        )
                    )
                    db.add(Evaluation(
                        ticket_id=ticket_id,
                        user_id=user_id,
                        run_id=run_id,
                        total_score=ev.get("total_score"),
                        complexity=ev.get("complexity"),
                        sentiment_start=(ev.get("sentiment") or {}).get("start"),
                        sentiment_end=(ev.get("sentiment") or {}).get("end"),
                        summary=ev.get("summary"),
                        churn_risk_flag=bool(ev.get("churn_risk_flag") or churn_kw),
                        churn_risk_reason=ev.get("churn_risk_reason") or (
                            f'Signal: "{churn_kw}"' if churn_kw else None
                        ),
                        contact_problem_flag=bool(ev.get("contact_problem_flag")),
                        coaching_tip=ev.get("coaching_tip"),
                        strengths=ev.get("strengths"),
                        improvements=ev.get("improvements"),
                        scores=ev.get("scores"),
                    ))

                    run.tickets_done = i + 1
                    if ev.get("churn_risk_flag") or churn_kw:
                        run.churn_count += 1
                    await db.commit()

                    # ── Per-ticket timing ─────────────────────────────────
                    t_elapsed = time.monotonic() - t_start
                    ticket_timings.append(t_elapsed)

                    # Log throughput + ETA every 10 tickets
                    if (i + 1) % 10 == 0 or i == 0:
                        recent   = ticket_timings[-10:]
                        avg_secs = sum(recent) / len(recent)
                        remaining = len(tickets) - (i + 1)
                        eta_min  = round(remaining * avg_secs / 60, 1)
                        tpm      = round(60 / avg_secs, 1) if avg_secs > 0 else 0

                        # Memory check
                        mem_now  = _rss_mb()
                        mem_peak_mb = max(mem_peak_mb, mem_now)
                        mem_delta = mem_now - mem_start_mb

                        logger.info(
                            f"[run={run_id}] [{i+1}/{len(tickets)}] "
                            f"avg {avg_secs:.1f}s/ticket | {tpm} t/min | "
                            f"ETA {eta_min}min | RSS {mem_now:.0f}MB "
                            f"(+{mem_delta:.0f}MB from start)"
                        )

                        # Memory warning
                        if mem_now > 400:
                            logger.warning(
                                f"[run={run_id}] ⚠️  RSS {mem_now:.0f}MB — "
                                f"approaching 512MB free tier limit"
                            )

                    await push(i + 1, len(tickets), run.churn_count)

                except Exception as e:
                    logger.error(f"[run={run_id}] FAILED #{ticket_id}: {e}")
                    run.tickets_done = i + 1
                    await db.commit()
                    continue

                await asyncio.sleep(2)  # Rate-limit breathing room

            # ── Done ─────────────────────────────────────────────────────
            _stop.set()
            total_secs  = time.monotonic() - run_start_ts
            avg_secs    = sum(ticket_timings) / len(ticket_timings) if ticket_timings else 0
            tpm_overall = round(len(ticket_timings) / (total_secs / 60), 1) if total_secs > 0 else 0
            mem_final   = _rss_mb()

            telemetry = {
                "total_secs":    round(total_secs),
                "avg_secs_per_ticket": round(avg_secs, 2),
                "tickets_per_min":     tpm_overall,
                "mem_start_mb":  round(mem_start_mb, 1),
                "mem_peak_mb":   round(mem_peak_mb, 1),
                "mem_final_mb":  round(mem_final, 1),
                "mem_growth_mb": round(mem_final - mem_start_mb, 1),
            }

            run.status      = "done"
            run.finished_at = datetime.now(timezone.utc)
            # Store telemetry in the error field (repurposed as metadata when done)
            run.error       = json.dumps({"telemetry": telemetry})
            await db.commit()

            await redis.publish(f"run:{run_id}", json.dumps({
                "run_id": run_id, "status": "done",
                "done": run.tickets_done, "total": run.tickets_total,
                "churn": run.churn_count, "pct": 100,
            }))
            logger.info(
                f"[run={run_id}] ✅ DONE — "
                f"{run.tickets_done}/{run.tickets_total} tickets | "
                f"{round(total_secs/60, 1)}min total | "
                f"{tpm_overall} t/min | "
                f"RSS {mem_final:.0f}MB (peak {mem_peak_mb:.0f}MB, "
                f"+{mem_final - mem_start_mb:.0f}MB growth)"
            )

        except Exception as e:
            _stop.set()
            run.status      = "failed"
            run.error       = str(e)
            run.finished_at = datetime.now(timezone.utc)
            await db.commit()
            try:
                await redis.publish(f"run:{run_id}", json.dumps({
                    "run_id": run_id, "status": "failed", "error": str(e)
                }))
            except Exception:
                pass
            logger.error(f"[run={run_id}] ❌ FATAL: {e}")
            raise
