import asyncio
import json
import logging
from datetime import datetime, timezone

from sqlalchemy import select, delete
from sqlalchemy.dialects.postgresql import insert as pg_insert

from database import AsyncSessionLocal
from models import Run, Ticket, Message, Evaluation
from services.freshdesk import (
    fetch_all_tickets, fetch_conversations,
    build_thread, detect_churn, frt_minutes,
)
from services.claude import eval_ticket

logger = logging.getLogger("simployer.evaluator")


def _dt(val) -> datetime | None:
    """
    Robustly parse any datetime value to a timezone-aware datetime object.
    asyncpg requires datetime objects — never ISO strings.
    """
    if val is None:
        return None
    if isinstance(val, datetime):
        # Ensure it is timezone-aware
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
    try:
        s = str(val).strip()
        if not s:
            return None
        # Replace trailing Z with +00:00 for fromisoformat compatibility
        s = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


async def _keep_alive(app_url: str, stop_event: asyncio.Event):
    """Ping own health endpoint every 10min to prevent Render free tier sleep."""
    import httpx
    while not stop_event.is_set():
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                await c.get(f"{app_url}/health")
                logger.debug("Keep-alive ping sent")
        except Exception:
            pass
        # Wait 10 minutes or until stop signal
        try:
            await asyncio.wait_for(asyncio.shield(asyncio.ensure_future(
                asyncio.sleep(600)
            )), timeout=600)
        except Exception:
            pass


async def evaluate_run(ctx: dict, run_id: str):
    """
    Main evaluation job — works as both an arq job and a FastAPI BackgroundTask.
    ctx["redis"] is used for pub/sub progress (can be a no-op FakeRedis).
    """
    redis = ctx["redis"]

    async with AsyncSessionLocal() as db:
        run = await db.get(Run, run_id)
        if not run:
            logger.error(f"Run {run_id} not found")
            return

        user_id = run.user_id
        run.status = "running"
        run.started_at = datetime.now(timezone.utc)
        await db.commit()

        # Keep-alive: prevent Render free tier from sleeping mid-run
        from config import settings as _cfg
        _stop = asyncio.Event()
        _app_url = getattr(_cfg, 'app_url', 'https://simployer-qa.onrender.com')
        _ka_task = asyncio.create_task(_keep_alive(_app_url, _stop))

        async def push(done: int, total: int, churn: int):
            try:
                await redis.publish(f"run:{run_id}", json.dumps({
                    "run_id": run_id,
                    "done": done,
                    "total": total,
                    "churn": churn,
                    "pct": round(done / total * 100) if total else 0,
                    "status": "running",
                }))
            except Exception:
                pass  # Non-fatal — progress is also readable via DB poll

        try:
            # ── Step 1: Fetch ticket metadata ──────────────────────────────
            logger.info(f"[run={run_id}] Fetching tickets (last {run.days_back} days)...")
            tickets = await fetch_all_tickets(run.days_back)
            run.tickets_total = len(tickets)
            await db.commit()
            logger.info(f"[run={run_id}] {len(tickets)} tickets to evaluate")

            # ── Step 2: Fetch conversations + evaluate each ticket ──────────
            for i, ticket in enumerate(tickets):
                ticket_id = str(ticket["id"])
                try:
                    convs = await fetch_conversations(ticket_id)
                    thread = build_thread(ticket, convs)
                    churn_kw = detect_churn(thread)

                    # Parse all datetimes ONCE per ticket — never pass strings to asyncpg
                    created_at  = _dt(ticket.get("created_at"))
                    updated_at  = _dt(ticket.get("updated_at"))
                    resolved_at = _dt((ticket.get("stats") or {}).get("resolved_at"))

                    # Upsert ticket — all datetime columns are proper objects
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
                            "updated_at": updated_at,   # datetime object, not string
                        }
                    )
                    await db.execute(stmt)

                    # Delete old messages and re-insert fresh ones
                    await db.execute(
                        delete(Message).where(
                            Message.ticket_id == ticket_id,
                            Message.user_id == user_id,
                        )
                    )
                    for msg in thread:
                        db.add(Message(
                            ticket_id=ticket_id,
                            user_id=user_id,
                            role=msg["role"],
                            ts=_dt(msg["ts"]),  # datetime object
                            body=msg["body"],
                        ))
                    await db.commit()

                    # ── Claude evaluation ──────────────────────────────────
                    await asyncio.sleep(0.3)
                    ev = await eval_ticket(ticket, thread)

                    # Upsert evaluation
                    await db.execute(
                        delete(Evaluation).where(
                            Evaluation.ticket_id == ticket_id,
                            Evaluation.user_id == user_id,
                            Evaluation.run_id == run_id,
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

                    logger.info(
                        f"[run={run_id}] [{i+1}/{len(tickets)}] "
                        f"#{ticket_id} score={ev.get('total_score')} "
                        f"churn={ev.get('churn_risk_flag')}"
                    )
                    await push(i + 1, len(tickets), run.churn_count)

                except Exception as e:
                    logger.error(f"[run={run_id}] FAILED #{ticket_id}: {e}")
                    run.tickets_done = i + 1
                    await db.commit()
                    continue

                await asyncio.sleep(2)  # Rate limit breathing room

            # ── Done ────────────────────────────────────────────────────────
            _stop.set()
            run.status = "done"
            run.finished_at = datetime.now(timezone.utc)
            await db.commit()

            await redis.publish(f"run:{run_id}", json.dumps({
                "run_id": run_id, "status": "done",
                "done": run.tickets_done, "total": run.tickets_total,
                "churn": run.churn_count, "pct": 100,
            }))
            logger.info(
                f"[run={run_id}] DONE — {run.tickets_done}/{run.tickets_total} tickets, "
                f"{run.churn_count} churn flags"
            )

        except Exception as e:
            _stop.set()
            run.status = "failed"
            run.error = str(e)
            run.finished_at = datetime.now(timezone.utc)
            await db.commit()
            try:
                await redis.publish(f"run:{run_id}", json.dumps({
                    "run_id": run_id, "status": "failed", "error": str(e)
                }))
            except Exception:
                pass
            logger.error(f"[run={run_id}] FATAL: {e}")
            raise
