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


async def evaluate_run(ctx: dict, run_id: str):
    """
    Main arq background job.
    ctx["redis"] is the arq Redis connection — used for pub/sub progress.
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

        async def push(done: int, total: int, churn: int):
            await redis.publish(f"run:{run_id}", json.dumps({
                "run_id": run_id,
                "done": done,
                "total": total,
                "churn": churn,
                "pct": round(done / total * 100) if total else 0,
                "status": "running",
            }))

        try:
            # ── Step 1: Fetch ticket metadata ────────────────────────
            logger.info(f"[run={run_id}] Fetching tickets (last {run.days_back} days)...")
            tickets = await fetch_all_tickets(run.days_back)
            run.tickets_total = len(tickets)
            await db.commit()
            logger.info(f"[run={run_id}] {len(tickets)} tickets fetched")

            # ── Step 2: Fetch conversations + eval each ticket ────────
            for i, ticket in enumerate(tickets):
                ticket_id = str(ticket["id"])
                try:
                    # Fetch conversations
                    convs = await fetch_conversations(ticket_id)
                    thread = build_thread(ticket, convs)
                    churn_kw = detect_churn(thread)
                    frt = frt_minutes(ticket)

                    # Upsert ticket
                    def _parse_dt(val):
                        """Parse ISO datetime string to datetime object."""
                        if not val:
                            return None
                        if hasattr(val, 'year'):
                            return val
                        from datetime import datetime, timezone
                        try:
                            s = str(val).replace("Z", "+00:00")
                            return datetime.fromisoformat(s)
                        except Exception:
                            return None

                    stmt = pg_insert(Ticket).values(
                        id=ticket_id,
                        user_id=user_id,
                        subject=ticket.get("subject"),
                        agent_name=(ticket.get("responder") or {}).get("name"),
                        group_name=(ticket.get("group") or {}).get("name"),
                        status=ticket.get("status"),
                        priority=ticket.get("priority"),
                        csat=(ticket.get("satisfaction_rating") or {}).get("rating"),
                        tags=ticket.get("tags") or [],
                        fr_escalated=bool(ticket.get("fr_escalated")),
                        nr_escalated=bool(ticket.get("nr_escalated")),
                        created_at=_parse_dt(ticket.get("created_at")),
                        resolved_at=_parse_dt((ticket.get("stats") or {}).get("resolved_at")),
                        updated_at=_parse_dt(ticket.get("updated_at")),
                    ).on_conflict_do_update(
                        index_elements=["id", "user_id"],
                        set_={
                            "subject": ticket.get("subject"),
                            "agent_name": (ticket.get("responder") or {}).get("name"),
                            "updated_at": ticket.get("updated_at"),
                        }
                    )
                    await db.execute(stmt)

                    # Delete old messages and re-insert
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
                            ts=_parse_dt(msg["ts"]),
                            body=msg["body"],
                        ))
                    await db.commit()

                    # Claude evaluation
                    await asyncio.sleep(0.3)  # small delay before Claude call
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
                        churn_risk_flag=ev.get("churn_risk_flag", False),
                        churn_risk_reason=ev.get("churn_risk_reason") or (
                            f'Signal: "{churn_kw}"' if churn_kw else None
                        ),
                        contact_problem_flag=ev.get("contact_problem_flag", False),
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
                        f"[run={run_id}] [{i + 1}/{len(tickets)}] "
                        f"#{ticket_id} score={ev.get('total_score')} "
                        f"churn={ev.get('churn_risk_flag')}"
                    )
                    await push(i + 1, len(tickets), run.churn_count)

                except Exception as e:
                    logger.error(f"[run={run_id}] FAILED #{ticket_id}: {e}")
                    run.tickets_done = i + 1
                    await db.commit()
                    continue

                await asyncio.sleep(2)  # Claude rate limit breathing room

            run.status = "done"
            run.finished_at = datetime.now(timezone.utc)
            await db.commit()

            # Final broadcast
            await redis.publish(f"run:{run_id}", json.dumps({
                "run_id": run_id,
                "status": "done",
                "done": run.tickets_done,
                "total": run.tickets_total,
                "churn": run.churn_count,
                "pct": 100,
            }))
            logger.info(
                f"[run={run_id}] DONE — {run.tickets_done}/{run.tickets_total} tickets, "
                f"{run.churn_count} churn flags"
            )

        except Exception as e:
            run.status = "failed"
            run.error = str(e)
            run.finished_at = datetime.now(timezone.utc)
            await db.commit()
            await redis.publish(f"run:{run_id}", json.dumps({
                "run_id": run_id, "status": "failed", "error": str(e)
            }))
            logger.error(f"[run={run_id}] FATAL: {e}")
            raise
