import uuid
import asyncio
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from database import get_db, AsyncSessionLocal
from models import Run, User
from routers.auth import current_user

router = APIRouter()


class RunCreate(BaseModel):
    days_back: int = 7


class RunOut(BaseModel):
    id: uuid.UUID
    status: str
    days_back: int
    tickets_total: int
    tickets_done: int
    churn_count: int
    error: Optional[str]
    telemetry: Optional[dict]     # populated when status == done
    created_at: str
    started_at: Optional[str]
    finished_at: Optional[str]

    class Config:
        from_attributes = True

    @classmethod
    def from_orm(cls, run: Run):
        import json as _json
        # error field doubles as telemetry store when run is done
        telemetry = None
        error_msg = run.error
        if run.status == "done" and run.error:
            try:
                parsed = _json.loads(run.error)
                if "telemetry" in parsed:
                    telemetry = parsed["telemetry"]
                    error_msg = None   # don't surface telemetry as an error
            except Exception:
                pass

        return cls(
            id=run.id,
            status=run.status,
            days_back=run.days_back,
            tickets_total=run.tickets_total,
            tickets_done=run.tickets_done,
            churn_count=run.churn_count,
            error=error_msg,
            telemetry=telemetry,
            created_at=str(run.created_at or ""),
            started_at=str(run.started_at) if run.started_at else None,
            finished_at=str(run.finished_at) if run.finished_at else None,
        )


async def _run_evaluation_task(run_id: str):
    """
    FastAPI background task — runs the full evaluation in-process.
    Uses real Redis pub/sub for live progress streaming to the WebSocket.
    """
    import logging
    from services.cache import redis as _redis, publish_run_event

    logger = logging.getLogger("simployer.runs")

    class RealRedis:
        """Thin wrapper that matches the arq ctx["redis"] interface."""
        async def publish(self, channel: str, message: str):
            try:
                await _redis.publish(channel, message)
            except Exception as e:
                logger.debug(f"redis publish failed (non-fatal): {e}")

    ctx = {"redis": RealRedis()}

    try:
        from services.evaluator import evaluate_run
        await evaluate_run(ctx, run_id)

        # Invalidate Redis cache — stale agents/tickets data is now outdated
        try:
            from services.cache import invalidate as cache_invalidate
            from database import AsyncSessionLocal
            from models import Run as RunModel
            async with AsyncSessionLocal() as db:
                run = await db.get(RunModel, uuid.UUID(run_id))
                if run:
                    await cache_invalidate(str(run.user_id))
                    logger.info(f"Cache invalidated for user {run.user_id} after run {run_id}")
        except Exception as ce:
            logger.warning(f"Cache invalidation failed (non-fatal): {ce}")

    except Exception as e:
        logger.error(f"Background evaluation failed for run {run_id}: {e}")
        async with AsyncSessionLocal() as db:
            run = await db.get(Run, uuid.UUID(run_id))
            if run:
                run.status = "failed"
                run.error = str(e)
                await db.commit()


@router.post("/", status_code=202)
async def start_run(
    body: RunCreate,
    background_tasks: BackgroundTasks,
    user: User = Depends(current_user),
    db: AsyncSession = Depends(get_db),
):
    run = Run(user_id=user.id, days_back=body.days_back)
    db.add(run)
    await db.commit()
    await db.refresh(run)

    # Run evaluation as FastAPI background task (no arq worker needed)
    background_tasks.add_task(_run_evaluation_task, str(run.id))

    return {"run_id": str(run.id), "status": "queued"}


@router.get("/", response_model=List[RunOut])
async def list_runs(
    user: User = Depends(current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Run)
        .where(Run.user_id == user.id)
        .order_by(Run.created_at.desc())
        .limit(20)
    )
    return [RunOut.from_orm(r) for r in result.scalars().all()]


@router.get("/{run_id}", response_model=RunOut)
async def get_run(
    run_id: uuid.UUID,
    user: User = Depends(current_user),
    db: AsyncSession = Depends(get_db),
):
    run = await db.get(Run, run_id)
    if not run or run.user_id != user.id:
        raise HTTPException(404, "Run not found")
    return RunOut.from_orm(run)


class BackfillCsatRequest(BaseModel):
    days_back: int = 30


@router.post("/backfill-csat")
async def backfill_csat(
    req: BackfillCsatRequest = BackfillCsatRequest(),
    user: User = Depends(current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Fetch CSAT scores from Freshdesk and patch existing Ticket records.

    Does NOT re-run AI evaluation — only updates the csat column.
    Run this once after the CSAT ingestion fix to backfill historical data.
    """
    from sqlalchemy import select, update
    from models import Ticket
    from services.freshdesk import fetch_all_csat_ratings

    # Fetch CSAT ratings from Freshdesk surveys endpoint
    try:
        csat_map = await fetch_all_csat_ratings(days_back=req.days_back)
    except Exception as e:
        raise HTTPException(500, f"Freshdesk CSAT fetch failed: {e}")

    if not csat_map:
        return {"updated": 0, "message": "No CSAT data found in Freshdesk. Check that satisfaction surveys are enabled and responses exist for this period."}

    # Bulk-update DB — only tickets owned by this user that have a csat value
    result = await db.execute(
        select(Ticket).where(Ticket.user_id == user.id)
    )
    tickets = result.scalars().all()

    updated = 0
    for ticket in tickets:
        tid = str(ticket.ticket_id)
        if tid in csat_map:
            ticket.csat = csat_map[tid]
            updated += 1

    await db.commit()

    # Also invalidate any relevant Redis caches that include csat
    try:
        from database import redis_client
        async with redis_client() as r:
            # Scorecard and agent scorecard caches include csat
            keys = await r.keys(f"sc2:{user.id}:*")
            keys += await r.keys(f"asc:{user.id}:*")
            keys += await r.keys(f"qaf:{user.id}:*")
            if keys:
                await r.delete(*keys)
    except Exception:
        pass  # cache invalidation is best-effort

    return {
        "updated": updated,
        "csat_found_in_freshdesk": len(csat_map),
        "tickets_in_db": len(tickets),
        "message": f"Backfilled CSAT for {updated} tickets from {len(csat_map)} Freshdesk responses.",
    }
