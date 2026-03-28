"""
Simployer QA — FastAPI backend
"""
import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from config import settings
from database import init_db, get_pool_status
from routers import auth, runs, tickets, agents, export, scorecard, agent_scorecard, debug, categories

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("simployer_qa")

if settings.sentry_dsn:
    import sentry_sdk
    from sentry_sdk.integrations.fastapi import FastApiIntegration
    sentry_sdk.init(dsn=settings.sentry_dsn, integrations=[FastApiIntegration()])


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── 1. Ensure tables exist ────────────────────────────────────────────────
    try:
        await init_db()
        logger.info("DB tables initialised")
    except Exception as e:
        logger.warning(f"init_db skipped: {e}")

    # ── 2. Stale run watchdog ─────────────────────────────────────────────────
    # On any restart (deploy, OOM, crash), a run in 'running' status was
    # interrupted mid-evaluation. Mark it failed so users see a clear signal
    # and know to start a new run rather than waiting forever.
    try:
        from sqlalchemy import select, update
        from datetime import datetime, timezone, timedelta
        from database import AsyncSessionLocal
        from models import Run

        async with AsyncSessionLocal() as db:
            # Mark runs stuck in 'running' as failed
            stale = await db.execute(
                select(Run).where(Run.status == "running")
            )
            stale_runs = stale.scalars().all()
            for run in stale_runs:
                run.status    = "failed"
                run.error     = (
                    "Run interrupted — the server was restarted or the instance "
                    "slept while this run was in progress. Start a new run to retry."
                )
                run.finished_at = datetime.now(timezone.utc)
                logger.warning(
                    f"Watchdog: marked stale run {run.id} as failed "
                    f"(was running since {run.started_at})"
                )
            if stale_runs:
                await db.commit()
                logger.info(f"Watchdog: {len(stale_runs)} stale run(s) marked failed")
            else:
                logger.info("Watchdog: no stale runs found")
    except Exception as e:
        logger.error(f"Watchdog failed: {e}")

    yield


app = FastAPI(title="Simployer QA", version="3.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Request logging ───────────────────────────────────────────────────────────
@app.middleware("http")
async def log_requests(request: Request, call_next):
    # Skip logging for health — keeps it pure in-memory, <5ms
    if request.url.path == "/health":
        return await call_next(request)
    t0 = time.time()
    response = await call_next(request)
    ms = round((time.time() - t0) * 1000)
    logger.info(f"{request.method} {request.url.path} → {response.status_code} ({ms}ms)")
    return response


# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(auth.router,          prefix="/auth",          tags=["auth"])
app.include_router(runs.router,          prefix="/runs",          tags=["runs"])
app.include_router(tickets.router,       prefix="/tickets",       tags=["tickets"])
app.include_router(agents.router,        prefix="/agents",        tags=["agents"])
app.include_router(export.router,        prefix="/export",        tags=["export"])
app.include_router(scorecard.router,     prefix="/scorecard",     tags=["scorecard"])
app.include_router(agent_scorecard.router, prefix="/agent-scorecard", tags=["agent-scorecard"])
app.include_router(debug.router,         prefix="/debug",         tags=["debug"])
app.include_router(categories.router,    prefix="/categories",    tags=["categories"])


# ── Health check ─────────────────────────────────────────────────────────────
@app.get("/health", tags=["ops"])
async def health():
    from services.cache import get_pool_status as redis_pool_status
    db_pool   = await get_pool_status()
    redis_pool = await redis_pool_status()
    return {
        "status":      "ok",
        "environment": settings.environment,
        "db_pool":     db_pool,
        "redis":       redis_pool,
    }


# ── WebSocket: live run progress (Redis pub/sub) ──────────────────────────────
@app.websocket("/ws/runs/{run_id}")
async def run_progress(ws: WebSocket, run_id: str):
    """
    Subscribe to Redis pub/sub channel for real-time run progress.
    Falls back to DB polling if Redis is unavailable.

    Upgrade: replaced 2s DB polling with instant Redis push events.
    Starter plan gives 250 connections — pub/sub subscriptions are cheap.
    """
    from services.cache import redis as _redis, key_run_channel
    import uuid as _uuid

    await ws.accept()

    # ── Try Redis pub/sub first ────────────────────────────────────────────────
    try:
        pubsub = _redis.pubsub()
        await pubsub.subscribe(key_run_channel(run_id))

        try:
            # Send current DB state immediately so UI doesn't wait for first event
            from database import AsyncSessionLocal
            from models import Run
            async with AsyncSessionLocal() as db:
                run = await db.get(Run, _uuid.UUID(run_id))
            if run:
                pct = round(run.tickets_done / run.tickets_total * 100) if run.tickets_total else 0
                await ws.send_text(json.dumps({
                    "run_id": run_id, "status": run.status,
                    "done": run.tickets_done, "total": run.tickets_total,
                    "churn": run.churn_count, "pct": pct,
                }))
                if run.status in ("done", "failed"):
                    return

            # Stream Redis events as they arrive
            while True:
                try:
                    msg = await asyncio.wait_for(
                        pubsub.get_message(ignore_subscribe_messages=True),
                        timeout=30.0,   # heartbeat — resend DB state every 30s
                    )
                except asyncio.TimeoutError:
                    # Heartbeat: re-read DB and push current state
                    from database import AsyncSessionLocal
                    from models import Run
                    async with AsyncSessionLocal() as db:
                        run = await db.get(Run, _uuid.UUID(run_id))
                    if run:
                        pct = round(run.tickets_done / run.tickets_total * 100) if run.tickets_total else 0
                        await ws.send_text(json.dumps({
                            "run_id": run_id, "status": run.status,
                            "done": run.tickets_done, "total": run.tickets_total,
                            "churn": run.churn_count, "pct": pct,
                        }))
                        if run.status in ("done", "failed"):
                            break
                    continue

                if msg and msg.get("type") == "message":
                    data = msg["data"]
                    await ws.send_text(data)
                    try:
                        parsed = json.loads(data)
                        if parsed.get("status") in ("done", "failed"):
                            break
                    except Exception:
                        pass

        except WebSocketDisconnect:
            pass
        finally:
            await pubsub.unsubscribe(key_run_channel(run_id))
            await pubsub.aclose()

    except Exception as redis_err:
        # ── Fallback: DB polling if Redis is down ─────────────────────────────
        logger.warning(f"Redis unavailable for WebSocket, falling back to DB poll: {redis_err}")
        from database import AsyncSessionLocal
        from models import Run
        try:
            while True:
                async with AsyncSessionLocal() as db:
                    run = await db.get(Run, _uuid.UUID(run_id))
                if run:
                    pct = round(run.tickets_done / run.tickets_total * 100) if run.tickets_total else 0
                    try:
                        await ws.send_text(json.dumps({
                            "run_id": run_id, "status": run.status,
                            "done": run.tickets_done, "total": run.tickets_total,
                            "churn": run.churn_count, "pct": pct,
                        }))
                    except Exception:
                        break
                    if run.status in ("done", "failed"):
                        break
                await asyncio.sleep(2)
        except WebSocketDisconnect:
            pass


# ── Serve frontend ────────────────────────────────────────────────────────────
try:
    app.mount("/app", StaticFiles(directory="frontend", html=True), name="frontend")

    @app.get("/")
    async def root():
        return FileResponse("frontend/index.html")
except Exception:
    @app.get("/")
    async def root():
        return {"message": "Simployer QA API", "docs": "/docs"}
