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
from database import init_db
from routers import auth, runs, tickets, agents, export, scorecard

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
    # Always ensure tables exist (safe to run multiple times - CREATE TABLE IF NOT EXISTS)
    try:
        await init_db()
        logger.info("DB tables initialised")
    except Exception as e:
        logger.warning(f"init_db skipped: {e}")
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
    t0 = time.time()
    response = await call_next(request)
    ms = round((time.time() - t0) * 1000)
    logger.info(f"{request.method} {request.url.path} → {response.status_code} ({ms}ms)")
    return response


# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(auth.router,    prefix="/auth",    tags=["auth"])
app.include_router(runs.router,    prefix="/runs",    tags=["runs"])
app.include_router(tickets.router, prefix="/tickets", tags=["tickets"])
app.include_router(agents.router,  prefix="/agents",  tags=["agents"])
app.include_router(export.router,  prefix="/export",  tags=["export"])


# ── Health check ─────────────────────────────────────────────────────────────
@app.get("/health", tags=["ops"])
async def health():
    return {"status": "ok", "environment": settings.environment}


# ── WebSocket: live run progress ──────────────────────────────────────────────
@app.websocket("/ws/runs/{run_id}")
async def run_progress(ws: WebSocket, run_id: str):
    """
    Poll the database every 2s and stream run progress to the browser.
    Works without Redis pub/sub — compatible with Render free tier.
    """
    from database import AsyncSessionLocal
    from models import Run
    import uuid as _uuid

    await ws.accept()
    try:
        while True:
            async with AsyncSessionLocal() as db:
                try:
                    run = await db.get(Run, _uuid.UUID(run_id))
                except Exception:
                    run = None

            if run:
                pct = 0
                if run.tickets_total and run.tickets_total > 0:
                    pct = round(run.tickets_done / run.tickets_total * 100)

                payload = json.dumps({
                    "run_id": run_id,
                    "status": run.status,
                    "done": run.tickets_done,
                    "total": run.tickets_total,
                    "churn": run.churn_count,
                    "pct": pct,
                    "error": run.error,
                })
                try:
                    await ws.send_text(payload)
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
