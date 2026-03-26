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
import redis.asyncio as aioredis

from config import settings
from database import init_db
from routers import auth, runs, tickets, agents, export

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
    if settings.environment == "development":
        await init_db()
        logger.info("DB tables initialised (dev mode)")
    yield


app = FastAPI(title="Simployer QA", version="3.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    t0 = time.time()
    response = await call_next(request)
    ms = round((time.time() - t0) * 1000)
    logger.info(f"{request.method} {request.url.path} → {response.status_code} ({ms}ms)")
    return response


app.include_router(auth.router,    prefix="/auth",    tags=["auth"])
app.include_router(runs.router,    prefix="/runs",    tags=["runs"])
app.include_router(tickets.router, prefix="/tickets", tags=["tickets"])
app.include_router(agents.router,  prefix="/agents",  tags=["agents"])
app.include_router(export.router,  prefix="/export",  tags=["export"])


@app.get("/health", tags=["ops"])
async def health():
    return {"status": "ok", "environment": settings.environment}


@app.websocket("/ws/runs/{run_id}")
async def run_progress(ws: WebSocket, run_id: str):
    await ws.accept()
    r = aioredis.from_url(settings.redis_url, decode_responses=True)
    pubsub = r.pubsub()
    await pubsub.subscribe(f"run:{run_id}")
    try:
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message and message.get("type") == "message":
                await ws.send_text(message["data"])
                try:
                    data = json.loads(message["data"])
                    if data.get("status") in ("done", "failed"):
                        break
                except Exception:
                    pass
            await asyncio.sleep(0.1)
    except WebSocketDisconnect:
        pass
    finally:
        await pubsub.unsubscribe(f"run:{run_id}")
        await r.close()


try:
    app.mount("/app", StaticFiles(directory="frontend", html=True), name="frontend")
    @app.get("/")
    async def root():
        return FileResponse("frontend/index.html")
except Exception:
    @app.get("/")
    async def root():
        return {"message": "Simployer QA API", "docs": "/docs"}
