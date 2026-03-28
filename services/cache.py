"""
Redis/Valkey cache service — Render Starter plan (Frankfurt, EU Central)
  Runtime:    Valkey 8.1.4
  RAM:        256 MB RAM | 250 connection limit | persistence enabled
  Policy:     allkeys-lru

Design:
  - Connection pool: max_connections=20 (well within 250 limit)
  - Application-level caching for expensive DB queries (agents, ticket list)
  - Pub/sub for real-time run progress via WebSocket
  - TTL_AGENTS=300s, TTL_TICKETS=120s (generous — 256 MB available)
  - No cache warming on startup — allkeys-lru persistence survives restarts
"""
import json
import logging
import time
from typing import Any, Optional

import redis.asyncio as aioredis

from config import settings

logger = logging.getLogger("simployer.cache")

# ── Connection pool ───────────────────────────────────────────────────────────
# 250 connection limit on Starter plan.
# Web service uses ~5 concurrent requests peak + 4 evaluator coroutines = ~10 active.
# Pool of 20 gives comfortable headroom without approaching the limit.
_pool = aioredis.ConnectionPool.from_url(
    settings.redis_url,
    max_connections=20,           # 20 of 250 available on Starter plan
    decode_responses=True,
    socket_timeout=5,             # fail fast rather than hang
    socket_connect_timeout=5,
    retry_on_timeout=True,
    health_check_interval=30,     # background health checks keep connections live
)

redis: aioredis.Redis = aioredis.Redis(connection_pool=_pool)

# ── TTLs (generous with 256 MB — old 25 MB forced very short TTLs) ────────────
TTL_AGENTS        = 300    # 5 min  — agents aggregation (GROUP BY, expensive)
TTL_TICKETS       = 120    # 2 min  — ticket list (large result set)
TTL_RUN_PROGRESS  = 3600   # 1 hour — run progress events (pub/sub history)
TTL_AGENT_DETAIL  = 300    # 5 min  — single agent detail
TTL_HEALTH        = 10     # 10 s   — health check Redis ping result


# ── Cache key builders ────────────────────────────────────────────────────────

def key_agents(user_id: str) -> str:
    return f"agents:v1:{user_id}"

def key_tickets(user_id: str, run_id: Optional[str], churn_only: bool,
                limit: int, offset: int) -> str:
    return f"tickets:v1:{user_id}:{run_id or 'all'}:{int(churn_only)}:{limit}:{offset}"

def key_agent_detail(user_id: str, agent_name: str) -> str:
    return f"agent_detail:v1:{user_id}:{agent_name}"

def key_run_channel(run_id: str) -> str:
    return f"run:{run_id}"


# ── Generic get/set ───────────────────────────────────────────────────────────

async def get(key: str) -> Optional[Any]:
    """Get a cached JSON value. Returns None on miss or error."""
    try:
        raw = await redis.get(key)
        if raw is None:
            return None
        return json.loads(raw)
    except Exception as e:
        logger.warning(f"cache get {key!r} failed: {e}")
        return None


async def set(key: str, value: Any, ttl: int) -> bool:
    """Cache a JSON-serialisable value with TTL. Returns True on success."""
    try:
        await redis.set(key, json.dumps(value, default=str), ex=ttl)
        return True
    except Exception as e:
        logger.warning(f"cache set {key!r} failed: {e}")
        return False


async def invalidate(user_id: str) -> None:
    """Invalidate all cached data for a user (call after a new run completes)."""
    patterns = [
        f"agents:v1:{user_id}",
        f"agent_detail:v1:{user_id}:*",
        f"tickets:v1:{user_id}:*",
    ]
    try:
        for pattern in patterns:
            keys = await redis.keys(pattern)
            if keys:
                await redis.delete(*keys)
        logger.info(f"cache invalidated for user {user_id}")
    except Exception as e:
        logger.warning(f"cache invalidate failed for user {user_id}: {e}")


# ── Pub/sub helpers ───────────────────────────────────────────────────────────

async def publish_run_event(run_id: str, payload: dict) -> None:
    """Publish a run progress event to the Redis channel."""
    try:
        await redis.publish(key_run_channel(run_id), json.dumps(payload))
    except Exception as e:
        logger.warning(f"publish run:{run_id} failed: {e}")


async def get_pool_status() -> dict:
    """Return Redis connection pool stats for /health endpoint."""
    try:
        info    = await redis.info("clients")
        latency = await _ping_latency()
        return {
            "connected_clients": info.get("connected_clients"),
            "pool_max":          _pool.max_connections,
            "ping_ms":           latency,
        }
    except Exception as e:
        return {"error": str(e)}


async def _ping_latency() -> float:
    """Measure Redis PING round-trip in ms."""
    t0 = time.monotonic()
    await redis.ping()
    return round((time.monotonic() - t0) * 1000, 1)
