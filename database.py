import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from config import settings

logger = logging.getLogger("simployer.db")

engine = create_async_engine(
    settings.database_url,

    # ── Pool settings tuned for Render free tier ──────────────────────────────
    # Free tier: 512 MB RAM, 0.1 CPU, PostgreSQL max_connections = 97
    # Keep pool small — the evaluator + web requests share this pool
    pool_size=5,           # 5 persistent connections (was 10 — too many for free tier)
    max_overflow=10,       # Up to 10 extra on burst (was 20)
    pool_timeout=30,       # Wait max 30s for a free connection before raising
    pool_recycle=1800,     # Recycle connections every 30min (prevents stale conn errors)
    pool_pre_ping=True,    # Test connection health before use (handles DB restarts)

    # ── Query timeout ─────────────────────────────────────────────────────────
    # Prevent runaway queries from holding connections indefinitely
    connect_args={
        "command_timeout": 60,                         # Kill queries > 60s
        "server_settings": {
            "application_name": "simployer_qa",        # Visible in pg_stat_activity
            "statement_timeout": "30000",              # 30s statement timeout (ms)
            "idle_in_transaction_session_timeout": "60000",  # Kill idle txn after 60s
        },
    },
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    pass


async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def init_db():
    """Create all tables if they don't exist (safe to run on every startup)."""
    from models import Base as ModelBase
    async with engine.begin() as conn:
        await conn.run_sync(ModelBase.metadata.create_all)
    logger.info("DB tables verified/created")


async def get_pool_status() -> dict:
    """Return current connection pool metrics for monitoring."""
    pool = engine.pool
    return {
        "pool_size":        pool.size(),
        "checked_in":       pool.checkedin(),
        "checked_out":      pool.checkedout(),
        "overflow":         pool.overflow(),
        "invalid":          pool.invalid(),
    }
