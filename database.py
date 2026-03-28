import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from config import settings

logger = logging.getLogger("simployer.db")

engine = create_async_engine(
    settings.database_url,

    # ── Pool settings — Render Starter plan (0.5 CPU, 512 MB RAM) ────────────
    # PostgreSQL on Render Starter allows up to 97 max_connections.
    # Web service + evaluator share this pool — 10 persistent + 20 overflow
    # gives comfortable headroom for concurrent API requests and evaluation runs.
    pool_size=10,          # 10 persistent connections
    max_overflow=20,       # Up to 20 extra on burst
    pool_timeout=30,       # Wait max 30s for a free connection before raising
    pool_recycle=1800,     # Recycle connections every 30min (prevents stale conn errors)
    pool_pre_ping=True,    # Test connection health before use (handles DB restarts)

    # ── Query timeout ─────────────────────────────────────────────────────────
    connect_args={
        "command_timeout": 60,
        "server_settings": {
            "application_name": "simployer_qa",
            "statement_timeout": "30000",              # 30s statement timeout (ms)
            "idle_in_transaction_session_timeout": "60000",
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
        "pool_size":    pool.size(),
        "checked_in":  pool.checkedin(),
        "checked_out": pool.checkedout(),
        "overflow":    pool.overflow(),
        "invalid":     pool.invalid(),
    }
