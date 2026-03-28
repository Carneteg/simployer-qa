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
    """
    Create all tables + ensure new columns exist (safe to run on every startup).

    SQLAlchemy create_all only creates missing TABLES, not missing COLUMNS on
    existing tables. For column additions we use ADD COLUMN IF NOT EXISTS so this
    is fully idempotent — safe to run on every boot regardless of current DB state.
    """
    from models import Base as ModelBase
    from sqlalchemy import text

    async with engine.begin() as conn:
        # 1. Create any missing tables
        await conn.run_sync(ModelBase.metadata.create_all)

        # 2. Ensure 003_cx_fields columns exist (idempotent — IF NOT EXISTS)
        new_cols = [
            "ALTER TABLE evaluations ADD COLUMN IF NOT EXISTS churn_confirmed  BOOLEAN NOT NULL DEFAULT false",
            "ALTER TABLE evaluations ADD COLUMN IF NOT EXISTS msg_count        INTEGER",
            "ALTER TABLE evaluations ADD COLUMN IF NOT EXISTS cx_bad           BOOLEAN NOT NULL DEFAULT false",
            "ALTER TABLE evaluations ADD COLUMN IF NOT EXISTS cx_signals       TEXT[]",
        ]
        for ddl in new_cols:
            try:
                await conn.execute(text(ddl))
            except Exception as e:
                logger.warning(f"Column DDL skipped (may already exist): {e}")

        # 3. Ensure supporting index exists
        try:
            await conn.execute(text(
                "CREATE INDEX IF NOT EXISTS ix_evals_user_cx_bad "
                "ON evaluations (user_id, cx_bad)"
            ))
        except Exception as e:
            logger.warning(f"Index creation skipped: {e}")

    logger.info("DB tables and columns verified/created")


async def get_pool_status() -> dict:
    """Return current connection pool metrics for monitoring."""
    pool = engine.pool
    stats = {
        "pool_size":   pool.size() if hasattr(pool, "size") else None,
        "checked_in":  pool.checkedin() if hasattr(pool, "checkedin") else None,
        "checked_out": pool.checkedout() if hasattr(pool, "checkedout") else None,
        "overflow":    pool.overflow() if hasattr(pool, "overflow") else None,
    }
    return {k: v for k, v in stats.items() if v is not None}
