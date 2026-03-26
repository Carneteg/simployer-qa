# Simployer QA Cloud

Production-ready QA analysis platform for Freshdesk tickets, powered by Claude AI.

## Quick Start (Render)

1. Create PostgreSQL + Redis on Render
2. Create Web Service: `uvicorn main:app --host 0.0.0.0 --port $PORT`
3. Create Worker: `python worker.py`
4. Set env vars: DATABASE_URL, REDIS_URL, SECRET_KEY, FRESHDESK_API_KEY, ANTHROPIC_API_KEY, CORS_ORIGINS
5. Run: `alembic upgrade head` (shell)
6. Create first user via shell

Cost: ~'$'23/month on Render
