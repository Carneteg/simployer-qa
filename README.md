# Simployer QA Cloud

A production-ready, multi-user QA analysis platform for Freshdesk support tickets, powered by Claude AI.

## Architecture

```
Browser (Vanilla JS)
    ↕ REST + WebSocket
FastAPI (Python)
    ↕ SQLAlchemy async
PostgreSQL          Redis (job queue)
                        ↕ arq
                    Worker Process
                        ↕
                Freshdesk API + Anthropic API
```

## Local Development

### Prerequisites
- Python 3.11+
- PostgreSQL 15+
- Redis 7+

### Setup

```bash
# 1. Clone and install
cd simployer_cloud
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# Edit .env with your values

# 3. Start PostgreSQL and Redis (or use Docker)
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=dev postgres:15
docker run -d -p 6379:6379 redis:7

# 4. Run migrations
alembic upgrade head

# 5. Create first user
python -c "
import asyncio
from database import AsyncSessionLocal
from models import User
from passlib.context import CryptContext

async def create():
    async with AsyncSessionLocal() as db:
        u = User(email='admin@simployer.com',
                 password_hash=CryptContext(['bcrypt']).hash('yourpassword'),
                 org_name='Simployer')
        db.add(u); await db.commit()
        print('Created:', u.id)
asyncio.run(create())
"

# 6. Start web server
uvicorn main:app --reload --port 8000

# 7. Start worker (separate terminal)
python worker.py

# 8. Open browser
open http://localhost:8000
```

## Deploy to Render (30 minutes)

### Step 1: Push to GitHub
```bash
git init && git add . && git commit -m "Initial commit"
gh repo create simployer-qa --private --push
```

### Step 2: Create Render services

1. **PostgreSQL** → render.com → New → PostgreSQL → Free tier → copy DATABASE_URL
2. **Redis** → Use [Upstash](https://upstash.com) free tier → copy REDIS_URL
3. **Web Service** → New → Web Service → connect repo
   - Build: `pip install -r requirements.txt`
   - Start: `uvicorn main:app --host 0.0.0.0 --port $PORT`
4. **Worker** → New → Background Worker → same repo
   - Start: `python worker.py`

### Step 3: Set environment variables (both services)

```
DATABASE_URL      = postgresql+asyncpg://...
REDIS_URL         = redis://...
SECRET_KEY        = <run: python -c "import secrets; print(secrets.token_hex(32))">
FRESHDESK_DOMAIN  = simployer.freshdesk.com
FRESHDESK_API_KEY = O9x5ATZHKOcdH_oaMkDW
ANTHROPIC_API_KEY = sk-ant-api03-...
CORS_ORIGINS      = ["https://your-app.onrender.com"]
ENVIRONMENT       = production
```

### Step 4: Run migrations
```bash
# From Render Web Service Shell tab:
alembic upgrade head
```

### Step 5: Create first user
```bash
# From Render Web Service Shell:
python -c "..."   # same as local setup above
```

**Your app is live at:** `https://your-app.onrender.com`

## API Documentation

Interactive docs available at: `http://localhost:8000/docs`

### Key endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | /auth/register | Create account |
| POST | /auth/login | Get JWT token |
| GET  | /auth/me | Current user |
| POST | /runs/ | Start analysis run |
| GET  | /runs/ | List runs |
| GET  | /runs/{id} | Run status |
| GET  | /tickets/ | All evaluated tickets |
| GET  | /tickets/{id}/messages | Conversation thread |
| GET  | /agents/ | Agent leaderboard |
| GET  | /agents/{name} | Agent scorecard |
| GET  | /export/xlsx | Download Excel |
| GET  | /export/json | Download JSON |
| GET  | /export/csv  | Download CSV |
| WS   | /ws/runs/{id} | Live run progress |

## Cost (Render)

| Service | Tier | Monthly |
|---------|------|---------|
| Web Service | Starter 512MB | $7 |
| Worker | Starter 512MB | $7 |
| PostgreSQL | Starter 1GB | $7 |
| Redis (Upstash) | Free | $0 |
| Anthropic Haiku | ~6k tickets/month | ~$2 |
| **Total** | | **~$23/month** |

## Project Structure

```
simployer_cloud/
├── main.py              # FastAPI app + WebSocket
├── config.py            # Settings from env vars
├── database.py          # SQLAlchemy async engine
├── models.py            # ORM models
├── worker.py            # arq background worker
├── Procfile             # Render/Railway process definitions
├── requirements.txt
├── alembic.ini
├── migrations/
│   ├── env.py
│   └── versions/001_initial_schema.py
├── routers/
│   ├── auth.py          # Login, register, me
│   ├── runs.py          # Start/list/get runs
│   ├── tickets.py       # Query evaluated tickets
│   ├── agents.py        # Agent scorecards
│   └── export.py        # Excel/JSON/CSV export
├── services/
│   ├── freshdesk.py     # Freshdesk API client
│   ├── claude.py        # Anthropic evaluation
│   ├── evaluator.py     # Main background job
│   └── exporter.py      # Excel workbook builder
└── frontend/
    └── index.html       # Complete single-file frontend
```
