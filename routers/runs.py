import uuid
from typing import List, Optional

import arq
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from database import get_db
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
    created_at: str
    started_at: Optional[str]
    finished_at: Optional[str]

    class Config:
        from_attributes = True

    @classmethod
    def from_orm(cls, run: Run):
        return cls(
            id=run.id,
            status=run.status,
            days_back=run.days_back,
            tickets_total=run.tickets_total,
            tickets_done=run.tickets_done,
            churn_count=run.churn_count,
            error=run.error,
            created_at=str(run.created_at or ""),
            started_at=str(run.started_at) if run.started_at else None,
            finished_at=str(run.finished_at) if run.finished_at else None,
        )


@router.post("/", status_code=202)
async def start_run(
    body: RunCreate,
    user: User = Depends(current_user),
    db: AsyncSession = Depends(get_db),
):
    run = Run(user_id=user.id, days_back=body.days_back)
    db.add(run)
    await db.commit()
    await db.refresh(run)

    pool = await arq.create_pool(settings.redis_settings)
    await pool.enqueue_job("evaluate_run", str(run.id))
    await pool.close()

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
