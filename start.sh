#!/bin/bash
set -e
echo "==> Running Alembic migrations..."
alembic upgrade head
echo "==> Migrations done. Starting uvicorn..."
exec uvicorn main:app --host 0.0.0.0 --port $PORT
