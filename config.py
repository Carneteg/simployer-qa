from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    # Database
    database_url: str = "postgresql+asyncpg://user:pass@localhost/simployer_qa"

    # Redis/Valkey — Render Starter plan (Frankfurt, EU Central)
    # Valkey 8.1.4 | 256 MB RAM | 250 connections | persistence enabled
    redis_url: str = "redis://localhost:6379"

    # Auth
    secret_key: str = "change-me-in-production-use-secrets-token-hex-32"
    token_ttl_hours: int = 8

    # Freshdesk
    freshdesk_domain: str = "simployer.freshdesk.com"
    freshdesk_api_key: str = ""

    # Anthropic
    anthropic_api_key: str = ""

    # App
    cors_origins: List[str] = ["http://localhost:8080", "http://localhost:3000"]
    environment: str = "development"
    sentry_dsn: str = ""
    app_url: str = "https://simployer-qa.onrender.com"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
