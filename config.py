from pydantic_settings import BaseSettings
from typing import List
import arq.connections


class Settings(BaseSettings):
    # Database
    database_url: str = "postgresql+asyncpg://user:pass@localhost/simployer_qa"

    # Redis
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

    @property
    def redis_settings(self) -> arq.connections.RedisSettings:
        return arq.connections.RedisSettings.from_dsn(self.redis_url)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
