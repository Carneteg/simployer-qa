from pydantic_settings import BaseSettings
from typing import List
import arq.connections


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://user:pass@localhost/simployer_qa"
    redis_url: str = "redis://localhost:6379"
    secret_key: str = "change-me-in-production"
    token_ttl_hours: int = 8
    freshdesk_domain: str = "simployer.freshdesk.com"
    freshdesk_api_key: str = ""
    anthropic_api_key: str = ""
    cors_origins: List[str] = ["http://localhost:8080"]
    environment: str = "development"
    sentry_dsn: str = ""

    @property
    def redis_settings(self):
        return arq.connections.RedisSettings.from_dsn(self.redis_url)

    class Config:
        env_file = ".env"


settings = Settings()
