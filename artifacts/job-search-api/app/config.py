from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str
    REDIS_URL: str = "redis://localhost:6379/0"
    TNJ_JWT_PUBLIC_KEY: str
    TNJ_FRONTEND_URL: str
    CELERY_BROKER_URL: str = ""
    CELERY_RESULT_BACKEND: str = ""
    OPENAI_API_KEY: str = ""
    ADMIN_USER_ID: str = ""

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


@lru_cache
def get_settings() -> Settings:
    return Settings()
