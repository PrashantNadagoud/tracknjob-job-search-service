from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str
    REDIS_URL: str = "redis://localhost:6379/0"
    TNJ_SECRET_KEY: str
    TNJ_FRONTEND_URL: str
    CELERY_BROKER_URL: str = ""
    CELERY_RESULT_BACKEND: str = ""
    OPENAI_API_KEY: str = ""
    ADMIN_USER_ID: str = ""
    CRUNCHBASE_API_KEY: str = ""

    # Crawler seed configuration (comma-separated lists)
    NAUKRI_KEYWORD_LIST: str = "software engineer,data engineer,product manager"
    FOUNDIT_KEYWORD_LIST: str = "software engineer,backend developer"
    WORKDAY_SEED_SLUGS: str = "google,microsoft,amazon,apple,meta,netflix,stripe,airbnb,uber,lyft"

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore"
    }

    def naukri_keywords(self) -> list[str]:
        return [k.strip() for k in self.NAUKRI_KEYWORD_LIST.split(",") if k.strip()]

    def foundit_keywords(self) -> list[str]:
        return [k.strip() for k in self.FOUNDIT_KEYWORD_LIST.split(",") if k.strip()]

    def workday_seed_slugs(self) -> list[str]:
        return [s.strip() for s in self.WORKDAY_SEED_SLUGS.split(",") if s.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
