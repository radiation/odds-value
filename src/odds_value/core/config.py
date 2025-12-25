from __future__ import annotations

from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # DB
    database_url: str = "sqlite+pysqlite:///./odds_value.db"
    db_echo: bool = False

    # api-sports
    api_sports_key: Optional[str] = Field(default=None, repr=False)
    api_sports_base_url: str = "https://v1.american-football.api-sports.io"

    store_ingested_payloads: bool = True

    def require_api_key(self) -> str:
        if not self.api_sports_key:
            raise RuntimeError(
                "API_SPORTS_KEY is not set. "
                "Set it in the environment or .env file."
            )
        return self.api_sports_key


settings = Settings()
