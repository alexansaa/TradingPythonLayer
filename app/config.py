import os
from pydantic_settings import BaseSettings
from pydantic import field_validator, Field
from dotenv import load_dotenv
from typing import List

load_dotenv()

class Settings(BaseSettings):
    TIINGO_API_KEY: str
    SQLSERVER_HOST: str
    SQLSERVER_PORT: str
    SQLSERVER_DB: str
    SQLSERVER_USER: str
    SQLSERVER_PASSWORD: str
    SQLSERVER_DB_SCHEMA: str

    # SYMBOLS: List[str] = Field(..., env='SYMBOLS', env_parse=lambda v: v.split(','))
    SYMBOLS: List[str]

    INIT_START_DATE: str
    FETCH_INTERVAL_MINUTES: str

    INTRADAY_ENABLED: bool
    INTRADAY_RESAMPLE: str
    INTRADAY_WINDOW_MINUTES: int

    # ODER OF PRECEDENCE
    INTRADAY_INTERVAL_SECONDS: int
    MAX_API_CALLS_PER_HOUR: int
    MAX_API_CALLS_PER_DAY: int

    API_CALLS_BUFFER: int

    UVICORN_HOST: str = "0.0.0.0"
    UVICORN_PORT: int = 18888

    TIMEZONE: str

    ENABLE_SCHEDULER: bool
    SQLSERVER_SCHEDULE_CRON: str
    PY_LAYER_HOST_PORT: int
    MAX_CALLS_PER_DAY: int
    MAX_CALLS_PER_MINUTE: int
    FETCH_INTERVAL_MINUTES: int
    SOURCE_EOD: str
    SA_PASSWORD: str
    RATE_LIMIT_SLEEP: int



    @field_validator("SYMBOLS", mode="before")
    @classmethod
    def split_symbols(cls, v):
        print("parsing symblos")
        if isinstance(v, str):
            print(v)
            return [s.strip().upper() for s in v.split(",") if s.strip()]
        return v

    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()

print("Loaded symbols:", settings.SYMBOLS)