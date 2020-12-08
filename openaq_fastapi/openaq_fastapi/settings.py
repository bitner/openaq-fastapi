from fastapi import FastAPI
from pydantic import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str
    DATABASE_WRITE_URL: str
    OPENAQ_ENV: str
    OPENAQ_FASTAPI_URL: str

    class Config:
        env_file = ".env"


settings = Settings()
