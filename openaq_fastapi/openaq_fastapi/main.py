import logging
from typing import Any

import asyncpg
import orjson
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.openapi.models import Server
from mangum import Mangum
from starlette.responses import JSONResponse

from .middleware import CacheControlMiddleware, TotalTimeMiddleware
from .routers.averages import router as averages_router
from .routers.measurements import router as measurements_router
from .routers.nodes import router as nodes_router
from .routers.lookups import router as lookups_router
from .settings import settings

logger = logging.getLogger("locations")
logger.setLevel(logging.DEBUG)


class ORJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        return orjson.dumps(content)

servers = [{"url": "https://ytr9800fbk.execute-api.us-east-1.amazonaws.com/"}]



app = FastAPI(
    title="OpenAQ",
    description="API for OpenAQ LCS",
    default_response_class=ORJSONResponse,
    servers=servers,
)

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(CacheControlMiddleware, cachecontrol="public, max-age=900")
app.add_middleware(TotalTimeMiddleware)


@app.on_event("startup")
async def startup_event():
    """
    Application startup:
    register the database connection and create table list.
    """
    logger.info(f"Connecting to {settings.DATABASE_URL}")
    app.state.pool = await asyncpg.create_pool(
        settings.DATABASE_URL, command_timeout=60
    )
    logger.info("Connection established")


@app.on_event("shutdown")
async def shutdown_event():
    """Application shutdown: de-register the database connection."""
    logger.info("Closing connection to database")
    await app.state.pool.close()
    logger.info("Connection closed")


@app.get("/ping")
def pong():
    """
    Sanity check.
    This will let the user know that the service is operational.
    And this path operation will:
    * show a lifesign
    """
    return {"ping": "pong!"}


app.include_router(nodes_router)
app.include_router(measurements_router)
app.include_router(averages_router)
app.include_router(lookups_router)

handler = Mangum(app, enable_lifespan=False)

def run():
    try:
        import uvicorn
        uvicorn.run('openaq_fastapi.main:app', host="0.0.0.0", port=8888, reload=True)
    except:
        pass

if __name__ == "__main__":
    run()
