import logging
import os
import pathlib

from fastapi import APIRouter, Depends, Path, Response
from starlette.requests import Request
from starlette.responses import HTMLResponse
from starlette.templating import Jinja2Templates
from timvt.endpoints.factory import TILE_RESPONSE_PARAMS
from timvt.models.mapbox import TileJSON
from timvt.models.metadata import TableMetadata

from .base import DB

templates = Jinja2Templates(
    directory=os.path.join(
        str(pathlib.Path(__file__).parent.parent), "templates"
    )
)


logger = logging.getLogger("locations")
logger.setLevel(logging.DEBUG)

router = APIRouter()


table = TableMetadata(
    id="locations",
    schema="public",
    table="locations",
    geometry_column="geom",
    srid=3857,
    geometry_type="Point",
    properties={
        "location_id": "int",
        "location": "text",
        "last_datetime": "timestamptz",
        "count": "int",
    },
    link="test",
)


@router.get(
    "/v2/locations/tiles/{z}/{x}/{y}.pbf",
    **TILE_RESPONSE_PARAMS,
)
async def get_tile(
    z: int = Path(..., ge=0, le=30, description="Mercator tiles's zoom level"),
    x: int = Path(..., description="Mercator tiles's column"),
    y: int = Path(..., description="Mercator tiles's row"),
    db: DB = Depends(),
):
    """Return vector tile."""
    query = """
        WITH
        bounds AS (
            SELECT ST_TileEnvelope(:z,:x,:y) as tile
        ),
        t AS (
            SELECT
                location_id,
                location,
                last_datetime,
                count,
                ST_AsMVTGeom(
                    geom,
                    tile
                ) as mvt
            FROM locations, bounds
            WHERE
            geom && tile
        )
        SELECT ST_AsMVT(t) FROM t;
    """

    params = {"z": z, "x": x, "y": y}

    vt = await db.fetchval(query, params)

    return Response(content=vt, status_code=200, media_type="application/x-protobuf")


@router.get(
    "/v2/locations/tiles/tiles.json",
    response_model=TileJSON,
    responses={200: {"description": "Return a tilejson"}},
    response_model_exclude_none=True,
)
async def tilejson(request: Request):
    """Return TileJSON document."""
    kwargs = {
        "z": "{z}",
        "x": "{x}",
        "y": "{y}",
    }
    tile_endpoint = request.url_for("get_tile", **kwargs).replace("\\", "")
    return {
        "minzoom": 0,
        "maxzoom": 30,
        "name": table.id,
        "tiles": [tile_endpoint],
    }


@router.get("/v2/locations/tiles/viewer", response_class=HTMLResponse)
def demo(
    request: Request,
):
    """Demo for each table."""
    tile_url = request.url_for("tilejson").replace("\\", "")
    context = {"endpoint": tile_url, "request": request}
    return templates.TemplateResponse(
        name="vtviewer.html", context=context, media_type="text/html"
    )
