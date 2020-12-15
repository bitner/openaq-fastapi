import logging
from typing import Dict, Optional

from fastapi import APIRouter, Depends, Path, Query
from starlette.requests import Request
from starlette.responses import Response
from timvt.db.tiles import WEB_MERCATOR_TMS, VectorTileReader
from timvt.endpoints.factory import TILE_RESPONSE_PARAMS
from timvt.models.mapbox import TileJSON
from timvt.models.metadata import TableMetadata
from timvt.ressources.enums import MimeTypes
from timvt.utils import Timer
from timvt.templates.factory import web_template
from starlette.responses import HTMLResponse
from timvt.endpoints.factory import VectorTilerFactory

from .base import DB

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

tms = WEB_MERCATOR_TMS
tiler = VectorTilerFactory()


@router.get(
    "/v2/locations/tiles/{z}/{x}/{y}.pbf",
    **TILE_RESPONSE_PARAMS,
    name="get_tile"
)
async def get_tile(
    z: int = Path(..., ge=0, le=30, description="Mercator tiles's zoom level"),
    x: int = Path(..., description="Mercator tiles's column"),
    y: int = Path(..., description="Mercator tiles's row"),
    db: DB = Depends(),
    columns: str = None,
):
    """Return vector tile."""
    db_pool = await db.pool()
    timings = []
    headers: Dict[str, str] = {}

    reader = VectorTileReader(db_pool, table=table)
    with Timer() as t:
        content = await reader.tile(x, y, z, columns=columns)
    timings.append(("db-read", t.elapsed))

    if timings:
        headers["X-Server-Timings"] = "; ".join(
            [
                "{} - {:0.2f}".format(name, time * 1000)
                for (name, time) in timings
            ]
        )

    return Response(content, media_type=MimeTypes.pbf.value, headers=headers)


@router.get(
    "/v2/locations/tiles/tiles.json",
    response_model=TileJSON,
    responses={200: {"description": "Return a tilejson"}},
    response_model_exclude_none=True,
)
async def tilejson(
    request: Request,
    minzoom: Optional[int] = Query(
        None, description="Overwrite default minzoom."
    ),
    maxzoom: Optional[int] = Query(
        None, description="Overwrite default maxzoom."
    ),
):
    """Return TileJSON document."""
    kwargs = {
        "z": "{z}",
        "x": "{x}",
        "y": "{y}",
    }
    tile_endpoint = request.url_for("get_tile", **kwargs).replace("\\", "")
    minzoom = minzoom or tms.minzoom
    maxzoom = maxzoom or tms.maxzoom
    return {
        "minzoom": minzoom,
        "maxzoom": maxzoom,
        "name": table.id,
        "tiles": [tile_endpoint],
    }


@router.get("/v2/locations/tiles/viewer", response_class=HTMLResponse)
def demo(
    request: Request,
    template=Depends(web_template),
):
    """Demo for each table."""
    tile_url = request.url_for("tilejson").replace("\\", "")
    context = {"endpoint": tile_url}
    return template(request, "demo.html", context)
