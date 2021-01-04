import inspect
import logging
import time
from datetime import date, datetime, timedelta
from enum import Enum
from types import FunctionType
from typing import Dict, List, Optional, Union

import asyncpg
import orjson
from aiocache import SimpleMemoryCache, cached
from aiocache.plugins import HitMissRatioPlugin, TimingPlugin
from buildpg import render
from dateutil.parser import parse
from dateutil.tz import UTC
from fastapi import Query, Request, HTTPException
from pydantic import (
    BaseModel,
    Field,
    confloat,
    conint,
    validator,
)
from pydantic.typing import Any

from ..settings import settings

logger = logging.getLogger("base")
logger.setLevel(logging.DEBUG)

int4 = conint(ge=0, lt=2147483647)

maxint = 2147483647


def parameter_dependency_from_model(name: str, model_cls):
    """
    Takes a pydantic model class as input and creates
    a dependency with corresponding
    Query parameter definitions that can be used for GET
    requests.

    This will only work, if the fields defined in the
    input model can be turned into
    suitable query parameters. Otherwise fastapi
    will complain down the road.

    Arguments:
        name: Name for the dependency function.
        model_cls: A ``BaseModel`` inheriting model class as input.
    """
    names = []
    annotations: Dict[str, type] = {}
    defaults = []
    for field_model in model_cls.__fields__.values():
        if field_model.name not in ["self"]:
            field_info = field_model.field_info

            names.append(field_model.name)
            annotations[field_model.name] = field_model.outer_type_
            defaults.append(
                Query(field_model.default, description=field_info.description)
            )

    code = inspect.cleandoc(
        """
    def %s(%s):
        return %s(%s)
    """
        % (
            name,
            ", ".join(names),
            model_cls.__name__,
            ", ".join(["%s=%s" % (name, name) for name in names]),
        )
    )

    compiled = compile(code, "string", "exec")
    env = {model_cls.__name__: model_cls}
    env.update(**globals())
    func = FunctionType(compiled.co_consts[0], env, name)
    func.__annotations__ = annotations
    func.__defaults__ = (*defaults,)

    return func


class OBaseModel(BaseModel):
    class Config:
        min_anystr_length = 1
        validate_assignment = True
        allow_population_by_field_name = True
        # alias_generator = humps.decamelize
        anystr_strip_whitespace = True

    """@validator("*", each_item=True)
    def validate_no_empty_strings(cls, v):
        if isinstance(v, str) and v == "":
            return None
        return v

    @validator("*")
    def validate_no_empty_lists(cls, v):
        if isinstance(v, list):
            v = list(filter(None, v))
            if len(v) == 0:
                return None
            return v
        return v"""

    @classmethod
    def depends(cls):
        logger.debug(f"Depends {cls}")
        return parameter_dependency_from_model("depends", cls)

    """def wheres(self):
        funcsnames, _ = getmembers(self, isfunction)
        wherefuncs = [f for f in funcnames if"""


class Meta(BaseModel):
    name: str = "openaq-api"
    license: str = "CC BY 4.0d"
    website: str = f"{settings.OPENAQ_FASTAPI_URL}/docs"
    page: int = 1
    limit: int = 100
    found: int = 0


class OpenAQResult(BaseModel):
    meta: Meta = Meta()
    results: Optional[List[Any]] = []


class City(OBaseModel):
    city: Optional[List[str]] = Query(None)


class Country(OBaseModel):
    country_id: Optional[str] = Query(
        None, min_length=2, max_length=2, regex="[a-zA-Z][a-zA-Z]"
    )
    country: Optional[List[str]] = Query(
        None, min_length=2, max_length=2, regex="[a-zA-Z][a-zA-Z]"
    )

    @validator("country", check_fields=False)
    def validate_country(cls, v, values):
        logger.debug(f"validating countries {v} {values}")
        cid = values.get('country_id')
        if cid is not None:
            v = [cid]
        if v is not None:
            logger.debug(f"returning countries {v} {values}")
            return [str.upper(val) for val in v]
        return None


class SourceName(OBaseModel):
    sourceName: Optional[List[str]] = None
    sourceId: Optional[List[int]] = None
    sourceSlug: Optional[List[str]] = None


def id_or_name_validator(name, v, values):
    ret = None
    logger.debug(f"validating {name} {v} {values}")
    id = values.get(f"{name}_id", None)
    if id is not None:
        ret = [id]
    elif v is not None:
        ret = v
    if isinstance(ret, list):
        if all(isinstance(x, int) for x in ret):
            logger.debug("everything is an int")
            if min(ret) < 1 or max(ret) > maxint:
                raise ValueError(
                    name,
                    f"{name}_id must be between 1 and {maxint}",
                )
    logger.debug(f"returning {ret}")
    return ret


class Project(OBaseModel):
    project_id: Optional[int] = None
    project: Optional[List[Union[int, str]]] = Query(None, gt=0, le=maxint)

    @validator("project")
    def validate_project(cls, v, values):
        return id_or_name_validator("project", v, values)


class Location(OBaseModel):
    location_id: Optional[int] = None
    location: Optional[List[Union[int, str]]] = None

    @validator("location")
    def validate_location(cls, v, values):
        return id_or_name_validator("location", v, values)


class HasGeo(OBaseModel):
    has_geo: bool = None

    def where(self):
        if self.has_geo is not None:
            if self.has_geo:
                return " AND geog is not null "
            if not self.has_geo:
                return " AND geog is null "
        return ""


class Geo(OBaseModel):
    coordinates: Optional[str] = Field(
        None, regex=r"^-?\d{1,2}\.?\d{0,8},-?1?\d{1,2}\.?\d{0,8}$"
    )
    lat: Optional[confloat(ge=-90, le=90)] = None
    lon: Optional[confloat(ge=-180, le=180)] = None
    radius: conint(gt=0, le=100000) = 1000

    @validator("lat")
    def validate_lat(cls, v, values):
        coordinates = values.get("coordinates", None)
        if coordinates is not None:
            try:
                lat, _ = values.get("coordinates").split(",")
                lat = float(lat)
                if lat >= -90 and lat <= 90:
                    return lat
                else:
                    raise ValueError("latitude is out of range")
            except Exception as e:
                raise ValueError(f"{e}")

    @validator("lon")
    def validate_lon(cls, v, values):
        coordinates = values.get("coordinates", None)
        if coordinates is not None:
            try:
                _, lon = values.get("coordinates").split(",")
                lon = float(lon)
                if lon >= -180 and lon <= 180:
                    return lon
                else:
                    raise ValueError("longitude is out of range")
            except Exception as e:
                raise ValueError(f"{e}")

    def where_geo(self):
        if self.lat is not None and self.lon is not None:
            return (
                " st_dwithin(st_makepoint(:lon, :lat)::geography,"
                " geom::geography, :radius) "
            )
        return None


class Measurands(OBaseModel):
    parameter_id: Optional[int] = None
    parameter: Optional[List[Union[int, str]]] = Query(None, gt=0, le=maxint)
    measurand: Optional[List[str]] = None
    units: Optional[List[str]] = None

    @validator("measurand", check_fields=False)
    def check_measurand(cls, v, values):
        if v is None:
            return values.get("parameter")
        return v

    @validator("parameter", check_fields=False)
    def validate_parameter(cls, v, values):
        if v is None:
            v = values.get("measurand")
        return id_or_name_validator("project", v, values)


class Paging(OBaseModel):
    limit: int = Query(100, gt=0, le=10000)
    page: int = Query(1, gt=0, le=6000)
    offset: int = Query(0, ge=0, le=10000)

    @validator("offset", check_fields=False)
    def check_offset(cls, v, values, **kwargs):
        offset = values["limit"] * (values["page"] - 1)
        if offset + values["limit"] > 100000:
            raise ValueError("offset + limit must be < 100000")
        return offset


# class Measurand(str, Enum):
#     pm25 = "pm25"
#     pm10 = "pm10"
#     so2 = "so2"
#     no2 = "no2"
#     o3 = "o3"
#     bc = "bc"


class Sort(str, Enum):
    asc = "asc"
    desc = "desc"


# class Order(str, Enum):
#     city = "city"
#     country = "country"
#     location = "location"
#     count = "count"
#     datetime = "datetime"
#     locations = "locations"
#     firstUpdated = "firstUpdated"
#     lastUpdated = "lastUpdated"
#     name = "name"
#     id = "id"
#     measurements = "measurements"


class Spatial(str, Enum):
    country = "country"
    location = "location"
    project = "project"
    total = "total"


class Temporal(str, Enum):
    day = "day"
    month = "month"
    year = "year"
    moy = "moy"
    dow = "dow"
    hour = "hour"
    total = "total"
    hod = "hod"


# class IncludeFields(str, Enum):
#     attribution = "attribution"
#     averagingPeriod = "averagingPeriod"
#     sourceName = "sourceName"


class APIBase(Paging):
    sort: Optional[Sort] = Query("asc")

    def where(self):
        wheres = []
        for f, v in self:
            logger.debug(f"APIBase {f} {v}")
            if isinstance(v, List):
                wheres.append(f"{f} = ANY(:{f})")
        if len(wheres) > 0:
            return (" AND ").join(wheres)
        return " TRUE "


def fix_datetime(
    d: Union[datetime, date, str, int, None],
    minutes_to_round_to: Optional[int] = None,
):
    # Make sure that date/datetime is turned into timzone
    # aware datetime optionally rounding to
    # given number of minutes
    if d is None:
        return None
    if isinstance(d, str):
        d = parse(d)
    if isinstance(d, int):
        d = datetime.fromtimestamp(d)
    if isinstance(d, date):
        d = datetime(
            *d.timetuple()[:-6],
        )
    if d.tzinfo is None:
        d = d.replace(tzinfo=UTC)
    if minutes_to_round_to is not None:
        d -= timedelta(
            minutes=d.minute % minutes_to_round_to,
            seconds=d.second,
            microseconds=d.microsecond,
        )
    return d


class DateRange(OBaseModel):
    date_from: Union[datetime, date, None] = fix_datetime("2000-01-01")
    date_to: Union[datetime, date, None] = fix_datetime(datetime.now())
    date_from_adj: Union[datetime, date, None] = None
    date_to_adj: Union[datetime, date, None] = None

    @validator(
        "date_from",
        "date_to",
        "date_from_adj",
        "date_to_adj",
        check_fields=False,
    )
    def check_dates(cls, v, values):
        return fix_datetime(v)


def default(obj):
    return str(obj)


def dbkey(m, f, query, args):
    j = orjson.dumps(
        args, option=orjson.OPT_OMIT_MICROSECONDS, default=default
    ).decode()
    dbkey = f"{query}{j}"
    h = hash(dbkey)
    # logger.debug(f"dbkey: {dbkey} h: {h}")
    return h


cache_config = {
    "key_builder": dbkey,
    "cache": SimpleMemoryCache,
    "noself": True,
    "plugins": [
        HitMissRatioPlugin(),
        TimingPlugin(),
    ],
}


async def db_pool(pool):
    if pool is None:
        pool = await asyncpg.create_pool(
            settings.DATABASE_URL,
            command_timeout=14,
            max_inactive_connection_lifetime=15,
            min_size=1,
            max_size=10,
        )
    return pool


class DB:
    def __init__(self, request: Request):
        self.request = request

    async def acquire(self):
        pool = await self.pool()
        return pool

    async def pool(self):
        self.request.app.state.pool = await db_pool(
            self.request.app.state.pool
        )
        return self.request.app.state.pool

    @cached(900, **cache_config)
    async def fetch(self, query, kwargs):
        pool = await self.pool()
        start = time.time()
        logger.debug("Start time: %s Query: %s Args:%s", start, query, kwargs)
        rquery, args = render(query, **kwargs)
        async with pool.acquire() as con:
            try:
                r = await con.fetch(rquery, *args)
            except asyncpg.exceptions.UndefinedColumnError as e:
                raise ValueError(f"{e}")
            except asyncpg.exceptions.DataError as e:
                raise ValueError(f"{e}")
            except asyncpg.exceptions.CharacterNotInRepertoireError as e:
                raise ValueError(f"{e}")
            except Exception as e:
                logger.debug(f"Database Error: {e}")
                raise HTTPException(status_code=500, detail=f"{e}")
        logger.debug(
            "query took: %s results_firstrow: %s",
            time.time() - start,
            str(r and r[0])[0:500]
        )
        return r

    async def fetchrow(self, query, kwargs):
        r = await self.fetch(query, kwargs)
        if len(r) > 0:
            return r[0]
        return []

    async def fetchval(self, query, kwargs):
        r = await self.fetchrow(query, kwargs)
        if len(r) > 0:
            return r[0]
        return []

    async def fetchOpenAQResult(self, query, kwargs):
        rows = await self.fetch(query, kwargs)

        if len(rows) == 0:
            found = 0
            results = []
        else:
            found = rows[0]["count"]
            # results = [orjson.dumps(r[1]) for r in rows]
            if len(rows) > 0 and rows[0][1] is not None:
                results = [
                    orjson.loads(r[1]) for r in rows if isinstance(r[1], str)
                ]
            else:
                results = []

        meta = Meta(
            page=kwargs["page"],
            limit=kwargs["limit"],
            found=found,
        )
        output = OpenAQResult(meta=meta, results=results)
        return output
