import logging
import time
from datetime import date, datetime, timedelta
from enum import Enum
from math import ceil
from typing import List, Optional, Union

import orjson
from aiocache import SimpleMemoryCache, cached
from aiocache.plugins import HitMissRatioPlugin, TimingPlugin
from buildpg import S, V, funcs, logic, render
from fastapi import Depends, Query, Request
from pydantic import BaseModel, validator

logger = logging.getLogger("base")
logger.setLevel(logging.DEBUG)


class Measurand(str, Enum):
    pm25 = "pm25"
    pm10 = "pm10"
    so2 = "so2"
    no2 = "no2"
    o3 = "o3"
    bc = "bc"


class Sort(str, Enum):
    asc = "asc"
    desc = "desc"


class Order(str, Enum):
    city = "city"
    country = "country"
    location = "location"
    count = "count"
    datetime = "datetime"


class Spatial(str, Enum):
    city = "city"
    country = "country"
    location = "location"


class Temporal(str, Enum):
    day = "day"
    month = "month"
    year = "year"


class Coordinates(BaseModel):
    lat: float = None
    lon: float = None

    @validator("lat")
    def lat_within_range(cls, v):
        if not -90 <= v <= 90:
            raise ValueError("Latitude outside allowed range")
        return v

    @validator("lon")
    def lon_within_range(cls, v):
        if not -180 <= v <= 180:
            raise ValueError("Longitude outside allowed range")
        return v


class Paging:
    def __init__(
        self,
        limit: Optional[int] = Query(100, gt=0, le=10000),
        page: Optional[int] = Query(1, gt=0),
        sort: Optional[Sort] = Query("desc"),
        order_by: Optional[Order] = Query("site_name"),
    ):
        self.limit = limit
        self.page = page
        self.sort = sort
        self.order_by = order_by

    async def sql(self):
        order_by = self.order_by
        sort = self.sort
        if sort == "asc":
            order = " ORDER BY json->:order_by ASC "
        else:
            order = " ORDER BY json->:order_by DESC NULLS LAST "
        offset = (self.page - 1) * self.limit
        return {
            "q": f"{order} OFFSET :offset LIMIT :limit",
            "params": {
                "order_by": order_by,
                "offset": offset,
                "limit": self.limit,
            },
        }


class MeasurementPaging:
    def __init__(
        self,
        limit: Optional[int] = Query(100, gt=0, le=10000),
        page: Optional[int] = Query(1, gt=0),
        sort: Optional[Sort] = Query("desc"),
        order_by: Optional[Order] = Query("datetime"),
        date_from: Union[datetime, date, None] = date.fromisoformat(
            "2014-01-01"
        ),
        date_to: Union[datetime, date, None] = datetime.utcnow(),
    ):
        self.limit = limit
        self.page = page
        self.sort = sort
        self.order_by = order_by
        self.date_from = self.date_from_adj = datetime(
            *date_from.timetuple()[:-6]
        )
        self.date_to = self.date_to_adj = datetime(*date_to.timetuple()[:-6])
        self.offset = (self.page - 1) * self.limit
        logger.debug(
            "%s %s %s", date_from, date_to, (self.offset + limit) / 1000
        )
        time_offset = timedelta(days=ceil((self.offset + limit) / 1000))
        logger.debug(
            "%s %s %s %s %s",
            self.date_from,
            self.date_to,
            time_offset,
            self.date_from_adj,
            self.date_to_adj,
        )

        if sort == "desc":
            self.date_from_adj = self.date_to - time_offset
        else:
            self.date_to_adj = self.date_from + time_offset

    async def sql(self):
        order_by = self.order_by
        sort = self.sort
        if sort == "asc":
            order = " ORDER BY :order_by ASC "
        else:
            order = " ORDER BY :order_by DESC NULLS LAST "
        offset = (self.page - 1) * self.limit
        return {
            "q": f"{order} OFFSET :offset LIMIT :limit",
            "params": {
                "order_by": order_by,
                "offset": offset,
                "limit": self.limit,
            },
        }


class Geo:
    def __init__(
        self,
        coordinates: Optional[str] = Query(
            None, regex=r"^-?1?\d{1,2}\.?\d{0,8},\d{1,2}\.?\d{0,8}$"
        ),
        radius: Optional[int] = Query(1000, gt=0, le=100000),
    ):
        self.radius = radius
        self.coordinates = None
        if coordinates is not None:
            lon, lat = str.split(coordinates, ",")
            self.coordinates = Coordinates(lon=lon, lat=lat)
            point = funcs.cast(
                logic.Func("st_makepoint", lon, lat), "geography"
            )
            self.sql = logic.Func("st_dwithin", point, V("geog"), self.radius)
        else:
            self.sql = S(True)


async def overlaps(field: str, param: str, val: List):
    if val is not None:
        q = orjson.dumps([{field: [v]} for v in val]).decode()
        w = f"json @> ANY(jsonb_array(:{param}::jsonb))"
        logger.debug("q: %s, w: %s", q, w)
        return {"w": w, "q": q, "param": param}
    return None


async def isin(field: str, param: str, val: List):
    if val is not None:
        q = orjson.dumps([{field: v} for v in val]).decode()
        w = f"json @> ANY(jsonb_array(:{param}::jsonb))"
        logger.debug("q: %s, w: %s", q, w)
        return {"w": w, "q": q, "param": param}
    return None


class Filters:
    def __init__(
        self,
        country: Optional[List[str]] = Query(
            None, aliases=("country[]",), max_length=2
        ),
        site_name: Optional[List[str]] = Query(
            None,
            aliases=(
                "location",
                "location[]",
            ),
        ),
        city: Optional[List[str]] = Query(None, aliases=("city[]",)),
        measurand: Optional[List[Measurand]] = Query(
            None,
            aliases=(
                "parameter",
                "parameter[]",
            ),
        ),
        has_geo: Optional[bool] = None,
        geo: Geo = Depends(),
    ):
        self.country = country
        self.site_name = site_name
        self.city = city
        self.measurand = measurand
        self.coordinates = geo.coordinates
        self.has_geo = has_geo
        self.radius = geo.radius

    async def sql(self):
        jsonpath = {}
        params = {}
        wheres = []
        if self.has_geo is not None:
            jsonpath["has_geo"] = self.has_geo

        site_names_clause = await overlaps(
            "site_names", "site_name", self.site_name
        )
        country_clause = await isin("country", "country", self.country)
        cities_clause = await overlaps("cities", "city", self.city)

        wheres.append(site_names_clause)
        wheres.append(country_clause)
        wheres.append(cities_clause)

        if self.coordinates is not None:
            lon = self.coordinates.lon
            lat = self.coordinates.lat
            radius = self.radius
            w = """
                st_dwithin(
                    st_makepoint(:lon,:lat)::geography,
                    geog,
                    :radius::int
                )
                """

            wheres.append({"w": w})
            params["lon"] = lon
            params["lat"] = lat
            params["radius"] = radius

        if jsonpath != {}:
            wheres.append(
                {"w": "json @> :jsonpath", "q": jsonpath, "param": "jsonpath"}
            )

        where_stmts = [w["w"] for w in wheres if w is not None]
        logger.debug("wheres: %s", where_stmts)

        sql = " AND ".join(where_stmts)

        for w in wheres:
            if w is not None and w.get("param", None) is not None:
                logger.debug(f"{w} {w['param']} {w['q']}")
                params[w["param"]] = w["q"]
        if sql == "":
            sql = " TRUE "

        return {"q": sql, "params": params}


def default(obj):
    return str(obj)


def dbkey(m, f, query, args):
    j = orjson.dumps(
        args, option=orjson.OPT_OMIT_MICROSECONDS, default=default
    ).decode()
    dbkey = f"{query}{j}"
    h = hash(dbkey)
    logger.debug(f"dbkey: {dbkey} h: {h}")
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


class DB:
    def __init__(self, request: Request):
        self.pool = request.app.state.pool

    @cached(900, **cache_config)
    async def fetch(self, query, kwargs):
        start = time.time()
        logger.debug("Start time: %s", start)
        rquery, args = render(query, **kwargs)
        r = await self.pool.fetch(rquery, *args)
        logger.debug(
            "query: %s, args: %s, took: %s", rquery, args, time.time() - start
        )
        return r  # [dict(row) for row in r]

    @cached(900, **cache_config)
    async def fetchrow(self, query, kwargs):
        start = time.time()
        logger.debug("Start time: %s", start)
        rquery, args = render(query, **kwargs)
        r = await self.pool.fetchrow(rquery, *args)
        logger.debug(
            "query: %s, args: %s, took: %s", rquery, args, time.time() - start
        )
        return r  # dict(r)

    @cached(900, **cache_config)
    async def fetchval(self, query, kwargs):
        start = time.time()
        logger.debug("Start time: %s", start)
        rquery, args = render(query, **kwargs)
        r = await self.pool.fetchval(rquery, *args)
        logger.debug(
            "query: %s, args: %s, took: %s", rquery, args, time.time() - start
        )
        return r
