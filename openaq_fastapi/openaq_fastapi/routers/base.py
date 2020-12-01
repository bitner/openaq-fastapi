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

import re
import pytz

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
    ASC = "ASC"
    DESC = "DESC"


class Order(str, Enum):
    city = "city"
    country = "country"
    location = "location"
    count = "count"
    datetime = "datetime"
    locations = "locations"
    firstUpdated = "firstUpdated"
    lastUpdated = "lastUpdated"
    name = "name"
    id = "id"


class Spatial(str, Enum):
    city = "city"
    country = "country"
    location = "location"
    project = "project"


class Temporal(str, Enum):
    day = "day"
    month = "month"
    year = "year"
    moy = "moy"
    dow = "dow"
    hour = "hour"




class IncludeFields(str, Enum):
    attribution = "attribution"
    averagingPeriod = "averagingPeriod"
    sourceName = "sourceName"


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
        order_by: Optional[Order] = Query("lastUpdated"),
    ):
        self.limit = limit
        self.page = page
        self.sort = sort
        self.order_by = order_by

    async def sql(self):
        order_by = self.order_by
        sort = str.lower(self.sort)
        if order_by in ['count', 'locations']:
            q = '(json->>:order_by)::int'
        elif order_by in ['firstUpdated', 'lastUpdated']:
            q = '(json->>:order_by)::timestamptz'
        else:
            q = 'json->>:order_by'
        if sort == "asc":
            order = f" ORDER BY {q} ASC "
        else:
            order = f" ORDER BY {q} DESC NULLS LAST "
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
            "2006-01-01"
        ),
        date_to: Union[datetime, date, None] = datetime.utcnow(),
    ):
        self.limit = limit
        self.page = page
        self.sort = sort
        self.order_by = order_by
        df = datetime(
            *date_from.timetuple()[:-6],
        )
        df -= timedelta(
            minutes=df.minute % 15,
            seconds=df.second,
            microseconds=df.microsecond
        )
        dt = datetime(
            *date_to.timetuple()[:-6],
        )
        dt -= timedelta(
            minutes=dt.minute % 15,
            seconds=dt.second,
            microseconds=dt.microsecond
        )
        self.date_from = self.date_from_adj = df.replace(tzinfo=pytz.UTC)
        self.date_to = self.date_to_adj = dt.replace(tzinfo=pytz.UTC)
        self.offset = (self.page - 1) * self.limit
        self.totalrows = self.limit + self.offset



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
            order = f" ORDER BY {order_by} ASC "
        else:
            order = f" ORDER BY {order_by} DESC NULLS LAST "
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
            None, regex=r"^-?1?\d{1,2}\.?\d{0,8},-?\d{1,2}\.?\d{0,8}$"
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
        project: Optional[List[str]] = Query(
            None, aliases=(
                "project",
                "projectid",
                "project_id",
                "projectId[]",
                "source_name",
            ),
        ),
        country: Optional[List[str]] = Query(
            None, aliases=(
                "country[]",
                "country[0]",
                "country[1]",
                "country[2]",
                "country[3]",
                "country[4]",
                "country[5]",
                "country[6]",
                "country[7]",
                "country[8]",
                "country[9]",
            ), max_length=2
        ),
        site_name: Optional[List[str]] = Query(
            None,
            aliases=(
                "location",
                "location[]",
                "location[0]",
                "location[1]",
                "location[2]",
                "location[3]",
                "location[4]",
                "location[5]",
                "location[6]",
                "location[7]",
                "location[8]",
                "location[9]",
            ),
        ),
        city: Optional[List[str]] = Query(None, aliases=("city[]",)),
        measurand: Optional[List[Measurand]] = Query(
            None,
            aliases=(
                "parameter",
                "parameter[]",
                "parameter[0]",
                "parameter[1]",
                "parameter[2]",
                "parameter[3]",
                "parameter[4]",
                "parameter[5]",
                "parameter[6]",
                "parameter[7]",
                "parameter[8]",
                "parameter[9]",
            ),
        ),
        include_fields: Optional[List[IncludeFields]] = Query(
            None,
            aliases=(
                "include_fields[]",
                "include_fields[0]",
                "include_fields[1]",
                "include_fields[2]",
                "include_fields[3]",
                "include_fields[4]",
                "include_fields[5]",
                "include_fields[6]",
                "include_fields[7]",
                "include_fields[8]",
                "include_fields[9]",
            ),
        ),
        has_geo: Optional[bool] = None,
        geo: Geo = Depends(),
    ):
        self.country = country

        self.sensor_nodes_id = None
        self.site_name = None
        r = re.compile(r'^\d+$')
        if site_name is not None:
            nodes_list = list(filter(r.match, site_name))
            logger.debug(f'NODES: {nodes_list} len: {len(nodes_list)} {len(site_name)}')
            if len(nodes_list) > 0 and len(nodes_list) == len(site_name):
                self.sensor_nodes_id = [int(s) for s in site_name]
            else:
                self.site_name = site_name

        self.city = city
        self.measurand = measurand
        self.coordinates = geo.coordinates
        self.has_geo = has_geo
        self.radius = geo.radius
        self.include_fields = include_fields
        self.project = project
        self.source_name = project

    def get_measurement_q(self):
        if self.measurand:
            m_array = orjson.dumps([{"sensor_systems":[{"sensors":[{"measurand":m}]}]} for m in self.measurand]).decode()
            measurand_clause = 'json @> ANY(jsonb_array(:measurand::jsonb))'
            return({'w':measurand_clause, 'q': m_array, 'param': 'measurand'})

    async def measurement_fields(self):
        if self.include_fields is not None:
            return ',' + ','.join([f'"{f}"' for f in self.include_fields])
        else:
            return ''

    async def sql(self):
        jsonpath = {}
        params = {}
        wheres = []
        if self.has_geo is not None:
            jsonpath["has_geo"] = self.has_geo

        site_names_clause = await overlaps(
            "site_names", "site_name", self.site_name
        )
        sensor_nodes_id_clause = await isin(
            "sensor_nodes_id", "sensor_nodes_id", self.sensor_nodes_id
        )
        country_clause = await isin("country", "country", self.country)
        project_clause = await isin('source_name', 'project', self.project)
        cities_clause = await overlaps("cities", "city", self.city)

        wheres.append(self.get_measurement_q())
        wheres.append(sensor_nodes_id_clause)
        wheres.append(site_names_clause)
        wheres.append(country_clause)
        wheres.append(cities_clause)
        wheres.append(project_clause)

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
                {"w": "json @> :jsonpath", "q": orjson.dumps(jsonpath).decode(), "param": "jsonpath"}
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

class MeasurementFilters(Filters):
    def get_measurement_q(self):
        if self.measurand:
            measurand_clause = 'measurand = ANY(:measurand)'
            return({'w':measurand_clause, 'q': self.measurand, 'param': 'measurand'})

    async def sql(self):
        wheres = []
        params = {}
        for p in ['city', 'country', 'measurand', 'site_name', 'sensor_nodes_id', 'source_name']:
            vals = getattr(self, p)
            if vals is not None:
                wheres.append({
                    'w': f"{p} = ANY(:{p})",
                    'q': getattr(self,p),
                    'param': p
                })

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
