import inspect
import logging
import re
import time
from dataclasses import asdict
from datetime import date, datetime, timedelta
from enum import Enum
from math import ceil
from types import FunctionType
from typing import Dict, List, Optional, Union

import asyncpg
import humps
import orjson
import pytz
from aiocache import SimpleMemoryCache, cached
from aiocache.plugins import HitMissRatioPlugin, TimingPlugin
from buildpg import S, V, funcs, logic, render
from dateutil.parser import parse
from dateutil.tz import UTC
from fastapi import Depends, HTTPException, Path, Query, Request
from pydantic import (BaseModel, Field, Json, ValidationError, confloat,
                      conint, constr, validator)
from pydantic.dataclasses import dataclass
from pydantic.error_wrappers import ValidationError
from pydantic.typing import Any, Literal
from starlette.exceptions import HTTPException

from ..settings import settings

logger = logging.getLogger("base")
logger.setLevel(logging.DEBUG)

int4 = conint(ge=0, lt=2147483647)

maxint = 2147483647


def parameter_dependency_from_model(name: str, model_cls):
    '''
    Takes a pydantic model class as input and creates a dependency with corresponding
    Query parameter definitions that can be used for GET
    requests.

    This will only work, if the fields defined in the input model can be turned into
    suitable query parameters. Otherwise fastapi will complain down the road.

    Arguments:
        name: Name for the dependency function.
        model_cls: A ``BaseModel`` inheriting model class as input.
    '''
    names = []
    annotations: Dict[str, type] = {}
    defaults = []
    for field_model in model_cls.__fields__.values():
        logger.debug(f"{field_model.name} {field_model.field_info} {field_model.outer_type_}")
        if field_model.name not in ['self']:
            field_info = field_model.field_info

            names.append(field_model.name)
            annotations[field_model.name] = field_model.outer_type_
            defaults.append(Query(field_model.default, description=field_info.description))

    code = inspect.cleandoc('''
    def %s(%s):
        try:
            return %s(%s)
        except ValidationError as e:
            errors = e.errors()
            for error in errors:
                error['loc'] = ['query'] + list(error['loc'])
            raise HTTPException(422, detail=errors)

    ''' % (
        name, ', '.join(names), model_cls.__name__,
        ', '.join(['%s=%s' % (name, name) for name in names])))

    compiled = compile(code, 'string', 'exec')
    env = {model_cls.__name__: model_cls}
    env.update(**globals())
    func = FunctionType(compiled.co_consts[0], env, name)
    func.__annotations__ = annotations
    func.__defaults__ = (*defaults,)

    return func

def invalid(loc, msg):
    detail = [{"loc": ["path", loc], "msg": msg, "type": "Validation Error"}]
    logger.debug(f"{detail}")
    raise HTTPException(status_code=422, detail=detail)


class OBaseModel(BaseModel):
    class Config:
        min_anystr_length=1
        validate_assignment=True
        allow_population_by_field_name=True
        alias_generator = humps.decamelize






class Meta(BaseModel):
    name: str = "openaq-api"
    license: str = "CC BY 4.0d"
    website: str = f"{settings.OPENAQ_FASTAPI_URL}/docs"
    page: int = 1
    limit: int = 100
    found: int = 0


class OpenAQResult(BaseModel):
    meta: Meta
    results: Optional[List[Any]] = []





@dataclass
class BaseWhere:
    def where(self):
        name = humps.decamelize(type(self).__name__)
        if getattr(self, name) is not None:
            return f" AND {name} = ANY(:{name}) "
        return ""


@dataclass
class BaseListStr(BaseWhere):
    @validator("*", each_item=True)
    def check_nonempty(cls, v):
        name = humps.decamelize(type(cls).__name__)
        if not v:
            invalid(name, "cannot be empty")
        return str.strip(v)


@dataclass
class BaseListInt(BaseWhere):
    @validator("*", each_item=True)
    def check_nonempty(cls, v):
        name = humps.decamelize(type(cls).__name__)
        logger.debug(f"baselistint {cls, v}")
        if v is not None:
            if v > maxint:
                invalid(name, f"cannot be > {maxint}")
            if v < 0:
                invalid(name, f"cannot be < 0")
        return v

class City(OBaseModel):
    city: Optional[List[str]] = Query(None)




class Country(OBaseModel):
    country: Optional[List[str]] = Query(None, min_length=2, max_length=2)

    @validator("country", check_fields=False)
    def validate_country(cls, v):
        if v is not None:
            return [str.upper(val) for val in v]
        return None


@dataclass
class SourceName(BaseListStr):
    source_name: Optional[List[str]] = Query(None, aliases=("source", "sourceName"))


@dataclass
class Project(BaseListInt):
    project_id: Optional[int] = None
    project: Optional[List[int]] = Query(None, gt=0, le=maxint)

    @validator("project")
    def validate_project(cls, v, values):
        logger.debug(f"project validate {v} {values}")
        project = v
        pid = values.get("project_id", None)
        if pid is not None:
            project = [pid]
        if (
            project is not None
            and len(project) > 0
            and (min(project) < 0 or max(project) > maxint)
        ):
            invalid("project", f"project must be an int between 0 and {maxint}")
        return project

    def where(self):
        logger.debug(f"{asdict(self)}")
        if self.project and len(self.project) > 0:
            return " AND groups_id = ANY(:project) "
        return ""


@dataclass
class Location:
    location_id: Optional[int] = None
    location: Optional[List[Union[int, str]]] = Query(None)

    @validator("location")
    def validate_location(cls, v, values):
        ret = None
        logger.debug(f"validate location {cls} {v} {values}")
        lid = values.get("location_id", None)
        if lid is not None:
            ret = [lid]
        elif v is not None:
            ret = v
        if isinstance(ret, list):
            if all(isinstance(x, int) for x in ret):
                logger.debug("everything is an int")
                if min(ret) < 0 or max(ret) > maxint:
                    invalid("location", f"location id must be between 0 and {maxint}")
        logger.debug(f"returning {ret}")
        return ret

    def where(self):
        logger.debug(f"{asdict(self)}")
        if isinstance(self.location, list) and all(
            isinstance(x, int) for x in self.location
        ):
            return " AND id = ANY(:location) "
        elif isinstance(self.location, list):
            return " AND name = ANY(:location) "
        return ""

@dataclass
class HasGeo:
    has_geo: bool = Query(None)

    def where(self):
        if self.has_geo is not None:
            if self.has_geo:
                return " AND geog is not null "
            if not self.has_geo:
                return " AND geog is null "
        return ""

@dataclass
class Geo:
    coordinates: Optional[str] = Query(
        None, regex=r"^-?\d{1,2}\.?\d{0,8},-?1?\d{1,2}\.?\d{0,8}$"
    )
    lat: Optional[confloat(ge=-90, le=90)] = None
    lon: Optional[confloat(ge=-180, le=180)] = None
    radius: conint(gt=0, le=100000) = 1000

    @validator('lat')
    def validate_lat(cls, v, values):
        coordinates = values.get('coordinates', None)
        if coordinates is not None:
            try:
                lat, _ = values.get('coordinates').split(',')
                lat=float(lat)
                if lat >= -90 and lat <= 90:
                    return lat
                else:
                    invalid('lat','latitude is out of range')
            except Exception as e:
                invalid('lat', f"{e}")

    @validator('lon')
    def validate_lon(cls, v, values):
        coordinates = values.get('coordinates', None)
        if coordinates is not None:
            try:
                _, lon = values.get('coordinates').split(',')
                lon=float(lon)
                if lon >= -180 and lon <= 180:
                    return lon
                else:
                    invalid('lon','longitude is out of range')
            except Exception as e:
                invalid('lon', f"{e}")

    def where(self):
        if self.lat is not None and self.lon is not None:
            return " AND st_dwithin(st_makepoint(:lon, :lat)::geography, geog, :radius) "
        return ""


@dataclass
class Measurands:
    measurands: Optional[List[str]] = Query(None, aliases=('parameter','parameters',))

    def where(self):
        v = self.measurands
        if v is not None:
           return f" AND parameters @> ANY(jsonb_array(:measurands_param::jsonb)) "
        return ""

    def param(self):
        v = self.measurands
        if v is not None:
            return orjson.dumps(
                [[
                    {"measurand": m}
                    for m in v
                ]]
            ).decode()




class Paging(BaseModel):
    limit: int = Query(100, gt=0, le=10000)
    page: int = Query(1, gt=0, le=1000)
    offset: int = Query(0, ge=0, le=10000)

    @validator("offset", check_fields=False)
    def check_offset(cls, v, values, **kwargs):
        offset = values["limit"] * (values["page"] - 1)
        if offset + values["limit"] > 10000:
            invalid("limit, offset", "offset + limit must be < 10000")
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
    ASC = "ASC"
    DESC = "DESC"


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


# class Spatial(str, Enum):
#     country = "country"
#     location = "location"
#     project = "project"


# class Temporal(str, Enum):
#     day = "day"
#     month = "month"
#     year = "year"
#     moy = "moy"
#     dow = "dow"
#     hour = "hour"


# class IncludeFields(str, Enum):
#     attribution = "attribution"
#     averagingPeriod = "averagingPeriod"
#     sourceName = "sourceName"

class APIBase(Paging):
    sort: Optional[Sort] = Query("asc")

    def where(self):
        wheres=[]
        for f, v in self:
            logger.debug(f"APIBase {f} {v}")
            if isinstance(v, List):
                wheres.append(f"{f} = ANY(:{f})")
        return (' AND ').join(wheres)


# class OldPaging:
#     def __init__(
#         self,
#         limit: Optional[int] = Query(100, gt=0, le=10000),
#         page: Optional[int] = Query(1, gt=0, le=10),
#         sort: Optional[Sort] = Query("desc"),
#         order_by: Optional[Order] = Query("lastUpdated"),
#     ):
#         self.limit = limit
#         self.page = page
#         self.sort = sort
#         self.order_by = order_by

#     async def sql(self):
#         order_by = self.order_by
#         sort = str.lower(self.sort)
#         if order_by in ["count", "locations"]:
#             q = "(json->>:order_by)::int"
#         elif order_by in ["firstUpdated", "lastUpdated"]:
#             q = "(json->>:order_by)::timestamptz"
#         else:
#             q = "json->>:order_by"
#         if sort == "asc":
#             order = f" ORDER BY {q} ASC "
#         else:
#             order = f" ORDER BY {q} DESC NULLS LAST "
#         offset = (self.page - 1) * self.limit
#         return {
#             "q": f"{order} OFFSET :offset LIMIT :limit",
#             "params": {
#                 "order_by": order_by,
#                 "offset": offset,
#                 "limit": self.limit,
#             },
#         }

#     async def sql_loc(self):
#         order_by = self.order_by
#         sort = str.lower(self.sort)
#         if order_by in ["count", "measurements"]:
#             q = "measurements"
#         elif order_by in ["lastUpdated"]:
#             q = '"lastUpdated"'
#         elif order_by in ["firstUpdated"]:
#             q = '"firstUpdated"'
#         else:
#             q = f'"{order_by}"'
#         if sort == "asc":
#             order = f" ORDER BY {q} ASC "
#         else:
#             order = f" ORDER BY {q} DESC NULLS LAST "
#         offset = (self.page - 1) * self.limit
#         return {
#             "q": f"{order} OFFSET :offset LIMIT :limit",
#             "params": {
#                 "order_by": order_by,
#                 "offset": offset,
#                 "limit": self.limit,
#             },
#         }


def fix_datetime(
    d: Union[datetime, date, str, int, None], minutes_to_round_to: Optional[int] = None
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


# class MeasurementPaging:
#     def __init__(
#         self,
#         limit: Optional[int] = Query(100, gt=0, le=10000),
#         page: Optional[int] = Query(1, gt=0, le=10),
#         sort: Optional[Sort] = Query("desc"),
#         order_by: Optional[Order] = Query("datetime"),
#         date_from: Union[datetime, date, None] = None,
#         date_to: Union[datetime, date, None] = None,
#     ):
#         self.limit = limit
#         self.page = page
#         self.sort = sort
#         self.order_by = order_by

#         self.date_from = fix_datetime(date_from, 15)
#         self.date_to = fix_datetime(date_to, 15)
#         self.offset = (self.page - 1) * self.limit
#         self.totalrows = self.limit + self.offset

#         logger.debug("%s %s %s", date_from, date_to, (self.offset + limit) / 1000)

#     async def sql(self):
#         order_by = self.order_by
#         sort = self.sort
#         if sort == "asc":
#             order = f" ORDER BY {order_by} ASC "
#         else:
#             order = f" ORDER BY {order_by} DESC NULLS LAST "
#         offset = (self.page - 1) * self.limit
#         return {
#             "q": f"{order} OFFSET :offset LIMIT :limit",
#             "params": {
#                 "order_by": order_by,
#                 "offset": offset,
#                 "limit": self.limit,
#             },
#         }


# class OldGeo:
#     def __init__(
#         self,
#         coordinates: Optional[str] = Query(
#             None, regex=r"^-?1?\d{1,2}\.?\d{0,8},-?\d{1,2}\.?\d{0,8}$"
#         ),
#         radius: Optional[int] = Query(1000, gt=0, le=100000),
#     ):
#         self.radius = radius
#         self.coordinates = None
#         if coordinates is not None:
#             lon, lat = str.split(coordinates, ",")
#             self.coordinates = Coordinates(lon=lon, lat=lat)
#             point = funcs.cast(logic.Func("st_makepoint", lon, lat), "geography")
#             self.sql = logic.Func("st_dwithin", point, V("geog"), self.radius)
#         else:
#             self.sql = S(True)


# async def overlaps(field: str, param: str, val: List):
#     if val is not None:
#         q = orjson.dumps([{field: [v]} for v in val]).decode()
#         w = f"json @> ANY(jsonb_array(:{param}::jsonb))"
#         logger.debug("q: %s, w: %s", q, w)
#         return {"w": w, "q": q, "param": param}
#     return None


# async def isin(field: str, param: str, val: List):
#     if val is not None:
#         q = orjson.dumps([{field: v} for v in val]).decode()
#         w = f"json @> ANY(jsonb_array(:{param}::jsonb))"
#         logger.debug("q: %s, w: %s", q, w)
#         return {"w": w, "q": q, "param": param}
#     return None


# class Filters:
#     def __init__(
#         self,
#         project: Optional[List[str]] = Query(
#             None,
#             aliases=(
#                 "project",
#                 "projectid",
#                 "project_id",
#                 "source_name",
#             ),
#         ),
#         country: Optional[List[str]] = Query(None, max_length=2),
#         site_name: Optional[List[str]] = Query(
#             None,
#             aliases=("location",),
#         ),
#         city: Optional[List[str]] = Query(
#             None,
#         ),
#         unit: Optional[List[str]] = Query(
#             None,
#             aliases="units",
#         ),
#         measurand: Optional[List[Measurand]] = Query(
#             None,
#             aliases=("parameter"),
#         ),
#         include_fields: Optional[List[IncludeFields]] = Query(
#             None,
#         ),
#         has_geo: Optional[bool] = None,
#         geo: Geo = Depends(),
#     ):
#         self.country = country

#         self.sensor_nodes_id = None
#         self.site_name = None
#         r = re.compile(r"^\d+$")
#         if site_name is not None:
#             nodes_list = list(filter(r.match, site_name))
#             logger.debug(f"NODES: {nodes_list} len: {len(nodes_list)} {len(site_name)}")
#             if len(nodes_list) > 0 and len(nodes_list) == len(site_name):
#                 self.sensor_nodes_id = [int(s) for s in site_name]
#             else:
#                 self.site_name = site_name

#         self.city = city
#         self.measurand = measurand
#         self.coordinates = geo.coordinates
#         self.has_geo = has_geo
#         self.radius = geo.radius
#         self.include_fields = include_fields
#         self.project = project
#         self.source_name = project
#         self.units = unit

#     def get_measurement_q(self):
#         if self.measurand:
#             m_array = orjson.dumps(
#                 [
#                     {"sensor_systems": [{"sensors": [{"measurand": m}]}]}
#                     for m in self.measurand
#                 ]
#             ).decode()
#             measurand_clause = "json @> ANY(jsonb_array(:measurand::jsonb))"
#             return {"w": measurand_clause, "q": m_array, "param": "measurand"}

#     async def measurement_fields(self):
#         if self.include_fields is not None:
#             return "," + ",".join([f'"{f}"' for f in self.include_fields])
#         else:
#             return ""

#     async def sql(self):
#         jsonpath = {}
#         params = {}
#         wheres = []
#         if self.has_geo is not None:
#             jsonpath["has_geo"] = self.has_geo

#         site_names_clause = await overlaps("site_names", "site_name", self.site_name)
#         sensor_nodes_id_clause = await isin(
#             "sensor_nodes_id", "sensor_nodes_id", self.sensor_nodes_id
#         )
#         country_clause = await isin("country", "country", self.country)
#         project_clause = await isin("source_name", "project", self.project)
#         cities_clause = await overlaps("cities", "city", self.city)

#         wheres.append(self.get_measurement_q())
#         wheres.append(sensor_nodes_id_clause)
#         wheres.append(site_names_clause)
#         wheres.append(country_clause)
#         wheres.append(cities_clause)
#         wheres.append(project_clause)

#         if self.coordinates is not None:
#             lon = self.coordinates.lon
#             lat = self.coordinates.lat
#             radius = self.radius
#             w = """
#                 st_dwithin(
#                     st_makepoint(:lon,:lat)::geography,
#                     geog,
#                     :radius::int
#                 )
#                 """

#             wheres.append({"w": w})
#             params["lon"] = lon
#             params["lat"] = lat
#             params["radius"] = radius

#         if jsonpath != {}:
#             wheres.append(
#                 {
#                     "w": "json @> :jsonpath",
#                     "q": orjson.dumps(jsonpath).decode(),
#                     "param": "jsonpath",
#                 }
#             )

#         where_stmts = [w["w"] for w in wheres if w is not None]
#         logger.debug("wheres: %s", where_stmts)

#         sql = " AND ".join(where_stmts)

#         for w in wheres:
#             if w is not None and w.get("param", None) is not None:
#                 logger.debug(f"{w} {w['param']} {w['q']}")
#                 params[w["param"]] = w["q"]
#         if sql == "":
#             sql = " TRUE "

#         return {"q": sql, "params": params}


# class MeasurementFilters(Filters):
#     def get_measurement_q(self):
#         if self.measurand:
#             measurand_clause = "measurand = ANY(:measurand)"
#             return {"w": measurand_clause, "q": self.measurand, "param": "measurand"}

#     async def sql(self):
#         wheres = []
#         params = {}
#         for p in [
#             "city",
#             "country",
#             "measurand",
#             "site_name",
#             "sensor_nodes_id",
#             "source_name",
#         ]:
#             vals = getattr(self, p)
#             if vals is not None:
#                 wheres.append(
#                     {"w": f"{p} = ANY(:{p})", "q": getattr(self, p), "param": p}
#                 )

#         if self.coordinates is not None:
#             lon = self.coordinates.lon
#             lat = self.coordinates.lat
#             radius = self.radius
#             w = """
#                 st_dwithin(
#                     st_makepoint(:lon,:lat)::geography,
#                     geog,
#                     :radius::int
#                 )
#                 """

#             wheres.append({"w": w})
#             params["lon"] = lon
#             params["lat"] = lat
#             params["radius"] = radius

#         where_stmts = [w["w"] for w in wheres if w is not None]
#         logger.debug("wheres: %s", where_stmts)

#         sql = " AND ".join(where_stmts)

#         for w in wheres:
#             if w is not None and w.get("param", None) is not None:
#                 logger.debug(f"{w} {w['param']} {w['q']}")
#                 params[w["param"]] = w["q"]
#         if sql == "":
#             sql = " TRUE "

#         return {"q": sql, "params": params}


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


class DB:
    def __init__(self, request: Request):
        self.pool = request.app.state.pool

    @cached(900, **cache_config)
    async def fetch(self, query, kwargs):
        start = time.time()
        logger.debug("Start time: %s Query: %s Args:%s", start, query, kwargs)
        rquery, args = render(query, **kwargs)
        try:
            r = await self.pool.fetch(rquery, *args)
        except asyncpg.exceptions.UndefinedColumnError as e:
            invalid("database", f"{e}")
        except asyncpg.exceptions.DataError as e:
            invalid("database", f"{e}")
        except asyncpg.exceptions.CharacterNotInRepertoireError as e:
            invalid("database", f"{e}")
        logger.debug("query: %s, args: %s, took: %s", rquery, args, time.time() - start)
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
            results = [orjson.loads(r[1]) for r in rows]

        meta = Meta(
            page=kwargs["page"],
            limit=kwargs["limit"],
            found=found,
        )

        output = OpenAQResult(meta=meta, results=results)
        return output
