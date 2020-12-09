import logging
import time
from dataclasses import asdict
from enum import Enum
from typing import List, Optional

import orjson as json
from asyncpg.exceptions import FunctionExecutedNoReturnStatementError
from fastapi import APIRouter, Depends, Path, Query
from pydantic.json import pydantic_encoder
from pydantic.typing import Literal
from pydantic.dataclasses import dataclass

from .base import (DB, City, Country, Location, Meta, OpenAQResult,
                   Paging, Project, Sort, SourceName, parameter_dependency_from_model,
                   maxint, HasGeo, Geo, Measurands, APIBase)

logger = logging.getLogger("locations")
logger.setLevel(logging.DEBUG)

router = APIRouter()


class Cities(City, Country, APIBase):
    order_by: Literal["city", "country", "firstUpdated", "lastUpdated"] = Query("city")

cities_depends = parameter_dependency_from_model('depends', Cities)

@router.get("/v1/cities", response_model=OpenAQResult)
@router.get("/v2/cities", response_model=OpenAQResult)
async def cities_get(
    db: DB = Depends(),
    cities: Cities = Depends(cities_depends)
):
    q = f"""
    WITH t AS (
    SELECT
        country,
        city,
        sum(value_count) as count,
        count(*) as locations,
        to_char(min(first_datetime),'YYYY-MM-DD') as "firstUpdated",
        to_char(max(last_datetime), 'YYYY-MM-DD') as "lastUpdated",
        array_agg(DISTINCT measurand) as parameters
    FROM sensors_total
    LEFT JOIN sensors_first_last USING (sensors_id)
    LEFT JOIN sensors USING (sensors_id)
    LEFT JOIN sensor_systems USING (sensor_systems_id)
    LEFT JOIN sensor_nodes USING (sensor_nodes_id)
    LEFT JOIN sensor_nodes_json USING (sensor_nodes_id)
    LEFT JOIN measurands USING (measurands_id)
    WHERE
    {cities.where()}
    GROUP BY
    country,
    city
    ORDER BY "{cities.order_by}" {cities.sort}
    OFFSET :offset
    LIMIT :limit
    )
    SELECT count(*) OVER () as count, row_to_json(t) as json FROM t

    """

    output = await db.fetchOpenAQResult(q, cities.dict())

    return output


class Countries(Country, APIBase):
    order_by: Literal["country", "firstUpdated", "lastUpdated"] = Query("country")

countries_depends = parameter_dependency_from_model('depends', Countries)

@router.get("/v1/countries", response_model=OpenAQResult)
@router.get("/v2/countries", response_model=OpenAQResult)
async def countries_get(
    db: DB = Depends(),
    countries: Countries = Depends(countries_depends),
):

    q = f"""
    WITH t AS (
    SELECT
        country as code,
        name,
        sum(value_count) as count,
        count(*) as locations,
        to_char(min(first_datetime),'YYYY-MM-DD') as "firstUpdated",
        to_char(max(last_datetime), 'YYYY-MM-DD') as "lastUpdated",
        array_agg(DISTINCT measurand) as parameters
    FROM sensors_total
    LEFT JOIN sensors_first_last USING (sensors_id)
    LEFT JOIN sensors USING (sensors_id)
    LEFT JOIN sensor_systems USING (sensor_systems_id)
    LEFT JOIN sensor_nodes USING (sensor_nodes_id)
    LEFT JOIN sensor_nodes_json USING (sensor_nodes_id)
    LEFT JOIN measurands USING (measurands_id)
    LEFT JOIN countries ON (country=iso_a2)
    WHERE
    {countries.where()}
    GROUP BY
    1,2
    ORDER BY "{countries.order_by}" {countries.sort}
    OFFSET :offset
    LIMIT :limit
    )
    SELECT count(*) OVER () as count, row_to_json(t) as json FROM t

    """

    output = await db.fetchOpenAQResult(q, countries.dict())

    return output

class Sources(SourceName, APIBase):
    order_by: Literal["sourceName", "firstUpdated", "lastUpdated"] = Query(
        "sourceName"
    )

sources_depends = parameter_dependency_from_model('depends', Sources)
@router.get("/v1/sources", response_model=OpenAQResult)
@router.get("/v2/sources", response_model=OpenAQResult)
async def sources_get(
    db: DB = Depends(),
    sources: Sources = Depends(),
):

    q = f"""
    WITH t AS (
    SELECT
        source_name as "sourceName",
        data::jsonb as data,
        sum(value_count) as count,
        count(*) as locations,
        to_char(min(first_datetime),'YYYY-MM-DD') as "firstUpdated",
        to_char(max(last_datetime), 'YYYY-MM-DD') as "lastUpdated",
        array_agg(DISTINCT measurand) as parameters
    FROM sources
    LEFT JOIN sensor_nodes USING (source_name)
    LEFT JOIN sensor_nodes_json USING (sensor_nodes_id)
    LEFT JOIN sensor_systems USING (sensor_nodes_id)
    LEFT JOIN sensors USING (sensor_systems_id)
    LEFT JOIN sensors_total USING (sensors_id)
    LEFT JOIN sensors_first_last USING (sensors_id)
    LEFT JOIN measurands USING (measurands_id)
    WHERE
    {sources.where()}
    GROUP BY
    1,2
    ORDER BY "{sources.order_by}" {sources.sort}
    OFFSET :offset
    LIMIT :limit
    )
    SELECT count(*) OVER () as count,
        (coalesce(data, '{{}}'::jsonb) || jsonb_build_object(
            'count', count,
            'locations', locations,
            'firstUpdated', "firstUpdated",
            'lastUpdated', "lastUpdated",
            'parameters', parameters
            )) as json FROM t

    """

    output = await db.fetchOpenAQResult(q, sources.dict())

    return output


class Parameters(SourceName, APIBase):
    order_by: Literal["id", "name", "preferredUnit"] = Query("id")


parameters_depends = parameter_dependency_from_model('depends', Parameters)

@router.get("/v1/parameters", response_model=OpenAQResult)
@router.get("/v2/parameters", response_model=OpenAQResult)
async def parameters_get(
    db: DB = Depends(),
    parameters: Parameters = Depends(parameters_depends),
):

    q = f"""
    WITH t AS (
    SELECT
        measurand as id,
        upper(measurand) as name,
        upper(measurand) as description,
        units as "preferredUnit"
    FROM measurands
    ORDER BY "{parameters.order_by}" {parameters.sort}
    )
    SELECT count(*) OVER () as count, row_to_json(t) as json FROM t
    LIMIT :limit
    OFFSET :offset
    """

    output = await db.fetchOpenAQResult(q, parameters.dict())

    return output

@router.get("/v2/projects/{project_id}", response_model=OpenAQResult)
@router.get("/v2/projects", response_model=OpenAQResult)
async def projects_get(
    db: DB = Depends(),
    project: Project = Depends(),
    group_type: Literal["source"] = Query("source", aliases=("groupType",)),
    order_by: Literal["id", "name", "subtitle", "firstUpdated", "lastUpdated"] = Query(
        "id"
    ),
    sort: Optional[Sort] = Query("asc"),
    paging: Paging = Depends(),
):

    params = {
        "project": project.project,
        "group_type": group_type,
        "limit": paging.limit,
        "offset": paging.offset,
        "page": paging.page,
    }

    where_sql = " type=:group_type AND rollup='total' "
    where_sql += project.where()

    q = f"""
        WITH t AS (
        SELECT
            groups_id as "id",
            name,
            subtitle,
            sum(value_count) as count,
            min("firstUpdated") as "firstUpdated",
            max("lastUpdated") as "lastUpdated",
            count(*) as locations,
            array_agg(DISTINCT measurand) as parameters
        FROM rollups
        LEFT JOIN groups USING (groups_id)
        LEFT JOIN measurands USING (measurands_id)
        LEFT JOIN LATERAL (
            SELECT
                min(first_datetime) as "firstUpdated",
                max(last_datetime) as "lastUpdated"
            FROM groups_sensors g, sensors_first_last sfl
            WHERE
                g.groups_id = rollups.groups_id
                AND
                sfl.sensors_id = g.sensors_id
            ) as fl ON TRUE
        WHERE
        {where_sql}
        GROUP BY
        1,2,3
        ORDER BY "{order_by}" {sort}

        )
        SELECT count(*) OVER () as count, row_to_json(t) as json FROM t
        LIMIT :limit
        OFFSET :offset
    """

    output = await db.fetchOpenAQResult(q, params)

    return output


@router.get("/v1/locations/{location_id}", response_model=OpenAQResult)
@router.get("/v2/locations/{location_id}", response_model=OpenAQResult)
@router.get("/v1/locations", response_model=OpenAQResult)
@router.get("/v2/locations", response_model=OpenAQResult)
async def locations_get(
    db: DB = Depends(),
    city: City = Depends(),
    country: Country=Depends(),
    location: Location=Depends(),
    geo: Geo=Depends(),
    measurands: Measurands=Depends(),
    has_geo: HasGeo=Depends(),
    order_by: Literal[
        "city", "country", "location", "sourceName", "firstUpdated", "lastUpdated"
        ] = Query(
        "id"
    ),
    sort: Optional[Sort] = Query("asc"),
    paging: Paging = Depends(),
):
    where_sql = ' TRUE '
    where_sql += location.where()
    where_sql += country.where()
    where_sql += city.where()
    where_sql += has_geo.where()
    where_sql += geo.where()
    where_sql += measurands.where()

    params = {
        "city": city.city,
        "country": country.country,
        "location": location.location,
        "limit": paging.limit,
        "offset": paging.offset,
        "page": paging.page,
        "lat": geo.lat,
        "lon": geo.lon,
        "radius": geo.radius,
        "measurands_param": measurands.param(),
    }

    q = f"""
        WITH t1 AS (
            SELECT *
            FROM locations_base_v2
            WHERE
            {where_sql}
            ORDER BY "{order_by}" {sort}
        ),
        nodes AS (
            SELECT count(distinct id) as nodes
            FROM locations_base_v2
            WHERE
            {where_sql}
        ),
        t2 AS (
        SELECT to_jsonb(t1) - '{{json,source_name,geog}}'::text[] as json
        FROM t1 group by t1, json
        )
        SELECT nodes as count, json FROM t2, nodes
        LIMIT :limit
        OFFSET :offset
        ;
        """

    output = await db.fetchOpenAQResult(q, params)

    return output
