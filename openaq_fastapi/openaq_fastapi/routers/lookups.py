import logging
import time
from typing import Optional

import orjson as json
from fastapi import APIRouter, Depends, Query, Path

from .base import DB, Filters, Paging, Spatial, Temporal, Sort
from enum import Enum
from typing import List, Optional

logger = logging.getLogger("locations")
logger.setLevel(logging.DEBUG)

router = APIRouter()


class CitiesOrder(str, Enum):
    city="city"
    country="country"
    count="count"
    locations="locations"
    firstUpdated= "firstUpdated"
    lastUpdated = "lastUpdated"

@router.get("/v1/cities")
@router.get("/v2/cities")
async def cities_get(
    db: DB = Depends(),
    city: Optional[List[str]] = Query(None,),
    country:Optional[List[str]] = Query(None, max_length=2),
    order_by: Optional[CitiesOrder] = Query("city"),
    sort: Optional[Sort] = Query("asc"),
    limit: Optional[int] = Query(100, gt=0, le=10000),
    page: Optional[int] = Query(1, gt=0, le=10),
):
    offset = (page - 1) * limit
    where_sql = " TRUE "
    if city is not None:
        where_sql += " AND city = ANY(:city) "
    if country is not None:
        where_sql += " And country = ANY(:country) "

    params={
        "city": city,
        "country": country,
        "limit": limit,
        "offset": offset,
    }

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
    {where_sql}
    GROUP BY
    country,
    city
    ORDER BY "{order_by}" {sort}
    OFFSET :offset
    LIMIT :limit
    )
    SELECT count(*) OVER () as count, row_to_json(t) as json FROM t

    """

    meta = {
        "name": "openaq-api",
        "license": "CC BY 4.0",
        "website": "https://docs.openaq.org/",
        "page": page,
        "limit": limit,
        "found": None,
    }
    rows = await db.fetch(q, params)
    if len(rows) == 0:
        meta["found"] = 0
        return {"meta": meta, "results": []}
    meta["found"] = rows[0]["count"]
    json_rows = [json.loads(r[1]) for r in rows]

    return {"meta": meta, "results": json_rows}

class CountriesOrder(str, Enum):
    country="country"
    firstUpdated= "firstUpdated"
    lastUpdated = "lastUpdated"


@router.get("/v1/countries")
@router.get("/v2/countries")
async def countries_get(
    db: DB = Depends(),
    country:Optional[List[str]] = Query(None, max_length=2),
    order_by: Optional[CountriesOrder] = Query("country"),
    sort: Optional[Sort] = Query("asc"),
    limit: Optional[int] = Query(100, gt=0, le=10000),
    page: Optional[int] = Query(1, gt=0, le=10),
):
    offset = (page - 1) * limit
    where_sql = " TRUE "

    if country is not None:
        where_sql += " And country = ANY(:country) "

    params={
        "country": country,
        "limit": limit,
        "offset": offset,
    }

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
    {where_sql}
    GROUP BY
    1,2
    ORDER BY "{order_by}" {sort}
    OFFSET :offset
    LIMIT :limit
    )
    SELECT count(*) OVER () as count, row_to_json(t) as json FROM t

    """

    meta = {
        "name": "openaq-api",
        "license": "CC BY 4.0",
        "website": "https://docs.openaq.org/",
        "page": page,
        "limit": limit,
        "found": None,
    }
    rows = await db.fetch(q, params)
    if len(rows) == 0:
        meta["found"] = 0
        return {"meta": meta, "results": []}
    meta["found"] = rows[0]["count"]
    json_rows = [json.loads(r[1]) for r in rows]

    return {"meta": meta, "results": json_rows}



class SourcesOrder(str, Enum):
    sourceName="sourceName"
    firstUpdated="firstUpdated"
    lastUpdated="lastUpdated"

@router.get("/v1/sources")
@router.get("/v2/sources")
async def sources_get(
    db: DB = Depends(),
    source_name:Optional[List[str]] = Query(
        None,
        aliases=('source', 'sourceName'),
        min_length=3
    ),
    order_by: Optional[SourcesOrder] = Query("sourceName"),
    sort: Optional[Sort] = Query("asc"),
    limit: Optional[int] = Query(100, gt=0, le=10000),
    page: Optional[int] = Query(1, gt=0, le=10),
):
    offset = (page - 1) * limit
    where_sql = " TRUE "

    if source_name is not None:
        where_sql += " AND source_name = ANY(:source_name) "

    params={
        "source_name": source_name,
        "limit": limit,
        "offset": offset,
    }

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
    {where_sql}
    GROUP BY
    1,2
    ORDER BY "{order_by}" {sort}
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

    meta = {
        "name": "openaq-api",
        "license": "CC BY 4.0",
        "website": "https://docs.openaq.org/",
        "page": page,
        "limit": limit,
        "found": None,
    }
    rows = await db.fetch(q, params)
    if len(rows) == 0:
        meta["found"] = 0
        return {"meta": meta, "results": []}
    meta["found"] = rows[0]["count"]
    json_rows = [json.loads(r[1]) for r in rows]

    return {"meta": meta, "results": json_rows}



class ParamsOrder(str, Enum):
    id="id"
    name="name"
    preferredUnit= "preferredUnit"
    description = "description"


@router.get("/v1/parameters")
@router.get("/v2/parameters")
async def parameters_get(
    db: DB = Depends(),
    order_by: Optional[ParamsOrder] = Query("name"),
    sort: Optional[Sort] = Query("asc"),
    limit: Optional[int] = Query(100, gt=0, le=10000),
    page: Optional[int] = Query(1, gt=0, le=10),
):
    offset = (page - 1) * limit

    params={
        "limit": limit,
        "offset": offset,
    }

    q = f"""
    WITH t AS (
    SELECT
        measurand as id,
        upper(measurand) as name,
        upper(measurand) as description,
        units as "preferredUnit"
    FROM measurands
    ORDER BY "{order_by}" {sort}
    )
    SELECT count(*) OVER () as count, row_to_json(t) as json FROM t
    LIMIT :limit
    OFFSET :offset
    """

    meta = {
        "name": "openaq-api",
        "license": "CC BY 4.0",
        "website": "https://docs.openaq.org/",
        "page": page,
        "limit": limit,
        "found": None,
    }
    rows = await db.fetch(q, params)
    if len(rows) == 0:
        meta["found"] = 0
        return {"meta": meta, "results": []}
    meta["found"] = rows[0]["count"]
    json_rows = [json.loads(r[1]) for r in rows]

    return {"meta": meta, "results": json_rows}


class GroupTypes(str, Enum):
    source = "source"


class GroupOrder(str, Enum):
    id = "id"
    name = "name"
    subtitle = "subtitle"
    firstUpdated= "firstUpdated"
    lastUpdated = "lastUpdated"


@router.get("/v2/projects")
@router.get("/v2/projects/{project_id}")
async def projects_get(
    db: DB = Depends(),
    project_id: Optional[int] = Path(None, gt=0, le=9999999),
    project: Optional[List[int]] = Query(
        None, gt=0, le=9999999
    ),
    group_type: Optional[GroupTypes] = Query('source', aliases=('groupType',)),
    order_by: Optional[GroupOrder] = Query("id"),
    sort: Optional[Sort] = Query("asc"),
    limit: Optional[int] = Query(100, gt=0, le=10000),
    page: Optional[int] = Query(1, gt=0, le=10),
):
    offset = (page - 1) * limit
    if project is None:
        project = project_id

    where_sql = " type=:group_type AND rollup='total' "

    if project_id is not None:
        project = [project_id]

    if project is not None:
        where_sql += " AND groups_id = ANY(:project) "


    params={
        "group_type": group_type,
        "limit": limit,
        "offset": offset,
        "project": project,
    }


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
    """

    meta = {
        "name": "openaq-api",
        "license": "CC BY 4.0",
        "website": "https://docs.openaq.org/",
        "page": page,
        "limit": limit,
        "found": None,
    }
    rows = await db.fetch(q, params)
    if len(rows) == 0:
        meta["found"] = 0
        return {"meta": meta, "results": []}
    meta["found"] = rows[0]["count"]
    json_rows = [json.loads(r[1]) for r in rows]

    return {"meta": meta, "results": json_rows}




@router.get("/v1/locations/{location_id}")
@router.get("/v2/locations/{location_id}")
@router.get("/v1/locations")
@router.get("/v2/locations")
async def locations_get(
    db: DB = Depends(),
    paging: Paging = Depends(Paging),
    filters: Filters = Depends(Filters),
    location_id: Optional[int] = Path(None, gt=0, le=9999999)
):
    logger.debug(f"{paging} {filters}")
    paging_q = await paging.sql_loc()
    where_q = await filters.sql()
    params = {
        'sensor_nodes_id': location_id
    }
    location_sql = ''
    if location_id is not None:
        location_sql = ' AND id=:sensor_nodes_id '
    params.update(paging_q["params"])
    params.update(where_q["params"])

    where_sql = where_q["q"]
    paging_sql = paging_q["q"]

    q = f"""
        WITH t1 AS (
            SELECT *
            FROM locations_base_v2
            WHERE
            {where_sql}
            {location_sql}
            {paging_sql}
        ),
        nodes AS (
            SELECT count(distinct id) as nodes
            FROM locations_base_v2
            WHERE
            {where_sql}
            {location_sql}
        ),
        t2 AS (
        SELECT to_jsonb(t1) - '{{json,source_name}}'::text[] as json
        FROM t1 group by t1, json
        )
        SELECT t2.*, nodes as count FROM t2, nodes

        ;
        """

    meta = {
        "name": "openaq-api",
        "license": "CC BY 4.0",
        "website": "https://docs.openaq.org/",
        "found": 0
    }
    rows = await db.fetch(q, params)
    if len(rows) == 0:
        meta["found"] = 0
        return {"meta": meta, "results": []}
    meta["found"] = rows[0]["count"]
    json_rows = [json.loads(r['json']) for r in rows]

    return {"meta": meta, "results": json_rows}
