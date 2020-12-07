import logging
import time
from typing import Optional

import orjson as json
from fastapi import APIRouter, Depends, Query

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

@router.get("/cities")
async def cities_get(
    db: DB = Depends(),
    city: Optional[List[str]] = Query(None,),
    country:Optional[List[str]] = Query(None, max_length=2),
    order_by: Optional[CitiesOrder] = Query("city"),
    sort: Optional[Sort] = Query("asc"),
    limit: Optional[int] = Query(100, gt=0, le=10000),
    page: Optional[int] = Query(1, gt=0, le=100),
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

@router.get("/countries")
async def countries_get(
    db: DB = Depends(),
    country:Optional[List[str]] = Query(None, max_length=2),
    order_by: Optional[CountriesOrder] = Query("country"),
    sort: Optional[Sort] = Query("asc"),
    limit: Optional[int] = Query(100, gt=0, le=10000),
    page: Optional[int] = Query(1, gt=0, le=100),
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
    source="sourceName"
    firstUpdated= "firstUpdated"
    lastUpdated = "lastUpdated"

@router.get("/sources")
async def sources_get(
    db: DB = Depends(),
    source_name:Optional[List[str]] = Query(
        None,
        aliases=('source', 'sourceName'),
    ),
    order_by: Optional[SourcesOrder] = Query("sourceName"),
    sort: Optional[Sort] = Query("asc"),
    limit: Optional[int] = Query(100, gt=0, le=10000),
    page: Optional[int] = Query(1, gt=0, le=100),
):
    offset = (page - 1) * limit
    where_sql = " TRUE "

    if source_name is not None:
        where_sql += " And country = ANY(:country) "

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





@router.get("/sources")
async def sources_get(
    db: DB = Depends(),
    paging: Paging = Depends(),
    filters: Filters = Depends()
):
        paging_q = await paging.sql()
        where_q = await filters.sql()
        params = {}
        params.update(paging_q["params"])
        params.update(where_q["params"])

        where_sql = where_q["q"]
        paging_sql = paging_q["q"]

        q = f"""
        WITH t AS (
        SELECT
            source_name,
            first(data)::jsonb as data,
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
        1

        ), t1 AS (
        SELECT count(*) OVER () as count,
        (coalesce(data, '{{}}'::jsonb) || jsonb_build_object(
            'count', count,
            'locations', locations,
            'firstUpdated', "firstUpdated",
            'lastUpdated', "lastUpdated",
            'parameters', parameters
            )) as json FROM t
        ) select * from t1
        {paging_sql}

        """

        meta = {
            "name": "openaq-api",
            "license": "CC BY 4.0",
            "website": "https://docs.openaq.org/",
            "page": paging.page,
            "limit": paging.limit,
            "found": None,
        }
        rows = await db.fetch(q, params)
        if len(rows) == 0:
            meta["found"] = 0
            return {"meta": meta, "results": []}
        meta["found"] = rows[0]["count"]
        logger.debug(f'{rows}')
        json_rows = [json.loads(r[1]) for r in rows]

        return {"meta": meta, "results": json_rows}


@router.get("/parameters")
async def parameters_get(
    db: DB = Depends(),
    paging: Paging = Depends(),
    filters: Filters = Depends()
):
        paging_q = await paging.sql()
        where_q = await filters.sql()
        params = {}
        params.update(paging_q["params"])
        params.update(where_q["params"])

        where_sql = where_q["q"]
        paging_sql = paging_q["q"]

        q = f"""
        WITH t AS (
        SELECT
            measurand as id,
            upper(measurand) as name,
            upper(measurand) as description,
            units as "preferredUnit"
        FROM measurands
        )
        SELECT count(*) OVER () as count, row_to_json(t) as json FROM t
        LIMIT :limit
        OFFSET :offset
        """

        meta = {
            "name": "openaq-api",
            "license": "CC BY 4.0",
            "website": "https://docs.openaq.org/",
            "page": paging.page,
            "limit": paging.limit,
            "found": None,
        }
        rows = await db.fetch(q, params)
        if len(rows) == 0:
            meta["found"] = 0
            return {"meta": meta, "results": []}
        meta["found"] = rows[0]["count"]
        json_rows = [json.loads(r[1]) for r in rows]

        return {"meta": meta, "results": json_rows}


@router.get("/projects")
async def projects_get(
    db: DB = Depends(),
    paging: Paging = Depends(),
    filters: Filters = Depends()
):
        paging_q = await paging.sql()
        where_q = await filters.sql()
        params = {}
        params.update(paging_q["params"])
        params.update(where_q["params"])

        where_sql = where_q["q"]
        paging_sql = paging_q["q"]

        q = f"""
        WITH t AS (
        SELECT
            source_name as "id",
            source_name as "name",
            coalesce(sources.data->>'name', n.metadata->>'sensor_node_source_fullname') as subtitle,
            sum(value_count) as count,
            to_char(min(first_datetime),'YYYY-MM-DD') as "firstUpdated",
            to_char(max(last_datetime), 'YYYY-MM-DD') as "lastUpdated",
            count(*) as locations,
            array_agg(DISTINCT measurand) as parameters
        FROM sensors_total
        LEFT JOIN sensors_first_last USING (sensors_id)
        LEFT JOIN sensors USING (sensors_id)
        LEFT JOIN sensor_systems USING (sensor_systems_id)
        LEFT JOIN sensor_nodes n USING (sensor_nodes_id)
        LEFT JOIN sensor_nodes_json USING (sensor_nodes_id)
        LEFT JOIN measurands USING (measurands_id)
        LEFT JOIN sources USING(source_name)
        WHERE
        {where_sql}
        GROUP BY
        source_name,
        sources.data->>'name',
        n.metadata->>'sensor_node_source_fullname'
        ORDER BY source_name

        ), t1 AS
        (
        SELECT count(*) OVER () as count, row_to_json(t) as json FROM t
        ) SELECT * FROM t1
        {paging_sql}

        """

        meta = {
            "name": "openaq-api",
            "license": "CC BY 4.0",
            "website": "https://docs.openaq.org/",
            "page": paging.page,
            "limit": paging.limit,
            "found": None,
        }
        rows = await db.fetch(q, params)
        if len(rows) == 0:
            meta["found"] = 0
            return {"meta": meta, "results": []}
        meta["found"] = rows[0]["count"]
        json_rows = [json.loads(r[1]) for r in rows]

        return {"meta": meta, "results": json_rows}


@router.get("/projects/{project_id}")
async def projects_get(
    project_id: str,
    db: DB = Depends(),
):

        params = {'project_id':project_id}
        q = """
        WITH base AS (
            SELECT *,
            n.geom as sgeom,
            coalesce(sources.data->>'name', n.metadata->>'sensor_node_source_fullname') as subtitle
            FROM
            sensors_total
            LEFT JOIN sensors_first_last USING (sensors_id)
            LEFT JOIN sensors USING (sensors_id)
            LEFT JOIN sensor_systems USING (sensor_systems_id)
            LEFT JOIN sensor_nodes n USING (sensor_nodes_id)
            LEFT JOIN sensor_nodes_json USING (sensor_nodes_id)
            LEFT JOIN measurands USING (measurands_id)
            LEFT JOIN sources USING (source_name)
            WHERE
            source_name=:project_id
        ),
        overall AS (
        SELECT
            source_name as "id",
            source_name as "name",
            subtitle,
            sum(value_count) as measurements,
            count(*) as locations,
            to_jsonb(st_collect(distinct sgeom)) as points,
            to_char(min(first_datetime),'YYYY-MM-DD') as "firstUpdated",
            to_char(max(last_datetime), 'YYYY-MM-DD') as "lastUpdated"
        FROM base
        group by 1,2,3
        ),
        byparameter AS (
            SELECT
                measurand,
                units as unit,
                sum(value_count) as count,
                sum(value_sum) / sum(value_count) as average,
                to_char(min(first_datetime),'YYYY-MM-DD') as "firstUpdated",
                to_char(max(last_datetime), 'YYYY-MM-DD') as "lastUpdated",
                last(last_value, last_datetime) as "lastValue",
                count(*) as locations
            FROM
            base
            GROUP BY measurand, units
        ),
        sources AS (
            SELECT jsonb_agg(data) as sources FROM sources WHERE source_name IN (SELECT distinct source_name FROM base)
        ), t1 AS
        (
        SELECT
            overall.*,
            jsonb_agg(to_jsonb(byparameter)) as parameters,
            sources
        FROM overall, byparameter, sources
        GROUP BY 1,2,3,4,5,6,7,8, sources
        ) SELECT to_jsonb(t1) as json FROM t1 group by t1

        """

        meta = {
            "name": "openaq-api",
            "license": "CC BY 4.0",
            "website": "https://docs.openaq.org/"
        }
        rows = await db.fetch(q, params)
        if len(rows) == 0:
            meta["found"] = 0
            return {"meta": meta, "results": []}
        # meta["found"] = rows[0]["count"]
        json_rows = [json.loads(r['json']) for r in rows]

        return {"meta": meta, "results": json_rows}

@router.get("/locations/{location_id}")
@router.get("/v2/locations/{location_id}")
@router.get("/v2/locations")
async def locations_get(
    db: DB = Depends(),
    paging: Paging = Depends(Paging),
    filters: Filters = Depends(Filters),
    location_id: Optional[int] = None,
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

    oq = f"""
        WITH base AS (
            SELECT
                *,
                sensor_nodes.geom as sgeom
            FROM
            sensors_total
            LEFT JOIN sensors_first_last USING (sensors_id)
            LEFT JOIN sensors USING (sensors_id)
            LEFT JOIN sensor_systems USING (sensor_systems_id)
            LEFT JOIN sensor_nodes USING (sensor_nodes_id)
            LEFT JOIN sensor_nodes_json USING (sensor_nodes_id)
            LEFT JOIN measurands USING (measurands_id)
            WHERE
            {where_sql}
            {location_sql}
        ),
        nsensors AS (
            select count(*) FROM base GROUP BY sensor_nodes_id
            ),
        overall AS (
        SELECT
            sensor_nodes_id as "id",
            site_name as "name",
            json->>'source_type' as "sourceType",
            ismobile as "isMobile",
            city,
            country,
            source_name,
            jsonb_build_object(
                'longitude', st_x(sgeom),
                'latitude', st_y(sgeom)
            ) as coordinates,
            sum(value_count) as measurements,
            min(first_datetime) as "firstUpdated",
            max(last_datetime) as "lastUpdated",
            json
        FROM base
        group by id, name,city,country,coordinates,json,source_name,"sourceType","isMobile"
        {paging_sql}
        ),
        byparameter AS (
            SELECT
                sensors_id as id,
                sensor_nodes_id,
                measurand,
                units as unit,
                value_count as count,
                value_sum / value_count as average,
                first_datetime as "firstUpdated",
                last_datetime as "lastUpdated",
                last_value as "lastValue"
            FROM
            base
        ),
        t1 AS (
        SELECT
            overall.*,
            sources.data::jsonb as sources,
            jsonb_agg(to_jsonb(byparameter) - '{{sensor_nodes_id}}'::text[]) as parameters
        FROM overall
        LEFT JOIN sources USING (source_name)
        LEFT JOIN byparameter ON (overall.id=sensor_nodes_id)
        GROUP BY
            overall.id,
            name,
            city,
            country,
            coordinates,
            overall."firstUpdated",
            overall."lastUpdated",
            "sourceType",
            "isMobile",
            overall.source_name,
            measurements,
            json,
            sources.data::jsonb

        ), t2 AS (
        SELECT to_jsonb(t1) - '{{json,nsensors,source_name}}'::text[] as json
        FROM t1 group by t1, json
        )
        SELECT *, count FROM t2, nsensors
        ;
    """

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
