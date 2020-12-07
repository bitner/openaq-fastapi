import logging
import time
import re
from typing import Optional

import orjson as json
from fastapi import APIRouter, Depends, Query
from datetime import timedelta
from dateutil.tz import UTC

from .base import DB, MeasurementFilters, MeasurementPaging, Spatial, Temporal

logger = logging.getLogger("locations")
logger.setLevel(logging.DEBUG)

router = APIRouter()


@router.get("/averages")
async def averages_get(
    db: DB = Depends(),
    paging: MeasurementPaging = Depends(),
    filters: MeasurementFilters = Depends(),
    spatial: Spatial = Query(...),
    temporal: Temporal = Query(...),
):
    start = time.time()
    paging_q = await paging.sql()
    where_q = await filters.sql()
    date_from = paging.date_from
    date_to = paging.date_to

    params = {}
    params.update(paging_q["params"])
    params.update(where_q["params"])

    params.update( {
        "date_to": date_to,
        "date_from": date_from,
    })

    where_sql = where_q["q"]
    paging_sql = paging_q["q"]

    summary_q = f"""
        SELECT
            sum(value_count) as count,
            min(first_datetime) as first,
            max(last_datetime) as last
        FROM sensors_total
        LEFT JOIN sensors_first_last USING (sensors_id)
        LEFT JOIN sensors USING (sensors_id)
        LEFT JOIN sensor_systems USING (sensor_systems_id)
        LEFT JOIN sensor_nodes USING (sensor_nodes_id)
        LEFT JOIN sensor_nodes_json USING (sensor_nodes_id)
        LEFT JOIN measurands USING (measurands_id)
        WHERE {where_sql}
        """
    summary = await db.fetchrow(summary_q, params)
    if summary is None or summary[0] is None:
        return {"error": "no sensors found matching search"}
    logger.debug('total_count %s, min %s, max %s', summary[0], summary[1], summary[2])
    total_count = summary[0]
    range_start = summary[1].replace(tzinfo=UTC)
    range_end = summary[2].replace(tzinfo=UTC)

    if date_from is None:
        date_from = range_start
    else:
        date_from = max(date_from, range_start)

    if date_to is None:
        date_to = range_end
    else:
        date_to = min(date_to, range_end)


    params.update( {
        "date_to": date_to,
        "date_from": date_from,
    })

    if spatial == 'project':
       spatial='source_name'

    count_q = 'value_count'
    sum_q = 'value_sum'


    temporal_col = temporal_q = temporal_order = f"{temporal}::date"
    if temporal == "day":
        table = 'sensors_daily'
    elif temporal == "month":
        table = 'sensors_monthly'
    elif temporal == "year":
        table = 'sensors_yearly'
    elif temporal == "moy":
        table = 'sensors_monthly'
        temporal_order = "to_char(month, 'MM')"
        temporal_col = "to_char(month, 'Mon')"
        temporal_q = "month"
    elif temporal == "dow":
        table = 'sensors_daily'
        temporal_col = "to_char(day, 'Dy')"
        temporal_order = "to_char(day, 'ID')"
        temporal_q = "day"
    elif temporal == "hour":
        table = "measurements"
        temporal_q = "datetime"
        temporal_order = temporal_col = "date_trunc('hour', datetime)"
        count_q = '1::int as value_count'
        sum_q = 'value as value_sum'

    else:
        raise Exception




    base_obj = [
        "'parameter'", "measurand",
        f"'{temporal}'", "d",
        "'measurement_count'", "measurement_count",
        "'average'", "average",
        "'unit'", "units"
    ]
    cols=[]
    if spatial in ['country', 'city', 'location']:
        base_obj.extend([
            "'country'", "country"
        ])
        cols.append('country')

    if spatial == 'source_name':
        base_obj.extend([
            "'project'", "source_name"
        ])
        cols.append('source_name')

    if spatial in ['city', 'location']:
        base_obj.extend([
            "'city'", "city"
        ])
        cols.append('city')

    if spatial == 'location':
        spatial = "site_name"
        cols.extend(["site_name", "lat", "lon"])
        base_obj.extend([
            "'location'", "site_name",
            "'coordinates'",
            "json_build_object('longitude', lon,'latitude', lat)"
        ])
    logger.debug(f"base_obj: {base_obj}")
    logger.debug(f"cols: {cols}")
    base = ','.join(base_obj)
    spatial_sql = ','.join(cols)

    final_query = f"""
        SELECT
            total,
            json_build_object({base}) as json
        FROM t
        """

    q = f"""
        WITH base AS (
            SELECT
                {temporal_col} as d,
                {temporal_order} as o,
                measurand,
                units,
                {spatial_sql},
                {count_q},
                {sum_q}
            FROM
                measurements_web_base b
                LEFT JOIN {table} t USING (sensors_id)
            WHERE
                {temporal_q} >= :date_from::timestamptz
                AND
                {temporal_q} <= :date_to::timestamptz
                AND
                {where_sql}
        ), t as (
        SELECT
            d,
            o,
            measurand,
            units,
            {spatial_sql},
            count(*) over () as total,
            sum(value_count) as measurement_count,
            sum(value_sum) / sum(value_count) as average
        FROM base
        GROUP BY
            d,
            o,
            measurand,
            units,
            {spatial_sql}
        ORDER BY {spatial} ASC, measurand, o ASC NULLS LAST
        LIMIT :limit
        OFFSET :offset
        )
        {final_query}
        """

    if (temporal in ['day','dow'] and spatial in  ["country", "source_name"]):
        if spatial == "country":
            spatial_filter = filters.country[0]
            table='countries_daily'
        elif spatial == "source_name":
            spatial_filter = filters.project[0]
            table='sources_daily'
        params.update({
            "spatial": spatial_filter,
            "measurand": filters.measurand[0].value,
        })
        q = f"""
            WITH base AS (
                SELECT
                    {temporal_col} as d,
                    {temporal_order} as o,
                    _measurand as measurand,
                    _units as units,
                    {spatial_sql},
                    value_count,
                    value_sum
                FROM
                    {table}
                WHERE
                    {spatial}=:spatial
                    AND
                    _measurand=:measurand
                    AND
                    {temporal_q} >= :date_from::timestamptz
                    AND
                    {temporal_q} <= :date_to::timestamptz
            ), t as (
            SELECT
                d,
                o,
                measurand,
                units,
                {spatial_sql},
                count(*) over () as total,
                sum(value_count) as measurement_count,
                sum(value_sum) / sum(value_count) as average
            FROM base
            GROUP BY
                d,
                o,
                measurand,
                units,
                {spatial_sql}
            ORDER BY {spatial} ASC, measurand, o ASC NULLS LAST
            LIMIT :limit
            OFFSET :offset
            )
            {final_query}
            """

    rows = await db.fetch(q, params)


    if len(rows) > 0:
        total = rows[0][0]
        json_rows = [json.loads(r["json"]) for r in rows]
    else:
        total = 0
        json_rows = []

    meta = {
        "name": "openaq-api",
        "license": "CC BY 4.0",
        "website": "https://docs.openaq.org/",
        "page": paging.page,
        "limit": paging.limit,
        "found": total,
    }

    logger.debug("Total Time: %s", time.time() - start)
    return {"meta": meta, "results": json_rows}


@router.get("/v2/averages")
async def averages_v2_get(
    db: DB = Depends(),
    paging: MeasurementPaging = Depends(),
    filters: MeasurementFilters = Depends(),
    spatial: Spatial = Query(...),
    temporal: Temporal = Query(...),
):
    start = time.time()
    paging_q = await paging.sql()
    where_q = await filters.sql()
    date_from = paging.date_from
    date_to = paging.date_to

    params = {}
    params.update(paging_q["params"])
    params.update(where_q["params"])

    params.update( {
        "date_to": date_to,
        "date_from": date_from,
    })

    where_sql = where_q["q"]
    paging_sql = paging_q["q"]

    summary_q = f"""
        SELECT
            sum(value_count) as count,
            min(first_datetime) as first,
            max(last_datetime) as last
        FROM sensors_total
        LEFT JOIN sensors_first_last USING (sensors_id)
        LEFT JOIN sensors USING (sensors_id)
        LEFT JOIN sensor_systems USING (sensor_systems_id)
        LEFT JOIN sensor_nodes USING (sensor_nodes_id)
        LEFT JOIN sensor_nodes_json USING (sensor_nodes_id)
        LEFT JOIN measurands USING (measurands_id)
        WHERE {where_sql}
        """
    summary = await db.fetchrow(summary_q, params)
    if summary is None or summary[0] is None:
        return {"error": "no sensors found matching search"}
    logger.debug('total_count %s, min %s, max %s', summary[0], summary[1], summary[2])
    total_count = summary[0]
    range_start = summary[1].replace(tzinfo=UTC)
    range_end = summary[2].replace(tzinfo=UTC)

    if date_from is None:
        date_from = range_start
    else:
        date_from = max(date_from, range_start)

    if date_to is None:
        date_to = range_end
    else:
        date_to = min(date_to, range_end)



    spatial_disp = spatial
    if spatial == 'project':
       spatial='source_name'
    if spatial == 'location':
        spatial='self'
        spatial_disp='location'
    params.update( {
        "date_to": date_to,
        "date_from": date_from,
        "spatial": spatial,
        "spatial_disp": spatial_disp,
        "temporal": temporal,
        "order_by": 'd'
    })

    where = ''
    if filters.measurand is not None:
        where += ' AND measurand = ANY(:measurand) '

    if filters.units is not None:
        where += ' AND units = ANY(:units) '

    if filters.country is not None and spatial == 'country':
        where += ' AND name = ANY(:country) '

    if filters.country is not None and spatial == '':
        where += ' AND name = ANY(:country) '

    q = f"""
        WITH base AS (
        SELECT
            jsonb_build_object(
                'parameter', measurand,
                'unit', units,
                :temporal::text, st::date,
                :spatial_disp::text, name,
                'subtitle', subtitle,
                'measurement_count', value_count,
                'average', round((value_sum/value_count)::numeric, 4)
            ) || metadata as json
        FROM rollups
        LEFT JOIN groups_w_measurand USING (groups_id, measurands_id)
        WHERE
            rollup = :temporal::text
            AND
            type = :spatial::text
            {where}
        ORDER BY st DESC
        OFFSET :offset
        LIMIT :limit
        )
        SELECT json
        FROM base;
        """

    rows = await db.fetch(q, params)

    total = len(rows)

    if total > 0:
        json_rows = [json.loads(row[0]) for row in rows]
    else:
        total = 0
        json_rows = []

    meta = {
        "name": "openaq-api",
        "license": "CC BY 4.0",
        "website": "https://docs.openaq.org/",
        "page": paging.page,
        "limit": paging.limit,
        "found": total,
    }

    logger.debug("Total Time: %s", time.time() - start)
    return {"meta": meta, "results": json_rows}
