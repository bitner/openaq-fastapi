import logging
import time
from typing import Optional

import orjson as json
from fastapi import APIRouter, Depends, Query

from .base import DB, MeasurementFilters, MeasurementPaging, Spatial, Temporal

logger = logging.getLogger("locations")
logger.setLevel(logging.DEBUG)

router = APIRouter()


@router.get("/averages")
async def locations_get(
    db: DB = Depends(),
    paging: MeasurementPaging = Depends(),
    filters: MeasurementFilters = Depends(),
    spatial: Optional[Spatial] = Query("country"),
    temporal: Optional[Temporal] = Query("year"),
):
    start = time.time()
    paging_q = await paging.sql()
    where_q = await filters.sql()
    date_from = paging.date_from
    date_to = paging.date_to
    date_from_adj = paging.date_from_adj
    date_to_adj = paging.date_to_adj

    params = {}
    params.update(paging_q["params"])
    params.update(where_q["params"])

    params.update( {
        "date_to": date_to,
        "date_from": date_from,
        "date_to_adj": date_to_adj,
        "date_from_adj": date_from_adj,
    })

    where_sql = where_q["q"]
    paging_sql = paging_q["q"]

    if spatial == 'project':
       spatial='source_name'



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
            "'source_name'", "source_name"
        ])
        cols.append('source_name')

    if spatial in ['city', 'location']:
        base_obj.extend([
            "'city'", "city"
        ])
        cols.append('city')

    if spatial == 'location':
        spatial = "site_name"
        cols.append(["site_name", "lat", "lon"])
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
                value_count,
                value_sum
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
